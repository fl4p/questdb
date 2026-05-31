/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cutlass.influxdb;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

/**
 * Translates the subset of InfluxQL that Grafana's InfluxDB v1 datasource emits
 * into QuestDB SQL. This is a hand-written tokenizer plus keyword dispatch, not
 * a general InfluxQL engine. Supported statements:
 * <ul>
 *     <li>{@code SHOW MEASUREMENTS [WITH MEASUREMENT =~ /re/] [LIMIT n]}</li>
 *     <li>{@code SHOW TAG KEYS [FROM "m"]}</li>
 *     <li>{@code SHOW FIELD KEYS [FROM "m"]}</li>
 *     <li>{@code SHOW TAG VALUES [FROM "m"] WITH KEY = "k" [WHERE ...]}</li>
 *     <li>{@code SHOW RETENTION POLICIES [ON "db"]} (synthesized)</li>
 *     <li>{@code SHOW DATABASES} (synthesized)</li>
 *     <li>{@code SELECT <aggs> FROM "m" WHERE ... GROUP BY time(d)[, "tag"] fill(...) [ORDER BY time [DESC]]}</li>
 * </ul>
 * The InfluxDB data model maps onto QuestDB as: measurement = table,
 * tag = SYMBOL column, field = any other column, {@code time} = designated
 * timestamp column.
 * <p>
 * The translator is single-threaded and reuses its scratch buffers across
 * calls. Each call fully completes before the HTTP response is streamed, so the
 * durable per-request output lives in {@link TranslatedQuery} objects supplied
 * by the {@link StatementConsumer}, not in this instance.
 */
public class InfluxQlTranslator {
    private static final int T_COMMA = 6;
    private static final int T_IDENT = 1;   // double-quoted identifier (inner text)
    private static final int T_LPAREN = 4;
    private static final int T_NUMBER = 8;  // digits, optionally with a trailing time-unit suffix
    private static final int T_OP = 3;      // = != <> =~ !~ >= <= > < - +
    private static final int T_REGEX = 7;   // /.../ (inner text)
    private static final int T_RPAREN = 5;
    private static final int T_SEMI = 9;
    private static final int T_STAR = 10;
    private static final int T_STRING = 2;  // single-quoted string (inner text)
    private static final int T_WORD = 0;    // bareword / keyword
    private final StringSink sql = new StringSink();
    private final IntList tokenType = new IntList();
    private final ObjList<String> tokens = new ObjList<>();
    private String dbPrefix = ""; // InfluxDB ?db= mapped to a table-name prefix (e.g. "mydb_"), or "" for none
    private int p; // parse cursor within the current statement's token range

    /**
     * Tokenizes the (possibly multi-statement) InfluxQL text and translates each
     * {@code ;}-separated statement, requesting one {@link TranslatedQuery} per
     * statement from {@code consumer}.
     */
    public void translate(CharSequence q, CairoEngine engine, String dbPrefix, StatementConsumer consumer) throws InfluxQlException {
        this.dbPrefix = dbPrefix == null ? "" : dbPrefix;
        tokenize(q);
        final int n = tokens.size();
        int lo = 0;
        boolean any = false;
        for (int i = 0; i <= n; i++) {
            if (i == n || tokenType.getQuick(i) == T_SEMI) {
                if (i > lo) {
                    translateStatement(lo, i, engine, consumer.nextStatement());
                    any = true;
                }
                lo = i + 1;
            }
        }
        if (!any) {
            throw new InfluxQlException("empty query");
        }
    }

    private static long unitToMicros(String unit) throws InfluxQlException {
        // InfluxDB absolute time literal units -> microseconds factor
        switch (unit) {
            case "ns":
            case "n":
                return -1; // nanos: handled by caller via division
            case "u":
            case "us":
                return 1L;
            case "ms":
                return 1_000L;
            case "s":
                return 1_000_000L;
            case "m":
                return 60_000_000L;
            case "h":
                return 3_600_000_000L;
            case "d":
                return 86_400_000_000L;
            case "w":
                return 604_800_000_000L;
            default:
                throw new InfluxQlException("unsupported time unit: " + unit);
        }
    }

    private void appendTimeMicros(String numberToken) throws InfluxQlException {
        int i = 0;
        final int len = numberToken.length();
        while (i < len && (Character.isDigit(numberToken.charAt(i)) || numberToken.charAt(i) == '.')) {
            i++;
        }
        final String unit = numberToken.substring(i);
        final long value;
        try {
            value = Numbers.parseLong(numberToken, 0, i);
        } catch (NumericException e) {
            throw new InfluxQlException("invalid numeric literal: " + numberToken);
        }
        if (unit.isEmpty()) {
            // no unit: already a raw epoch count; assume microseconds (QuestDB native)
            sql.put(value);
            return;
        }
        final long factor = unitToMicros(unit);
        if (factor < 0) {
            // nanoseconds -> microseconds
            sql.put(value / 1_000L);
        } else {
            sql.put(value * factor);
        }
    }

    private boolean eq(int idx, String kw) {
        return idx < tokens.size() && tokenType.getQuick(idx) == T_WORD && kw.equalsIgnoreCase(tokens.getQuick(idx));
    }

    private boolean isTimeUnit(String unit) {
        switch (unit) {
            case "ns":
            case "n":
            case "u":
            case "us":
            case "ms":
            case "s":
            case "m":
            case "h":
            case "d":
            case "w":
                return true;
            default:
                return false;
        }
    }

    private String mapAgg(String func) {
        // QuestDB shares names for sum/count/min/max/first/last/avg; InfluxQL "mean" -> "avg".
        if ("mean".equalsIgnoreCase(func)) {
            return "avg";
        }
        return func.toLowerCase();
    }

    private String numberUnit(String numberToken) {
        int i = 0;
        final int len = numberToken.length();
        while (i < len && (Character.isDigit(numberToken.charAt(i)) || numberToken.charAt(i) == '.')) {
            i++;
        }
        return numberToken.substring(i);
    }

    // Reads a measurement reference after FROM: one or more dot-separated identifiers
    // (e.g. "rp"."measurement"); returns the last component (the measurement).
    private String readMeasurement() throws InfluxQlException {
        String last = readName();
        while (p < tokens.size() && tokenType.getQuick(p) == T_WORD && ".".equals(tokens.getQuick(p))) {
            p++;
            last = readName();
        }
        return last;
    }

    private String readName() throws InfluxQlException {
        if (p >= tokens.size()) {
            throw new InfluxQlException("unexpected end of statement, expected an identifier");
        }
        final int t = tokenType.getQuick(p);
        if (t == T_IDENT || t == T_WORD || t == T_STRING) {
            return tokens.getQuick(p++);
        }
        throw new InfluxQlException("expected an identifier but got '" + tokens.getQuick(p) + "'");
    }

    private void tokenize(CharSequence q) throws InfluxQlException {
        tokens.clear();
        tokenType.clear();
        final int n = q.length();
        int i = 0;
        while (i < n) {
            char c = q.charAt(i);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                i++;
                continue;
            }
            if (c == '"') {
                int j = i + 1;
                StringSink sink = io.questdb.std.Misc.getThreadLocalSink();
                while (j < n) {
                    char d = q.charAt(j);
                    if (d == '\\' && j + 1 < n) {
                        sink.put(q.charAt(j + 1));
                        j += 2;
                        continue;
                    }
                    if (d == '"') {
                        break;
                    }
                    sink.put(d);
                    j++;
                }
                if (j >= n) {
                    throw new InfluxQlException("unterminated identifier");
                }
                push(T_IDENT, sink.toString());
                i = j + 1;
                continue;
            }
            if (c == '\'') {
                int j = i + 1;
                StringSink sink = io.questdb.std.Misc.getThreadLocalSink();
                while (j < n) {
                    char d = q.charAt(j);
                    if (d == '\\' && j + 1 < n) {
                        sink.put(q.charAt(j + 1));
                        j += 2;
                        continue;
                    }
                    if (d == '\'') {
                        break;
                    }
                    sink.put(d);
                    j++;
                }
                if (j >= n) {
                    throw new InfluxQlException("unterminated string");
                }
                push(T_STRING, sink.toString());
                i = j + 1;
                continue;
            }
            if (c == '/') {
                int j = i + 1;
                StringSink sink = io.questdb.std.Misc.getThreadLocalSink();
                while (j < n) {
                    char d = q.charAt(j);
                    if (d == '\\' && j + 1 < n) {
                        sink.put(d);
                        sink.put(q.charAt(j + 1));
                        j += 2;
                        continue;
                    }
                    if (d == '/') {
                        break;
                    }
                    sink.put(d);
                    j++;
                }
                if (j >= n) {
                    throw new InfluxQlException("unterminated regular expression");
                }
                push(T_REGEX, sink.toString());
                i = j + 1;
                continue;
            }
            if (c == '(') {
                push(T_LPAREN, "(");
                i++;
                continue;
            }
            if (c == ')') {
                push(T_RPAREN, ")");
                i++;
                continue;
            }
            if (c == ',') {
                push(T_COMMA, ",");
                i++;
                continue;
            }
            if (c == ';') {
                push(T_SEMI, ";");
                i++;
                continue;
            }
            if (c == '*') {
                push(T_STAR, "*");
                i++;
                continue;
            }
            if (c == '.') {
                push(T_WORD, ".");
                i++;
                continue;
            }
            if (c == '=' || c == '!' || c == '<' || c == '>' || c == '-' || c == '+') {
                String op;
                if (c == '=' && i + 1 < n && q.charAt(i + 1) == '~') {
                    op = "=~";
                } else if (c == '!' && i + 1 < n && q.charAt(i + 1) == '~') {
                    op = "!~";
                } else if (c == '!' && i + 1 < n && q.charAt(i + 1) == '=') {
                    op = "!=";
                } else if (c == '<' && i + 1 < n && q.charAt(i + 1) == '>') {
                    op = "<>";
                } else if (c == '<' && i + 1 < n && q.charAt(i + 1) == '=') {
                    op = "<=";
                } else if (c == '>' && i + 1 < n && q.charAt(i + 1) == '=') {
                    op = ">=";
                } else {
                    op = String.valueOf(c);
                }
                push(T_OP, op);
                i += op.length();
                continue;
            }
            if (Character.isDigit(c)) {
                int j = i;
                while (j < n && (Character.isDigit(q.charAt(j)) || q.charAt(j) == '.')) {
                    j++;
                }
                // capture an immediately adjacent unit suffix (letters), e.g. 1500ms
                int k = j;
                while (k < n && Character.isLetter(q.charAt(k))) {
                    k++;
                }
                push(T_NUMBER, q.subSequence(i, k).toString());
                i = k;
                continue;
            }
            if (Character.isLetter(c) || c == '_') {
                int j = i;
                while (j < n && (Character.isLetterOrDigit(q.charAt(j)) || q.charAt(j) == '_')) {
                    j++;
                }
                push(T_WORD, q.subSequence(i, j).toString());
                i = j;
                continue;
            }
            throw new InfluxQlException("unexpected character '" + c + "' in query");
        }
    }

    // Maps an InfluxDB measurement to a QuestDB table name, applying the ?db= prefix.
    private String tableName(String measurement) {
        return dbPrefix.isEmpty() ? measurement : dbPrefix + measurement;
    }

    private void translateSelect(int lo, int hi, CairoEngine engine, TranslatedQuery out) throws InfluxQlException {
        p = lo + 1; // skip SELECT

        // ----- select list -----
        final ObjList<String> itemExpr = new ObjList<>();
        final ObjList<String> itemLabel = new ObjList<>();
        final ObjList<String> itemField = new ObjList<>();
        boolean hasAggregate = false;
        while (p < hi && !eq(p, "FROM")) {
            if (tokenType.getQuick(p) == T_COMMA) {
                p++;
                continue;
            }
            if (tokenType.getQuick(p) == T_WORD && p + 1 < hi && tokenType.getQuick(p + 1) == T_LPAREN) {
                // aggregate: func ( arg )
                final String func = tokens.getQuick(p);
                p += 2; // func (
                String field;
                if (p < hi && tokenType.getQuick(p) == T_STAR) {
                    field = "*";
                    p++;
                } else {
                    field = readName();
                }
                if (p >= hi || tokenType.getQuick(p) != T_RPAREN) {
                    throw new InfluxQlException("expected ')' after aggregate argument");
                }
                p++; // )
                final String mapped = mapAgg(func);
                final StringSink expr = io.questdb.std.Misc.getThreadLocalSink();
                expr.put(mapped).put('(');
                if ("*".equals(field)) {
                    expr.put('*');
                } else {
                    expr.put('"').put(field).put('"');
                }
                expr.put(')');
                itemExpr.add(expr.toString());
                itemLabel.add(func.toLowerCase());
                itemField.add(field);
                hasAggregate = true;
            } else {
                // raw field
                final String field = readName();
                itemExpr.add('"' + field + '"');
                itemLabel.add(field);
                itemField.add(field);
            }
        }
        if (!eq(p, "FROM")) {
            throw new InfluxQlException("expected FROM in SELECT");
        }
        p++; // FROM
        final String measurement = readMeasurement();

        // ----- optional WHERE -----
        int whereLo = -1, whereHi = -1;
        if (eq(p, "WHERE")) {
            p++;
            whereLo = p;
            while (p < hi && !eq(p, "GROUP") && !eq(p, "ORDER") && !eq(p, "LIMIT") && !eq(p, "SLIMIT") && !eq(p, "FILL") && !eq(p, "fill")) {
                p++;
            }
            whereHi = p;
        }

        // ----- optional GROUP BY -----
        String sampleInterval = null;
        final ObjList<String> tags = new ObjList<>();
        if (eq(p, "GROUP")) {
            p++;
            if (!eq(p, "BY")) {
                throw new InfluxQlException("expected BY after GROUP");
            }
            p++;
            while (p < hi && !eq(p, "ORDER") && !eq(p, "LIMIT") && !eq(p, "SLIMIT") && !eq(p, "FILL") && !eq(p, "fill")) {
                if (tokenType.getQuick(p) == T_COMMA) {
                    p++;
                    continue;
                }
                if (eq(p, "time") && p + 1 < hi && tokenType.getQuick(p + 1) == T_LPAREN) {
                    p += 2; // time (
                    if (p >= hi || tokenType.getQuick(p) != T_NUMBER) {
                        throw new InfluxQlException("expected an interval in GROUP BY time(...)");
                    }
                    sampleInterval = tokens.getQuick(p);
                    p++;
                    // allow an optional offset argument: time(20s, 5s) -> ignore offset
                    while (p < hi && tokenType.getQuick(p) != T_RPAREN) {
                        p++;
                    }
                    if (p < hi) {
                        p++; // )
                    }
                } else {
                    tags.add(readName());
                }
            }
        }

        // fill() may follow GROUP BY (or, defensively, appear after WHERE)
        String fill = null;
        if (eq(p, "FILL") || eq(p, "fill")) {
            p++;
            if (p >= hi || tokenType.getQuick(p) != T_LPAREN) {
                throw new InfluxQlException("expected '(' after fill");
            }
            p++;
            final StringSink fb = io.questdb.std.Misc.getThreadLocalSink();
            while (p < hi && tokenType.getQuick(p) != T_RPAREN) {
                fb.put(tokens.getQuick(p));
                p++;
            }
            if (p < hi) {
                p++; // )
            }
            fill = fb.toString();
        }

        // ----- optional ORDER BY time [DESC] -----
        boolean orderDesc = false;
        if (eq(p, "ORDER")) {
            p++;
            if (eq(p, "BY")) {
                p++;
            }
            // skip the ordering column (always time) and read direction
            while (p < hi && !eq(p, "LIMIT") && !eq(p, "SLIMIT")) {
                if (eq(p, "DESC")) {
                    orderDesc = true;
                }
                p++;
            }
        }

        // resolve the measurement (with optional ?db= prefix) and its designated timestamp
        final String table = tableName(measurement);
        final String tsCol;
        TableToken tt = engine.getTableTokenIfExists(table);
        if (tt == null) {
            out.statementError = "measurement not found: " + measurement;
            return;
        }
        try (TableMetadata meta = engine.getTableMetadata(tt)) {
            int tsIdx = meta.getTimestampIndex();
            if (tsIdx < 0) {
                out.statementError = "measurement has no time column: " + measurement;
                return;
            }
            tsCol = meta.getColumnName(tsIdx);
        } catch (Throwable th) {
            out.statementError = "cannot read measurement metadata: " + measurement;
            return;
        }

        // ----- build the QuestDB SQL + column roles -----
        out.seriesName = measurement;
        sql.clear();
        sql.put("SELECT ");
        int cursorIdx = 0;
        final boolean hasTime = sampleInterval != null || !hasAggregate;
        if (hasTime) {
            sql.put('"').put(tsCol).put('"');
            out.timeCol = cursorIdx++;
        }
        for (int i = 0, n = tags.size(); i < n; i++) {
            if (cursorIdx > 0) {
                sql.put(", ");
            }
            sql.put('"').put(tags.getQuick(i)).put('"');
            out.tagCols.add(cursorIdx++);
            out.tagLabels.add(tags.getQuick(i));
        }
        for (int i = 0, n = itemExpr.size(); i < n; i++) {
            if (cursorIdx > 0) {
                sql.put(", ");
            }
            sql.put(itemExpr.getQuick(i));
            out.valueCols.add(cursorIdx++);
            out.valueLabels.add(uniqueLabel(out.valueLabels, itemLabel.getQuick(i), itemField.getQuick(i)));
        }
        sql.put(" FROM \"").put(table).put('"');
        if (whereLo >= 0 && whereHi > whereLo) {
            sql.put(" WHERE ");
            transformWhere(whereLo, whereHi, tsCol);
        }
        if (sampleInterval != null) {
            sql.put(" SAMPLE BY ").put(sampleInterval);
            sql.put(" FILL(").put(fillToQuestDb(fill)).put(')');
            sql.put(" ALIGN TO CALENDAR");
        }
        if (hasTime) {
            sql.put(" ORDER BY ");
            for (int i = 0, n = out.tagCols.size(); i < n; i++) {
                sql.put('"').put(out.tagLabels.getQuick(i)).put("\", ");
            }
            sql.put('"').put(tsCol).put('"');
            if (orderDesc) {
                sql.put(" DESC");
            }
        }
        out.sql = sql.toString();
    }

    private String fillToQuestDb(String fill) {
        if (fill == null) {
            return "NULL";
        }
        if ("null".equalsIgnoreCase(fill)) {
            return "NULL";
        }
        if ("none".equalsIgnoreCase(fill)) {
            return "NONE";
        }
        if ("previous".equalsIgnoreCase(fill)) {
            return "PREV";
        }
        if ("linear".equalsIgnoreCase(fill)) {
            return "LINEAR";
        }
        // numeric constant
        return fill;
    }

    private String uniqueLabel(ObjList<String> existing, String base, String field) {
        for (int i = 0, n = existing.size(); i < n; i++) {
            if (base.equals(existing.getQuick(i))) {
                return base + "_" + field;
            }
        }
        return base;
    }

    private void translateShow(int lo, int hi, TranslatedQuery out) throws InfluxQlException {
        // dispatch on the words after SHOW
        if (eq(lo + 1, "MEASUREMENTS")) {
            sql.clear();
            sql.put("SELECT table_name FROM tables()");
            // optional WITH MEASUREMENT =~ /re/
            int idx = lo + 2;
            String regex = null;
            String limit = null;
            for (int i = idx; i < hi; i++) {
                if (eq(i, "MEASUREMENT") && i + 2 < hi && tokenType.getQuick(i + 1) == T_OP && "=~".equals(tokens.getQuick(i + 1)) && tokenType.getQuick(i + 2) == T_REGEX) {
                    regex = tokens.getQuick(i + 2);
                }
                if (eq(i, "LIMIT") && i + 1 < hi && tokenType.getQuick(i + 1) == T_NUMBER) {
                    limit = tokens.getQuick(i + 1);
                }
            }
            boolean hasWhere = false;
            if (!dbPrefix.isEmpty()) {
                sql.put(" WHERE table_name LIKE '").put(escapeSingleQuotes(dbPrefix)).put("%'");
                hasWhere = true;
                out.stripPrefix = dbPrefix;
                out.stripPrefixCol = 0;
            }
            if (regex != null) {
                sql.put(hasWhere ? " AND " : " WHERE ").put("table_name ~ '").put(escapeSingleQuotes(regex)).put('\'');
            }
            if (limit != null) {
                sql.put(" LIMIT ").put(limit);
            }
            out.seriesName = "measurements";
            out.valueCols.add(0);
            out.valueLabels.add("name");
            out.sql = sql.toString();
            return;
        }
        if (eq(lo + 1, "DATABASES")) {
            out.synthesized = true;
            out.seriesName = "databases";
            out.synthColumns.add("name");
            out.synthRowJson.add("\"qdb\"");
            return;
        }
        if (eq(lo + 1, "RETENTION") && eq(lo + 2, "POLICIES")) {
            out.synthesized = true;
            out.seriesName = null;
            out.synthColumns.add("name");
            out.synthColumns.add("duration");
            out.synthColumns.add("shardGroupDuration");
            out.synthColumns.add("replicaN");
            out.synthColumns.add("default");
            out.synthRowJson.add("\"autogen\"");
            out.synthRowJson.add("\"0s\"");
            out.synthRowJson.add("\"0s\"");
            out.synthRowJson.add("1");
            out.synthRowJson.add("true");
            return;
        }
        if (eq(lo + 1, "TAG") && eq(lo + 2, "KEYS")) {
            final String m = measurementFrom(lo + 3, hi);
            if (m == null) {
                throw new InfluxQlException("SHOW TAG KEYS requires FROM \"measurement\"");
            }
            sql.clear();
            sql.put("SELECT \"column\" FROM table_columns('").put(escapeSingleQuotes(tableName(m))).put("') WHERE \"type\" = 'SYMBOL'");
            out.seriesName = m;
            out.valueCols.add(0);
            out.valueLabels.add("tagKey");
            out.sql = sql.toString();
            return;
        }
        if (eq(lo + 1, "FIELD") && eq(lo + 2, "KEYS")) {
            final String m = measurementFrom(lo + 3, hi);
            if (m == null) {
                throw new InfluxQlException("SHOW FIELD KEYS requires FROM \"measurement\"");
            }
            sql.clear();
            sql.put("SELECT \"column\", ")
                    .put("CASE WHEN \"type\" IN ('DOUBLE','FLOAT') THEN 'float' ")
                    .put("WHEN \"type\" IN ('LONG','INT','SHORT','BYTE') THEN 'integer' ")
                    .put("WHEN \"type\" = 'BOOLEAN' THEN 'boolean' ELSE 'string' END ")
                    .put("FROM table_columns('").put(escapeSingleQuotes(tableName(m))).put("') ")
                    .put("WHERE \"type\" != 'SYMBOL' AND \"designated\" = false");
            out.seriesName = m;
            out.valueCols.add(0);
            out.valueCols.add(1);
            out.valueLabels.add("fieldKey");
            out.valueLabels.add("fieldType");
            out.sql = sql.toString();
            return;
        }
        if (eq(lo + 1, "TAG") && eq(lo + 2, "VALUES")) {
            final String m = measurementFrom(lo + 3, hi);
            if (m == null) {
                throw new InfluxQlException("SHOW TAG VALUES requires FROM \"measurement\"");
            }
            // find WITH KEY = "k"
            String key = null;
            for (int i = lo + 3; i < hi; i++) {
                if (eq(i, "KEY") && i + 2 < hi && tokenType.getQuick(i + 1) == T_OP && "=".equals(tokens.getQuick(i + 1))) {
                    key = tokens.getQuick(i + 2);
                    break;
                }
            }
            if (key == null) {
                throw new InfluxQlException("SHOW TAG VALUES requires WITH KEY = \"tag\"");
            }
            sql.clear();
            sql.put("SELECT DISTINCT '").put(escapeSingleQuotes(key)).put("', \"").put(key).put("\" FROM \"").put(escapeSingleQuotes(tableName(m))).put('"');
            out.seriesName = m;
            out.valueCols.add(0);
            out.valueCols.add(1);
            out.valueLabels.add("key");
            out.valueLabels.add("value");
            out.sql = sql.toString();
            return;
        }
        throw new InfluxQlException("unsupported SHOW statement");
    }

    private static String escapeSingleQuotes(String s) {
        if (s.indexOf('\'') < 0) {
            return s;
        }
        return s.replace("'", "''");
    }

    // Returns the measurement named in a "FROM <name>" clause within [lo,hi), or null.
    private String measurementFrom(int lo, int hi) throws InfluxQlException {
        for (int i = lo; i < hi; i++) {
            if (eq(i, "FROM")) {
                p = i + 1;
                return readMeasurement();
            }
        }
        return null;
    }

    private void push(int type, String text) {
        tokens.add(text);
        tokenType.add(type);
    }

    private void transformWhere(int lo, int hi, String tsCol) throws InfluxQlException {
        int i = lo;
        boolean prevValue = false; // whether the previous emitted token was a value/identifier
        while (i < hi) {
            final int t = tokenType.getQuick(i);
            final String text = tokens.getQuick(i);
            if (t == T_WORD && "time".equalsIgnoreCase(text)) {
                sql.put('"').put(tsCol).put('"');
                prevValue = true;
                i++;
                continue;
            }
            if (t == T_WORD && "now".equalsIgnoreCase(text) && i + 2 < hi
                    && tokenType.getQuick(i + 1) == T_LPAREN && tokenType.getQuick(i + 2) == T_RPAREN) {
                i += 3;
                // optional relative offset: now() - 5m
                if (i + 1 < hi && tokenType.getQuick(i) == T_OP
                        && ("-".equals(tokens.getQuick(i)) || "+".equals(tokens.getQuick(i)))
                        && tokenType.getQuick(i + 1) == T_NUMBER) {
                    final String sign = tokens.getQuick(i);
                    final String num = tokens.getQuick(i + 1);
                    final String unit = numberUnit(num);
                    if (!isTimeUnit(unit)) {
                        throw new InfluxQlException("expected a duration after now()");
                    }
                    final String n = num.substring(0, num.length() - unit.length());
                    sql.put("dateadd('").put(relativeUnit(unit)).put("', ").put("-".equals(sign) ? "-" : "").put(n).put(", now())");
                    i += 2;
                } else {
                    sql.put("now()");
                }
                prevValue = true;
                continue;
            }
            if (t == T_OP && "=~".equals(text)) {
                sql.put(" ~ ");
                prevValue = false;
                i++;
                continue;
            }
            if (t == T_OP && "!~".equals(text)) {
                sql.put(" !~ ");
                prevValue = false;
                i++;
                continue;
            }
            if (t == T_OP && "<>".equals(text)) {
                sql.put(" != ");
                prevValue = false;
                i++;
                continue;
            }
            if (t == T_REGEX) {
                sql.put('\'').put(escapeSingleQuotes(text)).put('\'');
                prevValue = true;
                i++;
                continue;
            }
            if (t == T_NUMBER) {
                final String unit = numberUnit(text);
                if (!unit.isEmpty() && isTimeUnit(unit)) {
                    appendTimeMicros(text);
                } else {
                    sql.put(text);
                }
                prevValue = true;
                i++;
                continue;
            }
            switch (t) {
                case T_IDENT:
                    sql.put('"').put(text).put('"');
                    prevValue = true;
                    break;
                case T_STRING:
                    sql.put('\'').put(escapeSingleQuotes(text)).put('\'');
                    prevValue = true;
                    break;
                case T_LPAREN:
                    sql.put('(');
                    prevValue = false;
                    break;
                case T_RPAREN:
                    sql.put(')');
                    prevValue = true;
                    break;
                case T_OP:
                    sql.put(' ').put(text).put(' ');
                    prevValue = false;
                    break;
                case T_STAR:
                    sql.put('*');
                    prevValue = false;
                    break;
                case T_WORD:
                    // keyword (AND/OR) or bareword identifier
                    if (prevValue && ("and".equalsIgnoreCase(text) || "or".equalsIgnoreCase(text))) {
                        sql.put(' ').put(text).put(' ');
                    } else {
                        sql.put(text);
                    }
                    prevValue = !("and".equalsIgnoreCase(text) || "or".equalsIgnoreCase(text));
                    break;
                default:
                    sql.put(text);
                    prevValue = true;
                    break;
            }
            i++;
        }
    }

    private String relativeUnit(String influxUnit) {
        // map InfluxDB relative duration unit to a QuestDB dateadd period char
        switch (influxUnit) {
            case "s":
                return "s";
            case "m":
                return "m";
            case "h":
                return "h";
            case "d":
                return "d";
            case "w":
                return "w";
            default:
                return "s";
        }
    }

    private void translateStatement(int lo, int hi, CairoEngine engine, TranslatedQuery out) throws InfluxQlException {
        out.clear();
        if (eq(lo, "SHOW")) {
            translateShow(lo, hi, out);
        } else if (eq(lo, "SELECT")) {
            translateSelect(lo, hi, engine, out);
        } else {
            throw new InfluxQlException("unsupported InfluxQL statement");
        }
    }

    /**
     * Supplies a cleared {@link TranslatedQuery} for each statement. The
     * implementation owns the returned objects so they survive HTTP park/resume.
     */
    public interface StatementConsumer {
        TranslatedQuery nextStatement();
    }
}
