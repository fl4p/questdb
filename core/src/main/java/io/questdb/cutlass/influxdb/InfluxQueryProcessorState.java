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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;

import java.io.Closeable;

/**
 * Per-request state for {@link InfluxQueryProcessor}. Holds the translated
 * statements, their compiled cursors, and a chunked-response state machine that
 * streams InfluxDB v1 JSON
 * ({@code {"results":[{"statement_id":N,"series":[{name,tags,columns,values}]}]}}).
 * Lives on the {@link HttpConnectionContext} via a {@code LocalValue} so it
 * survives HTTP park/resume across worker threads.
 * <p>
 * Series are formed in a single pass: the translated SQL orders rows by tag
 * tuple then time, so a change of tag tuple starts a new series. Each row is one
 * bookmarked write unit; durable state (cursor position, open-series flag, tag
 * tuple) is only mutated after all of a unit's buffer writes succeed, so a
 * buffer-full retry from the bookmark reproduces the same bytes.
 */
public class InfluxQueryProcessorState implements Mutable, Closeable, InfluxQlTranslator.StatementConsumer {
    private static final int P_DONE = 7;
    private static final int P_END = 6;
    private static final int P_SERIES = 4;
    private static final int P_SERIES_CLOSE = 5;
    private static final int P_START = 0;
    private static final int P_STMT_OPEN = 1;
    private static final int P_SYNTH = 3;
    private final ObjList<String> currentTags = new ObjList<>();
    private final ObjList<RecordCursorFactory> factories = new ObjList<>();
    private final HttpConnectionContext httpConnectionContext;
    private final CharSequence keepAliveHeader;
    private final ObjList<RecordMetadata> metadatas = new ObjList<>();
    private final ObjList<String> pendingTags = new ObjList<>();
    private final StringSink postBody = new StringSink();
    private final StringSink query = new StringSink();
    private final ObjList<TranslatedQuery> statements = new ObjList<>();
    private final ObjList<RecordCursor> cursors = new ObjList<>();
    private RecordCursor cursor;
    private int fill;
    private boolean hasPendingRow;
    private RecordMetadata metadata;
    private boolean pendingIsNewSeries;
    private int phase;
    private Record record;
    private boolean seriesOpen;
    private int statementCount;
    private int stmtIndex;
    private boolean suffixSent;
    private TranslatedQuery tq;

    public InfluxQueryProcessorState(HttpConnectionContext httpConnectionContext, CharSequence keepAliveHeader) {
        this.httpConnectionContext = httpConnectionContext;
        this.keepAliveHeader = keepAliveHeader;
    }

    public void beginStreaming() {
        phase = P_START;
        stmtIndex = 0;
        seriesOpen = false;
        hasPendingRow = false;
        suffixSent = false;
    }

    @Override
    public void clear() {
        for (int i = 0, n = cursors.size(); i < n; i++) {
            cursors.setQuick(i, Misc.free(cursors.getQuick(i)));
        }
        for (int i = 0, n = factories.size(); i < n; i++) {
            factories.setQuick(i, Misc.free(factories.getQuick(i)));
        }
        for (int i = 0, n = metadatas.size(); i < n; i++) {
            metadatas.setQuick(i, null);
        }
        cursor = null;
        metadata = null;
        record = null;
        tq = null;
        fill = 0;
        statementCount = 0;
        stmtIndex = 0;
        phase = P_START;
        seriesOpen = false;
        hasPendingRow = false;
        suffixSent = false;
        query.clear();
        postBody.clear();
        currentTags.clear();
        pendingTags.clear();
    }

    @Override
    public void close() {
        clear();
    }

    public void finishTranslation() {
        statementCount = fill;
    }

    public HttpConnectionContext getHttpConnectionContext() {
        return httpConnectionContext;
    }

    public CharSequence getKeepAliveHeader() {
        return keepAliveHeader;
    }

    public StringSink getPostBody() {
        return postBody;
    }

    public StringSink getQuery() {
        return query;
    }

    public int getStatementCount() {
        return statementCount;
    }

    public TranslatedQuery getStatement(int i) {
        return statements.getQuick(i);
    }

    @Override
    public TranslatedQuery nextStatement() {
        TranslatedQuery t;
        if (fill < statements.size()) {
            t = statements.getQuick(fill);
            t.clear();
        } else {
            t = new TranslatedQuery();
            statements.add(t);
            factories.add(null);
            cursors.add(null);
            metadatas.add(null);
        }
        fill++;
        return t;
    }

    public void resetTranslation() {
        // free any cursors/factories left over from a previous request and reset
        // the statement fill counter, keeping the pooled objects for reuse
        for (int i = 0, n = cursors.size(); i < n; i++) {
            cursors.setQuick(i, Misc.free(cursors.getQuick(i)));
        }
        for (int i = 0, n = factories.size(); i < n; i++) {
            factories.setQuick(i, Misc.free(factories.getQuick(i)));
        }
        fill = 0;
        statementCount = 0;
        currentTags.clear();
        pendingTags.clear();
    }

    public void resume(HttpChunkedResponse response) throws PeerDisconnectedException, PeerIsSlowToReadException {
        while (true) {
            switch (phase) {
                case P_START:
                    response.bookmark();
                    response.putAscii("{\"results\":[");
                    stmtIndex = 0;
                    phase = statementCount == 0 ? P_END : P_STMT_OPEN;
                    break;
                case P_STMT_OPEN:
                    loadCurrentStatement();
                    response.bookmark();
                    if (stmtIndex > 0) {
                        response.putAscii(',');
                    }
                    response.putAscii("{\"statement_id\":").put(stmtIndex);
                    if (tq.statementError != null) {
                        response.putAscii(",\"error\":");
                        response.putQuote().escapeJsonStr(tq.statementError).putQuote();
                        response.putAscii('}');
                        phase = advance();
                    } else {
                        response.putAscii(",\"series\":[");
                        seriesOpen = false;
                        hasPendingRow = false;
                        phase = tq.synthesized ? P_SYNTH : P_SERIES;
                    }
                    break;
                case P_SYNTH:
                    response.bookmark();
                    writeSynthSeries(response);
                    response.putAscii("]}");
                    phase = advance();
                    break;
                case P_SERIES:
                    if (!hasPendingRow) {
                        if (cursor != null && cursor.hasNext()) {
                            record = cursor.getRecord();
                            computeTags();
                            pendingIsNewSeries = !seriesOpen || !tagsEqual();
                            hasPendingRow = true;
                        } else {
                            phase = P_SERIES_CLOSE;
                            break;
                        }
                    }
                    response.bookmark();
                    if (pendingIsNewSeries) {
                        if (seriesOpen) {
                            response.putAscii("]},");
                        }
                        writeSeriesHeader(response);
                    } else {
                        response.putAscii(',');
                    }
                    writeRowValues(response);
                    if (pendingIsNewSeries) {
                        commitTags();
                        seriesOpen = true;
                    }
                    hasPendingRow = false;
                    break;
                case P_SERIES_CLOSE:
                    response.bookmark();
                    if (seriesOpen) {
                        response.putAscii("]}");
                    }
                    response.putAscii("]}");
                    cursor = null;
                    cursors.setQuick(stmtIndex, Misc.free(cursors.getQuick(stmtIndex)));
                    phase = advance();
                    break;
                case P_END:
                    if (suffixSent) {
                        response.done();
                        return;
                    }
                    response.bookmark();
                    response.putAscii("]}");
                    suffixSent = true;
                    response.sendChunk(true);
                    return;
                case P_DONE:
                default:
                    response.done();
                    return;
            }
        }
    }

    public void setExec(int i, RecordCursorFactory factory, RecordCursor cursor, RecordMetadata metadata) {
        factories.setQuick(i, factory);
        cursors.setQuick(i, cursor);
        metadatas.setQuick(i, metadata);
    }

    private int advance() {
        stmtIndex++;
        return stmtIndex < statementCount ? P_STMT_OPEN : P_END;
    }

    private void commitTags() {
        currentTags.clear();
        for (int i = 0, n = pendingTags.size(); i < n; i++) {
            currentTags.add(pendingTags.getQuick(i));
        }
    }

    private void computeTags() {
        pendingTags.clear();
        for (int i = 0, n = tq.tagCols.size(); i < n; i++) {
            pendingTags.add(columnAsString(tq.tagCols.getQuick(i)));
        }
    }

    private String columnAsString(int idx) {
        final int type = metadata.getColumnType(idx);
        final CharSequence cs;
        switch (ColumnType.tagOf(type)) {
            case ColumnType.SYMBOL:
                cs = record.getSymA(idx);
                break;
            case ColumnType.STRING:
                cs = record.getStrA(idx);
                break;
            case ColumnType.VARCHAR:
                Utf8Sequence us = record.getVarcharA(idx);
                cs = us == null ? null : us.toString();
                break;
            default:
                cs = null;
                break;
        }
        return cs == null ? null : cs.toString();
    }

    private void loadCurrentStatement() {
        tq = statements.getQuick(stmtIndex);
        cursor = cursors.getQuick(stmtIndex);
        metadata = metadatas.getQuick(stmtIndex);
    }

    private boolean tagsEqual() {
        if (pendingTags.size() != currentTags.size()) {
            return false;
        }
        for (int i = 0, n = pendingTags.size(); i < n; i++) {
            final String a = pendingTags.getQuick(i);
            final String b = currentTags.getQuick(i);
            if (a == null ? b != null : !a.equals(b)) {
                return false;
            }
        }
        return true;
    }

    private void writeRowValues(HttpChunkedResponse response) {
        response.putAscii('[');
        boolean first = true;
        if (tq.timeCol >= 0) {
            writeTime(response, tq.timeCol);
            first = false;
        }
        for (int i = 0, n = tq.valueCols.size(); i < n; i++) {
            if (!first) {
                response.putAscii(',');
            }
            writeValue(response, tq.valueCols.getQuick(i));
            first = false;
        }
        response.putAscii(']');
    }

    private void writeSeriesHeader(HttpChunkedResponse response) {
        response.putAscii('{');
        if (tq.seriesName != null) {
            response.putAscii("\"name\":");
            response.putQuote().escapeJsonStr(tq.seriesName).putQuote();
            response.putAscii(',');
        }
        if (tq.tagLabels.size() > 0) {
            response.putAscii("\"tags\":{");
            for (int i = 0, n = tq.tagLabels.size(); i < n; i++) {
                if (i > 0) {
                    response.putAscii(',');
                }
                response.putQuote().escapeJsonStr(tq.tagLabels.getQuick(i)).putQuote();
                response.putAscii(':');
                final String v = pendingTags.getQuick(i);
                if (v == null) {
                    response.putAscii("null");
                } else {
                    response.putQuote().escapeJsonStr(v).putQuote();
                }
            }
            response.putAscii("},");
        }
        response.putAscii("\"columns\":[");
        boolean first = true;
        if (tq.timeCol >= 0) {
            response.putAscii("\"time\"");
            first = false;
        }
        for (int i = 0, n = tq.valueLabels.size(); i < n; i++) {
            if (!first) {
                response.putAscii(',');
            }
            response.putQuote().escapeJsonStr(tq.valueLabels.getQuick(i)).putQuote();
            first = false;
        }
        response.putAscii("],\"values\":[");
    }

    private void writeSynthSeries(HttpChunkedResponse response) {
        response.putAscii('{');
        if (tq.seriesName != null) {
            response.putAscii("\"name\":");
            response.putQuote().escapeJsonStr(tq.seriesName).putQuote();
            response.putAscii(',');
        }
        response.putAscii("\"columns\":[");
        for (int i = 0, n = tq.synthColumns.size(); i < n; i++) {
            if (i > 0) {
                response.putAscii(',');
            }
            response.putQuote().escapeJsonStr(tq.synthColumns.getQuick(i)).putQuote();
        }
        response.putAscii("],\"values\":[[");
        for (int i = 0, n = tq.synthRowJson.size(); i < n; i++) {
            if (i > 0) {
                response.putAscii(',');
            }
            response.putAscii(tq.synthRowJson.getQuick(i));
        }
        response.putAscii("]]}");
    }

    private void writeTime(HttpChunkedResponse response, int idx) {
        final long t = record.getTimestamp(idx);
        if (t == Numbers.LONG_NULL) {
            response.putAscii("null");
            return;
        }
        final long div = metadata.getColumnType(idx) == ColumnType.TIMESTAMP_NANO ? 1_000_000L : 1_000L;
        response.put(t / div);
    }

    private void writeValue(HttpChunkedResponse response, int idx) {
        // SHOW MEASUREMENTS: strip the <db>_ table prefix so Grafana sees bare names
        if (tq.stripPrefix != null && idx == tq.stripPrefixCol) {
            String s = columnAsString(idx);
            if (s != null && s.startsWith(tq.stripPrefix)) {
                s = s.substring(tq.stripPrefix.length());
            }
            writeStringOrNull(response, s);
            return;
        }
        final int type = metadata.getColumnType(idx);
        switch (ColumnType.tagOf(type)) {
            case ColumnType.DOUBLE: {
                double d = record.getDouble(idx);
                if (Numbers.isFinite(d)) {
                    response.put(d);
                } else {
                    response.putAscii("null");
                }
                break;
            }
            case ColumnType.FLOAT: {
                float f = record.getFloat(idx);
                if (Numbers.isFinite(f)) {
                    response.put(f);
                } else {
                    response.putAscii("null");
                }
                break;
            }
            case ColumnType.INT: {
                int i = record.getInt(idx);
                if (i == Numbers.INT_NULL) {
                    response.putAscii("null");
                } else {
                    response.put(i);
                }
                break;
            }
            case ColumnType.LONG: {
                long l = record.getLong(idx);
                if (l == Numbers.LONG_NULL) {
                    response.putAscii("null");
                } else {
                    response.put(l);
                }
                break;
            }
            case ColumnType.SHORT:
                response.put(record.getShort(idx));
                break;
            case ColumnType.BYTE:
                response.put((int) record.getByte(idx));
                break;
            case ColumnType.BOOLEAN:
                response.put(record.getBool(idx));
                break;
            case ColumnType.SYMBOL:
                writeStringOrNull(response, record.getSymA(idx));
                break;
            case ColumnType.STRING:
                writeStringOrNull(response, record.getStrA(idx));
                break;
            case ColumnType.VARCHAR: {
                Utf8Sequence us = record.getVarcharA(idx);
                if (us == null) {
                    response.putAscii("null");
                } else {
                    response.putQuote().escapeJsonStr(us).putQuote();
                }
                break;
            }
            case ColumnType.TIMESTAMP: {
                long l = record.getTimestamp(idx);
                if (l == Numbers.LONG_NULL) {
                    response.putAscii("null");
                } else {
                    long div = type == ColumnType.TIMESTAMP_NANO ? 1_000_000L : 1_000L;
                    response.put(l / div);
                }
                break;
            }
            default:
                response.putAscii("null");
                break;
        }
    }

    private static void writeStringOrNull(HttpChunkedResponse response, CharSequence cs) {
        if (cs == null) {
            response.putAscii("null");
        } else {
            response.putQuote().escapeJsonStr(cs).putQuote();
        }
    }
}
