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

package io.questdb.test.cutlass.influxdb;

import io.questdb.cutlass.influxdb.InfluxQlException;
import io.questdb.cutlass.influxdb.InfluxQlTranslator;
import io.questdb.cutlass.influxdb.TranslatedQuery;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the InfluxQL-to-SQL mappings that do not require a table
 * (the {@code SHOW} forms and parse-error handling). The {@code SELECT} path,
 * which resolves a real designated-timestamp column, is covered end-to-end by
 * {@link InfluxQueryProcessorTest}.
 */
public class InfluxQlTranslatorTest {
    private final ObjList<TranslatedQuery> out = new ObjList<>();
    private final InfluxQlTranslator.StatementConsumer consumer = () -> {
        TranslatedQuery t = new TranslatedQuery();
        out.add(t);
        return t;
    };
    private final InfluxQlTranslator translator = new InfluxQlTranslator();

    @Test
    public void testShowDatabasesIsSynthesized() throws InfluxQlException {
        TranslatedQuery tq = translateOne("SHOW DATABASES");
        Assert.assertTrue(tq.synthesized);
        Assert.assertEquals("databases", tq.seriesName);
        Assert.assertEquals(1, tq.synthColumns.size());
        Assert.assertEquals("name", tq.synthColumns.getQuick(0));
        Assert.assertEquals("\"qdb\"", tq.synthRowJson.getQuick(0));
    }

    @Test
    public void testShowFieldKeys() throws InfluxQlException {
        TranslatedQuery tq = translateOne("SHOW FIELD KEYS FROM \"cpu\"");
        Assert.assertEquals(
                "SELECT \"column\", CASE WHEN \"type\" IN ('DOUBLE','FLOAT') THEN 'float' " +
                        "WHEN \"type\" IN ('LONG','INT','SHORT','BYTE') THEN 'integer' " +
                        "WHEN \"type\" = 'BOOLEAN' THEN 'boolean' ELSE 'string' END " +
                        "FROM table_columns('cpu') WHERE \"type\" != 'SYMBOL' AND \"designated\" = false",
                tq.sql.toString());
        Assert.assertEquals("fieldKey", tq.valueLabels.getQuick(0));
        Assert.assertEquals("fieldType", tq.valueLabels.getQuick(1));
    }

    @Test
    public void testShowMeasurementsPlain() throws InfluxQlException {
        TranslatedQuery tq = translateOne("SHOW MEASUREMENTS");
        Assert.assertEquals("SELECT table_name FROM tables()", tq.sql.toString());
        Assert.assertEquals("measurements", tq.seriesName);
        Assert.assertEquals("name", tq.valueLabels.getQuick(0));
    }

    @Test
    public void testShowMeasurementsWithRegexAndLimit() throws InfluxQlException {
        TranslatedQuery tq = translateOne("SHOW MEASUREMENTS WITH MEASUREMENT =~ /^cpu$/ LIMIT 10");
        Assert.assertEquals("SELECT table_name FROM tables() WHERE table_name ~ '^cpu$' LIMIT 10", tq.sql.toString());
    }

    @Test
    public void testShowRetentionPoliciesIsSynthesized() throws InfluxQlException {
        TranslatedQuery tq = translateOne("SHOW RETENTION POLICIES ON \"qdb\"");
        Assert.assertTrue(tq.synthesized);
        Assert.assertEquals(5, tq.synthColumns.size());
        Assert.assertEquals("\"autogen\"", tq.synthRowJson.getQuick(0));
        Assert.assertEquals("true", tq.synthRowJson.getQuick(4));
    }

    @Test
    public void testShowTagKeys() throws InfluxQlException {
        TranslatedQuery tq = translateOne("SHOW TAG KEYS FROM \"cpu\"");
        Assert.assertEquals("SELECT \"column\" FROM table_columns('cpu') WHERE \"type\" = 'SYMBOL'", tq.sql.toString());
        Assert.assertEquals("cpu", tq.seriesName);
        Assert.assertEquals("tagKey", tq.valueLabels.getQuick(0));
    }

    @Test
    public void testShowTagValues() throws InfluxQlException {
        TranslatedQuery tq = translateOne("SHOW TAG VALUES FROM \"cpu\" WITH KEY = \"host\"");
        Assert.assertEquals("SELECT DISTINCT 'host', \"host\" FROM \"cpu\"", tq.sql.toString());
        Assert.assertEquals("key", tq.valueLabels.getQuick(0));
        Assert.assertEquals("value", tq.valueLabels.getQuick(1));
    }

    @Test(expected = InfluxQlException.class)
    public void testUnsupportedShow() throws InfluxQlException {
        translateOne("SHOW FOO");
    }

    @Test(expected = InfluxQlException.class)
    public void testUnsupportedStatement() throws InfluxQlException {
        translateOne("DELETE FROM x");
    }

    private TranslatedQuery translateOne(String q) throws InfluxQlException {
        out.clear();
        translator.translate(q, null, consumer);
        return out.getLast();
    }
}
