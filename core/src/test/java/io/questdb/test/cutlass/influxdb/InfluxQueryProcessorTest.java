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

import io.questdb.PropertyKey;
import io.questdb.client.Sender;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.cutlass.http.HttpUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

public class InfluxQueryProcessorTest extends AbstractBootstrapTest {

    // 2024-01-01T00:00:00Z
    private static final long BASE_MS = 1_704_067_200_000L;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAggregates() throws Exception {
        // distinct timestamps within the base bucket make first()/last() deterministic
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                final int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP).address("localhost:" + port).build()) {
                    sender.table("agg").doubleColumn("value", 1.0).doubleColumn("v2", 10.0).at(BASE_MS, ChronoUnit.MILLIS);
                    sender.table("agg").doubleColumn("value", 5.0).doubleColumn("v2", 50.0).at(BASE_MS + 1_000, ChronoUnit.MILLIS);
                    sender.table("agg").doubleColumn("value", 3.0).doubleColumn("v2", 30.0).at(BASE_MS + 10_000, ChronoUnit.MILLIS);
                    sender.flush();
                }
                serverMain.awaitTable("agg");
                serverMain.assertSql("SELECT count() FROM agg", "count\n3\n");

                final String window = " FROM \"agg\" WHERE time >= 1704067200000ms AND time <= 1704067210000ms GROUP BY time(10s) fill(none)";
                try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                    // all six pass-through aggregates in one query; count yields a bare integer (LONG)
                    assertQuery(client, port,
                            "SELECT sum(\"value\"), count(\"value\"), min(\"value\"), max(\"value\"), first(\"value\"), last(\"value\")" + window,
                            "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"agg\",\"columns\":[\"time\",\"sum\",\"count\",\"min\",\"max\",\"first\",\"last\"],\"values\":[" +
                                    "[1704067200000,6.0,2,1.0,5.0,1.0,5.0]," +
                                    "[1704067210000,3.0,1,3.0,3.0,3.0,3.0]]}]}]}");

                    // two aggregates of the same function get disambiguated column labels (mean, mean_v2)
                    assertQuery(client, port,
                            "SELECT mean(\"value\"), mean(\"v2\")" + window,
                            "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"agg\",\"columns\":[\"time\",\"mean\",\"mean_v2\"],\"values\":[" +
                                    "[1704067200000,3.0,30.0]," +
                                    "[1704067210000,3.0,30.0]]}]}]}");
                }
            }
        });
    }

    @Test
    public void testChunkedResume() throws Exception {
        // small send buffer forces the response to span many chunks, exercising the
        // bookmark/resetToBookmark resume path in InfluxQueryProcessorState
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_SEND_BUFFER_SIZE.getEnvVarName(), "1024"
            )) {
                final int rows = 200;
                final int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP).address("localhost:" + port).build()) {
                    for (int i = 0; i < rows; i++) {
                        sender.table("big").doubleColumn("value", i).at(BASE_MS + i * 1000L, ChronoUnit.MILLIS);
                    }
                    sender.flush();
                }
                serverMain.awaitTable("big");
                serverMain.assertSql("SELECT count() FROM big", "count\n" + rows + "\n");

                final StringBuilder expected = new StringBuilder(
                        "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"big\",\"columns\":[\"time\",\"mean\"],\"values\":[");
                for (int i = 0; i < rows; i++) {
                    if (i > 0) {
                        expected.append(',');
                    }
                    expected.append('[').append(BASE_MS + i * 1000L).append(',').append(i).append(".0]");
                }
                expected.append("]}]}]}");

                try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                    assertQuery(client, port,
                            "SELECT mean(\"value\") FROM \"big\" WHERE time >= 1704067200000ms AND time <= 1704067399000ms GROUP BY time(1s) fill(none)",
                            expected.toString());
                }
            }
        });
    }

    @Test
    public void testDbPrefix() throws Exception {
        // ?db=<db> maps a measurement to table <db>_<measurement>; SHOW strips the prefix
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                final int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP).address("localhost:" + port).build()) {
                    sender.table("mydb_cpu").symbol("host", "h1").doubleColumn("value", 2.0).at(BASE_MS, ChronoUnit.MILLIS);
                    sender.flush();
                }
                serverMain.awaitTable("mydb_cpu");
                serverMain.assertSql("SELECT count() FROM mydb_cpu", "count\n1\n");

                try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                    // SHOW MEASUREMENTS scoped to db=mydb returns the bare measurement name
                    assertQueryDb(client, port, "mydb", "SHOW MEASUREMENTS",
                            "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"measurements\",\"columns\":[\"name\"],\"values\":[[\"cpu\"]]}]}]}");

                    // SHOW TAG KEYS FROM "cpu" with db=mydb resolves table mydb_cpu
                    assertQueryDb(client, port, "mydb", "SHOW TAG KEYS FROM \"cpu\"",
                            "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"cpu\",\"columns\":[\"tagKey\"],\"values\":[[\"host\"]]}]}]}");

                    // SELECT FROM "cpu" with db=mydb resolves table mydb_cpu; series name is bare "cpu"
                    assertQueryDb(client, port, "mydb",
                            "SELECT mean(\"value\") FROM \"cpu\" WHERE time >= 1704067200000ms AND time <= 1704067200000ms GROUP BY time(10s) fill(none)",
                            "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"cpu\",\"columns\":[\"time\",\"mean\"],\"values\":[[1704067200000,2.0]]}]}]}");

                    // without ?db=, bare "cpu" does not exist -> per-statement error
                    assertQueryDb(client, port, "",
                            "SELECT mean(\"value\") FROM \"cpu\" WHERE time >= 1704067200000ms AND time <= 1704067200000ms GROUP BY time(10s) fill(none)",
                            "{\"results\":[{\"statement_id\":0,\"error\":\"measurement not found: cpu\"}]}");
                }
            }
        });
    }

    @Test
    public void testFillModes() throws Exception {
        // table "g" has a gap: points at base and base+20s, nothing at base+10s,
        // so SAMPLE BY 10s leaves the middle bucket empty for fill() to act on
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                final int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP).address("localhost:" + port).build()) {
                    sender.table("g").doubleColumn("value", 10.0).at(BASE_MS, ChronoUnit.MILLIS);
                    sender.table("g").doubleColumn("value", 30.0).at(BASE_MS + 20_000, ChronoUnit.MILLIS);
                    sender.flush();
                }
                serverMain.awaitTable("g");
                serverMain.assertSql("SELECT count() FROM g", "count\n2\n");

                final String base = "SELECT mean(\"value\") FROM \"g\" WHERE time >= 1704067200000ms AND time <= 1704067220000ms GROUP BY time(10s) ";
                try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                    assertQuery(client, port, base + "fill(null)",
                            series("g", null, "[[1704067200000,10.0],[1704067210000,null],[1704067220000,30.0]]"));
                    // default (no fill) must behave like fill(null) — the common Grafana case
                    assertQuery(client, port, base.trim(),
                            series("g", null, "[[1704067200000,10.0],[1704067210000,null],[1704067220000,30.0]]"));
                    assertQuery(client, port, base + "fill(previous)",
                            series("g", null, "[[1704067200000,10.0],[1704067210000,10.0],[1704067220000,30.0]]"));
                    assertQuery(client, port, base + "fill(0)",
                            series("g", null, "[[1704067200000,10.0],[1704067210000,0.0],[1704067220000,30.0]]"));
                    assertQuery(client, port, base + "fill(linear)",
                            series("g", null, "[[1704067200000,10.0],[1704067210000,20.0],[1704067220000,30.0]]"));
                }
            }
        });
    }

    @Test
    public void testOrderByDesc() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                seed(serverMain);
                final int port = serverMain.getHttpServerPort();
                try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                    // tag grouping (host ASC) preserved while time runs DESC within each series
                    assertQuery(client, port,
                            "SELECT mean(\"value\") FROM \"m\" WHERE time >= 1704067200000ms AND time <= 1704067210000ms GROUP BY time(10s), \"host\" fill(none) ORDER BY time DESC",
                            "{\"results\":[{\"statement_id\":0,\"series\":[" +
                                    "{\"name\":\"m\",\"tags\":{\"host\":\"h1\"},\"columns\":[\"time\",\"mean\"],\"values\":[[1704067210000,3.0],[1704067200000,1.0]]}," +
                                    "{\"name\":\"m\",\"tags\":{\"host\":\"h2\"},\"columns\":[\"time\",\"mean\"],\"values\":[[1704067200000,5.0]]}" +
                                    "]}]}");
                }
            }
        });
    }

    @Test
    public void testWhereFilters() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                seed(serverMain);
                final int port = serverMain.getHttpServerPort();
                final String h1Only = series("m", null, "[[1704067200000,1.0],[1704067210000,3.0]]");
                try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                    // equality tag filter
                    assertQuery(client, port,
                            "SELECT mean(\"value\") FROM \"m\" WHERE (\"host\" = 'h1') AND time >= 1704067200000ms AND time <= 1704067210000ms GROUP BY time(10s) fill(none)",
                            h1Only);
                    // regex tag filter =~
                    assertQuery(client, port,
                            "SELECT mean(\"value\") FROM \"m\" WHERE (\"host\" =~ /h1/) AND time >= 1704067200000ms AND time <= 1704067210000ms GROUP BY time(10s) fill(none)",
                            h1Only);
                    // now()-relative time bound (huge window includes the 2024 data regardless of wall clock)
                    assertQuery(client, port,
                            "SELECT mean(\"value\") FROM \"m\" WHERE (\"host\" = 'h1') AND time > now() - 9999d GROUP BY time(10s) fill(none)",
                            h1Only);
                }
            }
        });
    }

    @Test
    public void testErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                seed(serverMain);
                final int port = serverMain.getHttpServerPort();
                try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                    // unknown measurement -> HTTP 200 with a per-statement error
                    assertQuery(client, port,
                            "SELECT mean(\"value\") FROM \"nope\" WHERE time >= 1704067200000ms AND time <= 1704067210000ms GROUP BY time(10s) fill(none)",
                            "{\"results\":[{\"statement_id\":0,\"error\":\"measurement not found: nope\"}]}");

                    // malformed InfluxQL -> HTTP 400 with a top-level error
                    assertStatus(client, port, "DELETE FROM x", "400");
                }
            }
        });
    }

    @Test
    public void testPing() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                final int port = serverMain.getHttpServerPort();
                try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                    try (HttpClient.ResponseHeaders rh = client.newRequest("127.0.0.1", port)
                            .GET()
                            .url("/ping")
                            .send()
                    ) {
                        rh.await();
                        TestUtils.assertEquals("204", rh.getStatusCode());
                        TestUtils.assertEquals("1.8.10", rh.getHeader(new Utf8String("x-influxdb-version")));
                    }
                }
            }
        });
    }

    @Test
    public void testPostFormBody() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                seed(serverMain);
                final int port = serverMain.getHttpServerPort();
                try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                    try (HttpClient.ResponseHeaders rh = client.newRequest("127.0.0.1", port)
                            .POST()
                            .url("/query")
                            .header("Content-Type", "application/x-www-form-urlencoded")
                            .withContent()
                            .putAscii("q=SHOW+MEASUREMENTS+WITH+MEASUREMENT+%3D~+%2F%5Em%24%2F")
                            .send()
                    ) {
                        rh.await();
                        TestUtils.assertEquals("200", rh.getStatusCode());
                        HttpUtils.assertChunkedBody(rh,
                                "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"measurements\",\"columns\":[\"name\"],\"values\":[[\"m\"]]}]}]}");
                    }
                }
            }
        });
    }

    @Test
    public void testShowAndSelect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                seed(serverMain);
                final int port = serverMain.getHttpServerPort();
                try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                    assertQuery(client, port, "SHOW MEASUREMENTS WITH MEASUREMENT =~ /^m$/",
                            "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"measurements\",\"columns\":[\"name\"],\"values\":[[\"m\"]]}]}]}");

                    assertQuery(client, port, "SHOW TAG KEYS FROM \"m\"",
                            "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"m\",\"columns\":[\"tagKey\"],\"values\":[[\"host\"]]}]}]}");

                    assertQuery(client, port, "SHOW FIELD KEYS FROM \"m\"",
                            "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"m\",\"columns\":[\"fieldKey\",\"fieldType\"],\"values\":[[\"value\",\"float\"]]}]}]}");

                    assertQuery(client, port, "SHOW TAG VALUES FROM \"m\" WITH KEY = \"host\"",
                            "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"m\",\"columns\":[\"key\",\"value\"],\"values\":[[\"host\",\"h1\"],[\"host\",\"h2\"]]}]}]}");

                    assertQuery(client, port, "SHOW RETENTION POLICIES ON \"qdb\"",
                            "{\"results\":[{\"statement_id\":0,\"series\":[{\"columns\":[\"name\",\"duration\",\"shardGroupDuration\",\"replicaN\",\"default\"],\"values\":[[\"autogen\",\"0s\",\"0s\",1,true]]}]}]}");

                    // SELECT without GROUP BY tag -> single series, no tags object
                    assertQuery(client, port,
                            "SELECT mean(\"value\") FROM \"m\" WHERE time >= 1704067200000ms AND time <= 1704067210000ms GROUP BY time(10s) fill(none)",
                            "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"m\",\"columns\":[\"time\",\"mean\"],\"values\":[[1704067200000,3.0],[1704067210000,3.0]]}]}]}");

                    // SELECT GROUP BY tag -> one series per host with a tags object
                    assertQuery(client, port,
                            "SELECT mean(\"value\") FROM \"m\" WHERE time >= 1704067200000ms AND time <= 1704067210000ms GROUP BY time(10s), \"host\" fill(none)",
                            "{\"results\":[{\"statement_id\":0,\"series\":[" +
                                    "{\"name\":\"m\",\"tags\":{\"host\":\"h1\"},\"columns\":[\"time\",\"mean\"],\"values\":[[1704067200000,1.0],[1704067210000,3.0]]}," +
                                    "{\"name\":\"m\",\"tags\":{\"host\":\"h2\"},\"columns\":[\"time\",\"mean\"],\"values\":[[1704067200000,5.0]]}" +
                                    "]}]}");
                }
            }
        });
    }

    private void assertQuery(HttpClient client, int port, String influxql, String expectedBody) {
        try (HttpClient.ResponseHeaders rh = client.newRequest("127.0.0.1", port)
                .GET()
                .url("/query")
                .query("q", influxql)
                .send()
        ) {
            rh.await();
            TestUtils.assertEquals("200", rh.getStatusCode());
            HttpUtils.assertChunkedBody(rh, expectedBody);
        }
    }

    private void assertQueryDb(HttpClient client, int port, String db, String influxql, String expectedBody) {
        try (HttpClient.ResponseHeaders rh = client.newRequest("127.0.0.1", port)
                .GET()
                .url("/query")
                .query("db", db)
                .query("q", influxql)
                .send()
        ) {
            rh.await();
            TestUtils.assertEquals("200", rh.getStatusCode());
            HttpUtils.assertChunkedBody(rh, expectedBody);
        }
    }

    private void assertStatus(HttpClient client, int port, String influxql, String expectedStatus) {
        try (HttpClient.ResponseHeaders rh = client.newRequest("127.0.0.1", port)
                .GET()
                .url("/query")
                .query("q", influxql)
                .send()
        ) {
            rh.await();
            TestUtils.assertEquals(expectedStatus, rh.getStatusCode());
        }
    }

    // Builds the expected JSON for a single-series result with columns [time, mean].
    private static String series(String name, String tags, String valuesJson) {
        final String tagsPart = tags == null ? "" : "\"tags\":" + tags + ",";
        return "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"" + name + "\","
                + tagsPart + "\"columns\":[\"time\",\"mean\"],\"values\":" + valuesJson + "}]}]}";
    }

    private void seed(TestServerMain serverMain) {
        final int port = serverMain.getHttpServerPort();
        try (Sender sender = Sender.builder(Sender.Transport.HTTP).address("localhost:" + port).build()) {
            sender.table("m").symbol("host", "h1").doubleColumn("value", 1.0).at(BASE_MS, ChronoUnit.MILLIS);
            sender.table("m").symbol("host", "h1").doubleColumn("value", 3.0).at(BASE_MS + 10_000, ChronoUnit.MILLIS);
            sender.table("m").symbol("host", "h2").doubleColumn("value", 5.0).at(BASE_MS, ChronoUnit.MILLIS);
            sender.flush();
        }
        serverMain.awaitTable("m");
        serverMain.assertSql("SELECT count() FROM m", "count\n3\n");
    }
}
