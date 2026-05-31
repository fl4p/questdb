/*******************************************************************************
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

package io.questdb.test.cairo.security;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.PrefixAwareSecurityContext;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PrefixAwareSecurityContextTest extends AbstractCairoTest {

    @Test
    public void testPrefixScoping() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContext alice = userContext("alice", "projecta_", false)) {
                // read own-prefix table
                TestUtils.assertSql(engine, alice, "select px from projecta_trades", sink, "px\n1.0\n");
                // read out-of-prefix table is denied
                assertQueryDenied(alice, "select px from projectb_trades", "Access denied");

                // every table-listing surface shows only in-prefix tables
                final String onlyAlice = "table_name\nprojecta_orders\nprojecta_trades\n";
                TestUtils.assertSql(engine, alice, "select table_name from tables() order by table_name", sink, onlyAlice);
                TestUtils.assertSql(engine, alice, "select table_name from all_tables() order by table_name", sink, onlyAlice);
                TestUtils.assertSql(engine, alice, "show tables", sink, onlyAlice);
                TestUtils.assertSql(engine, alice, "select table_name from information_schema.tables() order by table_name", sink, onlyAlice);

                // can introspect own table's columns
                TestUtils.assertSql(engine, alice, "select \"column\" from table_columns('projecta_trades')", sink, "column\nts\npx\n");
                // out-of-prefix schema is hidden (reported as non-existent)
                assertQueryDenied(alice, "select \"column\" from table_columns('projectb_trades')", "does not exist");
            }
        });
    }

    @Test
    public void testReadOnlyUser() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContext ro = userContext("rouser", "projecta_", true)) {
                // reads work
                TestUtils.assertSql(engine, ro, "select px from projecta_trades", sink, "px\n1.0\n");
                // writes and DDL are denied
                assertWriteDenied(ro, "insert into projecta_trades values (1, 2.0)", "Write permission denied");
                assertWriteDenied(ro, "drop table projecta_trades", "Write permission denied");
            }
        });
    }

    private static SqlExecutionContext userContext(String name, String prefix, boolean readOnly) {
        return new SqlExecutionContextImpl(engine, 1).with(
                new PrefixAwareSecurityContext(name, prefix, readOnly),
                bindVariableService,
                null,
                -1,
                SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER
        );
    }

    private void assertContains(CharSequence message, String fragment) {
        Assert.assertTrue("expected '" + message + "' to contain '" + fragment + "'", message.toString().contains(fragment));
    }

    private void assertQueryDenied(SqlExecutionContext ctx, String sql, String fragment) throws SqlException {
        try {
            TestUtils.assertSql(engine, ctx, sql, sink, "");
            Assert.fail("expected denial: " + sql);
        } catch (CairoException e) {
            assertContains(e.getFlyweightMessage(), fragment);
        }
    }

    private void assertWriteDenied(SqlExecutionContext ctx, String sql, String fragment) {
        try {
            execute(sql, ctx);
            Assert.fail("expected denial: " + sql);
        } catch (CairoException e) {
            assertContains(e.getFlyweightMessage(), fragment);
        } catch (SqlException e) {
            Assert.fail("unexpected SqlException for '" + sql + "': " + e.getMessage());
        }
    }

    private void createTables() throws SqlException {
        execute("create table projecta_trades (ts timestamp, px double) timestamp(ts)");
        execute("create table projecta_orders (ts timestamp, qty long) timestamp(ts)");
        execute("create table projectb_trades (ts timestamp, px double) timestamp(ts)");
        execute("insert into projecta_trades values (0, 1.0)");
        execute("insert into projectb_trades values (0, 2.0)");
    }
}
