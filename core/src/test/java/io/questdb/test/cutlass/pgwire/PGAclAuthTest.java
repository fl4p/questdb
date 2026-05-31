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

package io.questdb.test.cutlass.pgwire;

import io.questdb.ServerMain;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * End-to-end test of ACL-backed pgwire authentication and prefix-scoped
 * authorization over a real pgwire socket. A {@code conf/acl.conf} turns the
 * server into multi-user mode: {@link io.questdb.FactoryProviderImpl} then wires
 * {@link io.questdb.cutlass.pgwire.AclPGAuthenticatorFactory} and
 * {@link io.questdb.cairo.security.PrefixAwareSecurityContextFactory} into the
 * pgwire path. Mirrors the HTTP behavior: when the ACL is present, only ACL users
 * can log in and the built-in admin/quest account is rejected.
 */
public class PGAclAuthTest extends AbstractBootstrapTest {

    private static final String ACL =
            "user.ops.password=opspass\n" +
                    "user.ops.access=rw\n" +
                    "user.alice.password=s3cret\n" +
                    "user.alice.access=rw\n" +
                    "user.alice.prefix=projecta_\n" +
                    "user.bob.password=bobpass\n" +
                    "user.bob.access=ro\n" +
                    "user.bob.prefix=projecta_\n";

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> {
            createDummyConfiguration();
            final File aclFile = new File(new File(root, "conf"), "acl.conf");
            Files.write(aclFile.toPath(), ACL.getBytes(StandardCharsets.UTF_8));
        });
        dbPath.parent().$();
    }

    @Test
    public void testAdminRejectedWhenAclPresent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getBindPort();
                // The built-in admin/quest account is fully replaced by the ACL, just like over HTTP.
                assertQueryFails(
                        "admin",
                        "quest",
                        port,
                        "select 1",
                        "invalid username/password",
                        "admin/quest must be rejected when acl.conf is present"
                );
            }
        });
    }

    @Test
    public void testKnownGap_prefixUserCanCreateOutsidePrefix() throws Exception {
        // SECURITY KNOWN GAP (tracked): a prefix-scoped rw user can CREATE objects OUTSIDE its
        // prefix. authorizeTableCreate()/authorizeViewCreate()/authorizeMatViewCreate() receive no
        // object name, and checkCreate() only checks the read-only flag -- so the prefix is not
        // enforced at create time. This is pre-existing in the shared PrefixAwareSecurityContext
        // (already reachable over HTTP); pgwire ACL only extends the reach. The blast radius is
        // namespace squatting only: the creator gets NO data access to the squatted table (read,
        // insert, drop all route through the prefix check and are denied), so it is not a data leak.
        //
        // TRIPWIRE: when authorizeTableCreate enforces the prefix, the CREATE below will start
        // failing with "Access denied". At that point, invert this assertion (expect failure) and
        // delete this known-gap note. See docs/acl-pgwire-plan.md.
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getBindPort();

                // alice (prefix projecta_, rw) can currently squat a name outside her prefix.
                assertQuerySucceeds(
                        "alice",
                        "s3cret",
                        port,
                        "create table projectb_squat (x int)",
                        "KNOWN GAP: prefix-scoped rw user can currently create outside its prefix"
                );

                // ...but squatting grants no data access: she cannot even read what she created.
                assertQueryFails(
                        "alice",
                        "s3cret",
                        port,
                        "select count() from projectb_squat",
                        "Access denied [table=projectb_squat]",
                        "squatted table must not be readable by its creator"
                );
            }
        });
    }

    @Test
    public void testPrefixScopedAuthorization() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getBindPort();

                // ops has no prefix, so it can create tables across prefixes and seed data.
                try (Connection conn = getConnection("ops", "opspass", port);
                     Statement stmt = conn.createStatement()) {
                    stmt.execute("create table projecta_trades (px double)");
                    stmt.execute("create table projecta_orders (qty long)");
                    stmt.execute("create table projectb_trades (px double)");
                    stmt.execute("insert into projecta_trades values (1.0)");
                }

                // alice sees only her two projecta_ tables, not projectb_trades.
                Assert.assertEquals(2, queryLong("alice", "s3cret", port, "select count() from tables()"));

                // alice can read inside her prefix...
                Assert.assertEquals(1, queryLong("alice", "s3cret", port, "select count() from projecta_trades"));

                // ...but reading outside her prefix is denied.
                assertQueryFails(
                        "alice",
                        "s3cret",
                        port,
                        "select count() from projectb_trades",
                        "Access denied [table=projectb_trades]",
                        "alice must not read a table outside her prefix"
                );
            }
        });
    }

    @Test
    public void testReadOnlyUser() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getBindPort();

                try (Connection conn = getConnection("ops", "opspass", port);
                     Statement stmt = conn.createStatement()) {
                    stmt.execute("create table projecta_trades (px double)");
                    stmt.execute("insert into projecta_trades values (1.0)");
                }

                // bob is read-only: he can select inside his prefix...
                Assert.assertEquals(1, queryLong("bob", "bobpass", port, "select count() from projecta_trades"));

                // ...but cannot write, even inside his prefix.
                assertQueryFails(
                        "bob",
                        "bobpass",
                        port,
                        "insert into projecta_trades values (2.0)",
                        "Write permission denied",
                        "read-only bob must not be able to insert"
                );
            }
        });
    }

    @Test
    public void testUnknownUserRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getBindPort();
                assertQueryFails(
                        "ghost",
                        "whatever",
                        port,
                        "select 1",
                        "invalid username/password",
                        "unknown user must be rejected"
                );
            }
        });
    }

    @Test
    public void testValidUserAuthenticates() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getBindPort();
                assertQuerySucceeds(
                        "alice",
                        "s3cret",
                        port,
                        "select 1",
                        "valid ACL credentials must authenticate"
                );
            }
        });
    }

    @Test
    public void testWrongPasswordRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ServerMain serverMain = startWithEnvVariables()) {
                int port = serverMain.getConfiguration().getPGWireConfiguration().getBindPort();
                assertQueryFails(
                        "alice",
                        "wrong",
                        port,
                        "select 1",
                        "invalid username/password",
                        "wrong password must be rejected"
                );
            }
        });
    }

    private static long queryLong(String username, String password, int port, String sql) throws SQLException {
        try (Connection conn = getConnection(username, password, port);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            Assert.assertTrue(rs.next());
            return rs.getLong(1);
        }
    }
}
