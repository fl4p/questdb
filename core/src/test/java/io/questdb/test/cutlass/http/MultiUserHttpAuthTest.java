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

package io.questdb.test.cutlass.http;

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.cairo.security.AclStore;
import io.questdb.cairo.security.AllowAllSecurityContextFactory;
import io.questdb.cairo.security.PrefixAwareSecurityContextFactory;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cutlass.http.HttpAuthenticatorFactory;
import io.questdb.cutlass.http.MultiUserHttpAuthenticatorFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * End-to-end test of the ACL-backed multi-user HTTP Basic authentication and
 * prefix-scoped authorization over a real HTTP socket. Each user is defined in an
 * in-memory acl.conf; the test wires {@link MultiUserHttpAuthenticatorFactory} and
 * {@link PrefixAwareSecurityContextFactory} into the server and exercises /exec.
 */
public class MultiUserHttpAuthTest extends AbstractTest {

    private static final String ACL =
            "user.alice.password=s3cret\n" +
                    "user.alice.access=rw\n" +
                    "user.alice.prefix=projecta_\n";
    private static final TestHttpClient testHttpClient = new TestHttpClient();

    @AfterClass
    public static void tearDownStatic() {
        testHttpClient.close();
        AbstractTest.tearDownStatic();
        assert Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN) == 0;
    }

    @Test
    public void testPrefixFilteringOverHttp() throws Exception {
        runWithAcl((engine, sqlExecutionContext) -> {
            engine.execute("create table projecta_trades (ts timestamp, px double) timestamp(ts)", sqlExecutionContext);
            engine.execute("create table projecta_orders (ts timestamp, qty long) timestamp(ts)", sqlExecutionContext);
            engine.execute("create table projectb_trades (ts timestamp, px double) timestamp(ts)", sqlExecutionContext);
            engine.execute("insert into projecta_trades values (0, 1.0)", sqlExecutionContext);

            // tables() is filtered to alice's prefix: she sees 2, not the projectb table
            testHttpClient.assertGet(
                    "/exec",
                    "{\"query\":\"select count() from tables()\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[2]],\"count\":1}",
                    "select count() from tables()",
                    "alice",
                    "s3cret"
            );

            // and she can read her own table
            testHttpClient.assertGet(
                    "/exec",
                    "{\"query\":\"select count() from projecta_trades\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                    "select count() from projecta_trades",
                    "alice",
                    "s3cret"
            );
        });
    }

    @Test
    public void testUnknownUserRejected() throws Exception {
        runWithAcl((engine, sqlExecutionContext) -> testHttpClient.assertGet(
                "/exec",
                "Unauthorized\r\n",
                "select 1",
                "ghost",
                "whatever"
        ));
    }

    @Test
    public void testValidCredentialsAuthenticate() throws Exception {
        runWithAcl((engine, sqlExecutionContext) -> testHttpClient.assertGet(
                "/exec",
                "{\"query\":\"select 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                "select 1",
                "alice",
                "s3cret"
        ));
    }

    @Test
    public void testWrongPasswordRejected() throws Exception {
        runWithAcl((engine, sqlExecutionContext) -> testHttpClient.assertGet(
                "/exec",
                "Unauthorized\r\n",
                "select 1",
                "alice",
                "wrong"
        ));
    }

    private void runWithAcl(HttpQueryTestBuilder.HttpClientCode code) throws Exception {
        final File aclDir = temp.newFolder();
        Files.write(new File(aclDir, "acl.conf").toPath(), ACL.getBytes(StandardCharsets.UTF_8));
        final AclStore store = AclStore.load(aclDir.getAbsolutePath());
        Assert.assertNotNull(store);

        final FactoryProvider factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull HttpAuthenticatorFactory getHttpAuthenticatorFactory() {
                return new MultiUserHttpAuthenticatorFactory(store);
            }

            @Override
            public @NotNull SecurityContextFactory getSecurityContextFactory() {
                return new PrefixAwareSecurityContextFactory(store, AllowAllSecurityContextFactory.INSTANCE);
            }
        };

        new HttpQueryTestBuilder()
                .withWorkerCount(1)
                .withTempFolder(root)
                .withFactoryProvider(factoryProvider)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .run(code);
    }
}
