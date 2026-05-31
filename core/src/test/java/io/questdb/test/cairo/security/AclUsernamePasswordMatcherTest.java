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

import io.questdb.cairo.security.AclStore;
import io.questdb.cairo.security.AclUsernamePasswordMatcher;
import io.questdb.std.Misc;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static io.questdb.cairo.SecurityContext.AUTH_TYPE_CREDENTIALS;
import static io.questdb.cairo.SecurityContext.AUTH_TYPE_NONE;

public class AclUsernamePasswordMatcherTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testEmptyUsernameReturnsNone() throws Exception {
        // Must not reach aclStore.lookup() with an empty key.
        AclUsernamePasswordMatcher matcher = new AclUsernamePasswordMatcher(load("user.alice.password=s3cret\n"));
        Assert.assertEquals(AUTH_TYPE_NONE, matcher.verifyPassword("", 0, 0));
    }

    @Test
    public void testNullUsernameReturnsNone() throws Exception {
        // A pgwire startup packet may omit the user field, leaving username null. The guard must
        // short-circuit before aclStore.lookup(null), which would NPE on the worker thread.
        AclUsernamePasswordMatcher matcher = new AclUsernamePasswordMatcher(load("user.alice.password=s3cret\n"));
        Assert.assertEquals(AUTH_TYPE_NONE, matcher.verifyPassword(null, 0, 0));
    }

    @Test
    public void testPasswordVerification() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AclUsernamePasswordMatcher matcher = new AclUsernamePasswordMatcher(load("user.alice.password=s3cret\n"));
            DirectUtf8Sink pwd = new DirectUtf8Sink(8);
            try {
                pwd.put("s3cret");
                Assert.assertEquals(AUTH_TYPE_CREDENTIALS, matcher.verifyPassword("alice", pwd.ptr(), pwd.size()));
                // wrong password
                Assert.assertEquals(AUTH_TYPE_NONE, matcher.verifyPassword("alice", pwd.ptr(), pwd.size() - 1));
                // unknown user
                Assert.assertEquals(AUTH_TYPE_NONE, matcher.verifyPassword("nobody", pwd.ptr(), pwd.size()));
            } finally {
                Misc.free(pwd);
            }
        });
    }

    private AclStore load(String contents) throws Exception {
        final File file = new File(temp.getRoot(), "acl.conf");
        Files.write(file.toPath(), contents.getBytes(StandardCharsets.UTF_8));
        return AclStore.load(temp.getRoot().getAbsolutePath());
    }
}
