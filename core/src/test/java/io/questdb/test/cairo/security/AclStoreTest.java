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
import io.questdb.cairo.security.AclEntry;
import io.questdb.cairo.security.AclStore;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class AclStoreTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testAbsentFileReturnsNull() throws Exception {
        Assert.assertNull(AclStore.load(temp.getRoot().getAbsolutePath()));
    }

    @Test
    public void testCommentedOutFileReturnsNull() throws Exception {
        Assert.assertNull(load("# no users here\n# user.x.password=y\n"));
    }

    @Test
    public void testDefaultAccessIsReadWrite() throws Exception {
        AclStore store = load("user.bob.password=pw\n");
        Assert.assertFalse(store.lookup("bob").isReadOnly());
    }

    @Test
    public void testInvalidAccessRejected() throws Exception {
        assertConfigError("user.bob.password=pw\nuser.bob.access=write\n", "invalid access");
    }

    @Test
    public void testMissingPasswordRejected() throws Exception {
        assertConfigError("user.bob.access=ro\n", "no password");
    }

    @Test
    public void testParsesUsers() throws Exception {
        AclStore store = load(
                "user.alice.password=s3cret\n" +
                        "user.alice.access=ro\n" +
                        "user.alice.prefix=projectA_\n" +
                        "user.bob.password=hunter2\n" +
                        "user.bob.access=rw\n"
        );
        Assert.assertEquals(2, store.size());

        AclEntry alice = store.lookup("alice");
        Assert.assertNotNull(alice);
        Assert.assertTrue(alice.isReadOnly());
        Assert.assertEquals("projectA_", alice.getPrefix());
        Assert.assertTrue(store.authenticate("alice", "s3cret"));
        Assert.assertFalse(store.authenticate("alice", "wrong"));

        AclEntry bob = store.lookup("bob");
        Assert.assertFalse(bob.isReadOnly());
        Assert.assertNull(bob.getPrefix()); // empty prefix -> null -> all tables

        Assert.assertNull(store.lookup("nobody"));
        Assert.assertFalse(store.authenticate("nobody", "x"));
    }

    @Test
    public void testUnknownFieldRejected() throws Exception {
        assertConfigError("user.bob.password=pw\nuser.bob.role=admin\n", "unknown field");
    }

    @Test
    public void testUnrecognizedPropertyRejected() throws Exception {
        assertConfigError("admins=bob\n", "unrecognized property");
    }

    private void assertConfigError(String contents, String expectedFragment) throws Exception {
        try {
            load(contents);
            Assert.fail("expected CairoException");
        } catch (CairoException e) {
            assertContains(e.getFlyweightMessage().toString(), expectedFragment);
        }
    }

    private void assertContains(String actual, String fragment) {
        Assert.assertTrue("expected '" + actual + "' to contain '" + fragment + "'", actual.contains(fragment));
    }

    private AclStore load(String contents) throws Exception {
        final File file = new File(temp.getRoot(), "acl.conf");
        Files.write(file.toPath(), contents.getBytes(StandardCharsets.UTF_8));
        return AclStore.load(temp.getRoot().getAbsolutePath());
    }
}
