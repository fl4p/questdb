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

package io.questdb.cairo.security;

import io.questdb.cutlass.auth.UsernamePasswordMatcher;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8s;

import static io.questdb.cairo.SecurityContext.AUTH_TYPE_CREDENTIALS;
import static io.questdb.cairo.SecurityContext.AUTH_TYPE_NONE;

/**
 * Verifies pgwire credentials against an {@link AclStore} loaded from
 * {@code conf/acl.conf}. When the ACL is active, this matcher fully replaces the
 * built-in pgwire accounts: only users defined in the ACL can authenticate,
 * mirroring the multi-user HTTP authentication path.
 * <p>
 * Instances are created per pgwire connection, so the mutable {@link #flyweight}
 * is never shared across threads.
 */
public class AclUsernamePasswordMatcher implements UsernamePasswordMatcher {
    private final AclStore aclStore;
    // Wraps the on-wire password bytes for comparison; holds no native memory of its own.
    private final DirectUtf8String flyweight = new DirectUtf8String();

    public AclUsernamePasswordMatcher(AclStore aclStore) {
        this.aclStore = aclStore;
    }

    @Override
    public byte verifyPassword(CharSequence username, long passwordPtr, int passwordLen) {
        // A pgwire startup packet may omit the user field, leaving username null; honor the
        // UsernamePasswordMatcher contract (return AUTH_TYPE_NONE for null/empty) instead of
        // letting a null reach aclStore.lookup() and NPE on the worker thread.
        if (username == null || username.length() == 0) {
            return AUTH_TYPE_NONE;
        }
        final AclEntry entry = aclStore.lookup(username);
        if (entry == null) {
            return AUTH_TYPE_NONE;
        }
        flyweight.of(passwordPtr, passwordPtr + passwordLen);
        return Utf8s.equalsUtf16(entry.getPassword(), flyweight) ? AUTH_TYPE_CREDENTIALS : AUTH_TYPE_NONE;
    }
}
