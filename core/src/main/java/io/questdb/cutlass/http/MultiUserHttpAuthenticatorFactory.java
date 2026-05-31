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

package io.questdb.cutlass.http;

import io.questdb.cairo.security.AclEntry;
import io.questdb.cairo.security.AclStore;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.Utf8SequenceObjHashMap;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;

/**
 * Produces HTTP Basic authenticators that validate credentials against the ACL
 * store. At construction it precomputes, for every ACL user, the expected
 * {@code Basic <base64(user:password)>} header, so per-request authentication is
 * a single hash lookup. Mirrors {@link StaticHttpAuthenticatorFactory}, extended
 * from one user to many.
 */
public class MultiUserHttpAuthenticatorFactory implements HttpAuthenticatorFactory {
    private final Utf8SequenceObjHashMap<String> headerToUser = new Utf8SequenceObjHashMap<>();

    public MultiUserHttpAuthenticatorFactory(AclStore aclStore) {
        final BinarySequenceAdapter adapter = new BinarySequenceAdapter();
        final ObjList<AclEntry> entries = aclStore.getEntries();
        for (int i = 0, n = entries.size(); i < n; i++) {
            final AclEntry entry = entries.getQuick(i);
            adapter.clear();
            adapter.put(entry.getName()).colon().put(entry.getPassword());
            final Utf8StringSink expectedHeader = new Utf8StringSink();
            expectedHeader.put("Basic ");
            Chars.base64Encode(adapter, (int) adapter.length(), expectedHeader);
            headerToUser.put(new Utf8String(expectedHeader.toString()), entry.getName());
        }
    }

    @Override
    public HttpAuthenticator getHttpAuthenticator() {
        return new MultiUserHttpAuthenticator(headerToUser);
    }

    private static class BinarySequenceAdapter implements BinarySequence, Mutable {
        private final Utf8StringSink baseSink = new Utf8StringSink();

        @Override
        public byte byteAt(long index) {
            return baseSink.byteAt((int) index);
        }

        @Override
        public void clear() {
            baseSink.clear();
        }

        @Override
        public long length() {
            return baseSink.size();
        }

        BinarySequenceAdapter colon() {
            baseSink.putAscii(':');
            return this;
        }

        BinarySequenceAdapter put(CharSequence value) {
            baseSink.put(value);
            return this;
        }
    }
}
