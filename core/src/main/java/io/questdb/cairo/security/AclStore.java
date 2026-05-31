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

import io.questdb.cairo.CairoException;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Loads and holds the user access-control list from {@code conf/acl.conf}.
 * <p>
 * The file is a plain properties file, one block per user:
 * <pre>
 * user.alice.password=s3cret
 * user.alice.access=ro          # ro | rw   (default rw)
 * user.alice.prefix=projectA_   # optional; empty = all tables
 * </pre>
 * Loading is startup-only and fails fast (throws) on a malformed file so a
 * misconfiguration cannot silently grant or deny access.
 */
public class AclStore {
    private static final String ACL_FILE = "acl.conf";
    private static final String KEY_PREFIX = "user.";
    private final ObjList<AclEntry> entries;
    private final CharSequenceObjHashMap<AclEntry> users;

    private AclStore(CharSequenceObjHashMap<AclEntry> users, ObjList<AclEntry> entries) {
        this.users = users;
        this.entries = entries;
    }

    /**
     * Loads the ACL from {@code <confRoot>/acl.conf}. Returns null when the file
     * is absent or defines no users, meaning the ACL feature is off and the
     * server keeps its default single-user behavior.
     */
    @Nullable
    public static AclStore load(CharSequence confRoot) {
        final File file = new File(confRoot.toString(), ACL_FILE);
        if (!file.exists()) {
            return null;
        }
        final Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(file)) {
            props.load(fis);
        } catch (IOException e) {
            throw CairoException.critical(0)
                    .put("could not read acl.conf [path=").put(file.getAbsolutePath())
                    .put(", error=").put(e.getMessage()).put(']');
        }

        // collect fields per user name
        final Map<String, String[]> byName = new HashMap<>(); // [password, access, prefix]
        for (String key : props.stringPropertyNames()) {
            if (!key.startsWith(KEY_PREFIX)) {
                throw configError("unrecognized property '" + key + "' (expected user.<name>.<field>)");
            }
            final String rest = key.substring(KEY_PREFIX.length());
            final int dot = rest.lastIndexOf('.');
            if (dot <= 0 || dot == rest.length() - 1) {
                throw configError("malformed property '" + key + "' (expected user.<name>.<field>)");
            }
            final String name = rest.substring(0, dot);
            final String field = rest.substring(dot + 1);
            final String[] slot = byName.computeIfAbsent(name, n -> new String[3]);
            switch (field) {
                case "password":
                    slot[0] = props.getProperty(key);
                    break;
                case "access":
                    slot[1] = props.getProperty(key);
                    break;
                case "prefix":
                    slot[2] = props.getProperty(key);
                    break;
                default:
                    throw configError("unknown field '" + field + "' for user '" + name + "' (expected password, access or prefix)");
            }
        }

        final CharSequenceObjHashMap<AclEntry> users = new CharSequenceObjHashMap<>();
        final ObjList<AclEntry> entries = new ObjList<>();
        for (Map.Entry<String, String[]> e : byName.entrySet()) {
            final String name = e.getKey();
            final String password = e.getValue()[0];
            final String access = e.getValue()[1];
            final String prefix = e.getValue()[2];
            if (password == null || password.isEmpty()) {
                throw configError("user '" + name + "' has no password");
            }
            final boolean readOnly;
            if (access == null || access.isEmpty() || "rw".equals(access)) {
                readOnly = false;
            } else if ("ro".equals(access)) {
                readOnly = true;
            } else {
                throw configError("user '" + name + "' has invalid access '" + access + "' (expected ro or rw)");
            }
            final AclEntry entry = new AclEntry(name, password, prefix, readOnly);
            users.put(name, entry);
            entries.add(entry);
        }

        return entries.size() == 0 ? null : new AclStore(users, entries);
    }

    /**
     * All user entries, used to pre-compute per-user credentials at startup.
     */
    @NotNull
    public ObjList<AclEntry> getEntries() {
        return entries;
    }

    /**
     * Returns true if the given name exists and the plaintext password matches.
     */
    public boolean authenticate(CharSequence name, CharSequence password) {
        final AclEntry entry = users.get(name);
        return entry != null && Chars.equals(entry.getPassword(), password);
    }

    @Nullable
    public AclEntry lookup(CharSequence name) {
        return users.get(name);
    }

    public int size() {
        return users.size();
    }

    private static CairoException configError(String message) {
        return CairoException.critical(0).put("invalid acl.conf: ").put(message);
    }
}
