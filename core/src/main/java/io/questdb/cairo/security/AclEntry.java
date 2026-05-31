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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * One immutable user entry from {@code conf/acl.conf}: a name, a plaintext
 * password, an access level (read-write or read-only), and an optional table
 * name prefix the user is restricted to. A null/empty prefix means the user may
 * access all tables.
 */
public class AclEntry {
    private final String name;
    private final String password;
    private final String prefix;
    private final boolean readOnly;

    public AclEntry(@NotNull String name, @NotNull String password, @Nullable String prefix, boolean readOnly) {
        this.name = name;
        this.password = password;
        this.prefix = prefix == null || prefix.isEmpty() ? null : prefix;
        this.readOnly = readOnly;
    }

    public String getName() {
        return name;
    }

    public String getPassword() {
        return password;
    }

    @Nullable
    public String getPrefix() {
        return prefix;
    }

    public boolean isReadOnly() {
        return readOnly;
    }
}
