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

import io.questdb.cairo.SecurityContext;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

/**
 * Builds a {@link PrefixAwareSecurityContext} for principals defined in the
 * {@link AclStore}. Principals not in the ACL (e.g. an internal/admin identity)
 * fall through to the wrapped factory, preserving the server's default behavior
 * for everyone except the configured ACL users.
 */
public class PrefixAwareSecurityContextFactory implements SecurityContextFactory {
    private final AclStore aclStore;
    private final SecurityContextFactory fallback;

    public PrefixAwareSecurityContextFactory(@NotNull AclStore aclStore, @NotNull SecurityContextFactory fallback) {
        this.aclStore = aclStore;
        this.fallback = fallback;
    }

    @Override
    public SecurityContext getInstance(@Transient @NotNull PrincipalContext principalContext, byte interfaceId) {
        final CharSequence principal = principalContext.getPrincipal();
        if (principal != null) {
            final AclEntry entry = aclStore.lookup(principal);
            if (entry != null) {
                return new PrefixAwareSecurityContext(entry.getName(), entry.getPrefix(), entry.isReadOnly());
            }
        }
        return fallback.getInstance(principalContext, interfaceId);
    }
}
