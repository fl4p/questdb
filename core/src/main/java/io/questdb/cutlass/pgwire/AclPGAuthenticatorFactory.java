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

package io.questdb.cutlass.pgwire;

import io.questdb.BuildInformation;
import io.questdb.ServerConfiguration;
import io.questdb.cairo.security.AclStore;
import io.questdb.cairo.security.AclUsernamePasswordMatcher;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.auth.SocketAuthenticator;
import io.questdb.cutlass.auth.UsernamePasswordMatcher;
import org.jetbrains.annotations.NotNull;

/**
 * Produces pgwire authenticators that verify credentials against an
 * {@link AclStore} loaded from {@code conf/acl.conf}. {@link io.questdb.FactoryProviderImpl}
 * installs this factory in place of {@link DefaultPGAuthenticatorFactory} when an
 * ACL is present, so pgwire logins follow the same multi-user rules as HTTP.
 */
public final class AclPGAuthenticatorFactory implements PGAuthenticatorFactory {
    private final AclStore aclStore;
    private final BuildInformation buildInformation;

    public AclPGAuthenticatorFactory(@NotNull ServerConfiguration serverConfiguration, @NotNull AclStore aclStore) {
        this.aclStore = aclStore;
        this.buildInformation = serverConfiguration.getCairoConfiguration().getBuildInformation();
    }

    @Override
    public SocketAuthenticator getPgWireAuthenticator(
            PGConfiguration configuration,
            NetworkSqlExecutionCircuitBreaker circuitBreaker,
            PGCircuitBreakerRegistry registry,
            OptionsListener optionsListener
    ) {
        // Ownership contract: AclUsernamePasswordMatcher holds no native memory (only an AclStore
        // reference and a DirectUtf8String flyweight), so it is created fresh per connection and
        // passed with matcherOwned=false -- there is nothing to close. If this matcher ever gains a
        // native buffer or pooled resource, make it QuietCloseable and pass matcherOwned=true so
        // PGCleartextPasswordAuthenticator releases it on connection close.
        final UsernamePasswordMatcher matcher = new AclUsernamePasswordMatcher(aclStore);

        // HexTestsCircuitBreakRegistry implies we are either recording or replaying a hex test.
        // In this case, we don't send build information to the client. Build information is volatile by nature, we
        // only record what does not change over time.
        BuildInformation buildInformationToUse = (registry == PGHexTestsCircuitBreakRegistry.INSTANCE ? null : buildInformation);

        return new PGCleartextPasswordAuthenticator(
                configuration,
                buildInformationToUse,
                circuitBreaker,
                registry,
                optionsListener,
                matcher,
                false
        );
    }
}
