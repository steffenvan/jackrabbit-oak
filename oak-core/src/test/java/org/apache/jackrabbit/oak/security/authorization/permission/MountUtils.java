/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.security.authorization.permission;

import org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

final class MountUtils {

    private MountUtils() {
    }

    static AuthorizationConfiguration bindMountInfoProvider(
        @NotNull SecurityProvider securityProvider, @NotNull MountInfoProvider mountInfoProvider) {
        AuthorizationConfiguration acConfig = securityProvider.getConfiguration(
            AuthorizationConfiguration.class);
        Assert.assertTrue(acConfig instanceof CompositeAuthorizationConfiguration);
        ((AuthorizationConfigurationImpl) ((CompositeAuthorizationConfiguration) acConfig).getDefaultConfig())
            .bindMountInfoProvider(mountInfoProvider);
        return acConfig;
    }
}
