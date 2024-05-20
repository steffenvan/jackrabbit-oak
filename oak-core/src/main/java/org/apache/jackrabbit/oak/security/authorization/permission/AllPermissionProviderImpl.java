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

import java.util.Collections;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.security.authorization.ProviderCtx;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of the {@code PermissionProvider} interface that grants full permission
 * everywhere.
 */
public final class AllPermissionProviderImpl implements PermissionProvider,
    AggregatedPermissionProvider {

    private static final Set<String> ALL = Collections.singleton(PrivilegeConstants.JCR_ALL);

    private final Root root;
    private final ProviderCtx providerCtx;
    private Root immutableRoot;

    public AllPermissionProviderImpl(@NotNull Root root, @NotNull ProviderCtx providerCtx) {
        this.root = root;
        this.providerCtx = providerCtx;
        this.immutableRoot = providerCtx.getRootProvider().createReadOnlyRoot(root);
    }

    @Override
    public void refresh() {
        immutableRoot = providerCtx.getRootProvider().createReadOnlyRoot(root);
    }

    @NotNull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        return ALL;
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @NotNull String... privilegeNames) {
        return true;
    }

    @NotNull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        return RepositoryPermission.ALL;
    }

    @NotNull
    @Override
    public TreePermission getTreePermission(@NotNull Tree tree,
        @NotNull TreePermission parentPermission) {
        return TreePermission.ALL;
    }

    @Override
    public boolean isGranted(@NotNull Tree tree, @Nullable PropertyState property,
        long permissions) {
        return true;
    }

    @Override
    public boolean isGranted(@NotNull String oakPath, @NotNull String jcrActions) {
        return true;
    }

    //---------------------------------------< AggregatedPermissionProvider >---
    @NotNull
    @Override
    public PrivilegeBits supportedPrivileges(@Nullable Tree tree,
        @Nullable PrivilegeBits privilegeBits) {
        return (privilegeBits != null) ? privilegeBits
            : new PrivilegeBitsProvider(immutableRoot).getBits(PrivilegeConstants.JCR_ALL);
    }

    @Override
    public long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property,
        long permissions) {
        return permissions;
    }

    @Override
    public long supportedPermissions(@NotNull TreeLocation location, long permissions) {
        return permissions;
    }

    @Override
    public long supportedPermissions(@NotNull TreePermission treePermission, PropertyState property,
        long permissions) {
        return permissions;
    }

    @Override
    public boolean isGranted(@NotNull TreeLocation location, long permissions) {
        return true;
    }

    @NotNull
    @Override
    public TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreeType type,
        @NotNull TreePermission parentPermission) {
        return TreePermission.ALL;
    }
}
