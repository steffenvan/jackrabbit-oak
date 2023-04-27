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
package org.apache.jackrabbit.oak.spi.security.privilege;

import java.util.Set;
import org.apache.jackrabbit.guava.common.base.Objects;
import org.apache.jackrabbit.guava.common.collect.ImmutableSet;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Default implementation of the {@code PrivilegeDefinition} interface.
 */
public final class ImmutablePrivilegeDefinition implements PrivilegeDefinition {

    private final String name;
    private final boolean isAbstract;
    private final Set<String> declaredAggregateNames;
    private final int hashcode;

    public ImmutablePrivilegeDefinition(@NotNull String name, boolean isAbstract, @Nullable Iterable<String> declaredAggregateNames) {
        this.name = name;
        this.isAbstract = isAbstract;
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        if (declaredAggregateNames != null) {
            builder.addAll(declaredAggregateNames);
        }
        this.declaredAggregateNames = builder.build();
        hashcode = Objects.hashCode(this.name, this.isAbstract, this.declaredAggregateNames);
    }

    //------------------------------------------------< PrivilegeDefinition >---
    @NotNull
    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isAbstract() {
        return isAbstract;
    }

    @NotNull
    @Override
    public Set<String> getDeclaredAggregateNames() {
        return declaredAggregateNames;
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public int hashCode() {
        return hashcode;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ImmutablePrivilegeDefinition) {
            ImmutablePrivilegeDefinition other = (ImmutablePrivilegeDefinition) o;
            return name.equals(other.name) &&
                    isAbstract == other.isAbstract &&
                    declaredAggregateNames.equals(other.declaredAggregateNames);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "PrivilegeDefinition: " + name;
    }
}
