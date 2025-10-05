/*
 * Copyright 2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.jdbc.statement;

import java.util.Optional;

import com.tsurugidb.jdbc.util.type.ArbitraryInt;
import com.tsurugidb.jdbc.util.type.TsurugiJdbcType;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;

/**
 * Tsurugi type for Placeholder.
 */
public class TsurugiJdbcPlaceholderType implements TsurugiJdbcType {

    private final Placeholder lowPlaceholder;

    /**
     * Creates a new instance.
     *
     * @param lowPlaceholder placeholder
     */
    public TsurugiJdbcPlaceholderType(Placeholder lowPlaceholder) {
        this.lowPlaceholder = lowPlaceholder;
    }

    @Override
    public AtomType getAtomType() {
        return lowPlaceholder.getAtomType();
    }

    @Override
    public Optional<Boolean> findVarying() {
        return Optional.empty();
    }

    @Override
    public Optional<ArbitraryInt> findLength() {
        return Optional.empty();
    }

    @Override
    public Optional<ArbitraryInt> findPrecision() {
        return Optional.empty();
    }

    @Override
    public Optional<ArbitraryInt> findScale() {
        return Optional.empty();
    }

    @Override
    public Optional<Boolean> findNullable() {
        return Optional.empty();
    }
}
