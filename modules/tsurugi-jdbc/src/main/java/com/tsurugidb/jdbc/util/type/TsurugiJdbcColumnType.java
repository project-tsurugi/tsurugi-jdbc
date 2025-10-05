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
package com.tsurugidb.jdbc.util.type;

import java.util.Optional;

import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

/**
 * Tsurugi type for Column.
 */
public class TsurugiJdbcColumnType implements TsurugiJdbcType {

    private final SqlCommon.Column lowColumn;

    /**
     * Creates a new instance.
     *
     * @param lowColumn column
     */
    public TsurugiJdbcColumnType(SqlCommon.Column lowColumn) {
        this.lowColumn = lowColumn;
    }

    @Override
    public AtomType getAtomType() {
        return lowColumn.getAtomType();
    }

    @Override
    public Optional<Boolean> findVarying() {
        var c = lowColumn.getVaryingOptCase();
        if (c == null) {
            return Optional.empty();
        }
        switch (c) {
        case VARYING:
            return Optional.of(lowColumn.getVarying());
        case VARYINGOPT_NOT_SET:
        default:
            return Optional.empty();
        }
    }

    @Override
    public Optional<ArbitraryInt> findLength() {
        var c = lowColumn.getLengthOptCase();
        if (c == null) {
            return Optional.empty();
        }
        switch (c) {
        case LENGTH:
            return Optional.of(ArbitraryInt.of(lowColumn.getLength()));
        case ARBITRARY_LENGTH:
            return Optional.of(ArbitraryInt.ofArbitrary());
        case LENGTHOPT_NOT_SET:
        default:
            return Optional.empty();
        }
    }

    @Override
    public Optional<ArbitraryInt> findPrecision() {
        var c = lowColumn.getPrecisionOptCase();
        if (c == null) {
            return Optional.empty();
        }
        switch (c) {
        case PRECISION:
            return Optional.of(ArbitraryInt.of(lowColumn.getPrecision()));
        case ARBITRARY_PRECISION:
            return Optional.of(ArbitraryInt.ofArbitrary());
        case PRECISIONOPT_NOT_SET:
        default:
            return Optional.empty();
        }
    }

    @Override
    public Optional<ArbitraryInt> findScale() {
        var c = lowColumn.getScaleOptCase();
        if (c == null) {
            return Optional.empty();
        }
        switch (c) {
        case SCALE:
            return Optional.of(ArbitraryInt.of(lowColumn.getScale()));
        case ARBITRARY_SCALE:
            return Optional.of(ArbitraryInt.ofArbitrary());
        case SCALEOPT_NOT_SET:
        default:
            return Optional.empty();
        }
    }

    @Override
    public Optional<Boolean> findNullable() {
        var c = lowColumn.getNullableOptCase();
        if (c == null) {
            return Optional.empty();
        }
        switch (c) {
        case NULLABLE:
            return Optional.of(lowColumn.getNullable());
        case NULLABLEOPT_NOT_SET:
        default:
            return Optional.empty();
        }
    }
}
