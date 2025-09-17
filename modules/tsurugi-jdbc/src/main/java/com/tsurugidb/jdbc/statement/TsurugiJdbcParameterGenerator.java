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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.resultset.type.TsurugiJdbcBlobReference;
import com.tsurugidb.jdbc.resultset.type.TsurugiJdbcClobReference;
import com.tsurugidb.jdbc.util.TsurugiJdbcConvertUtil;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.sql.Parameters;

public class TsurugiJdbcParameterGenerator {

    private final TsurugiJdbcPreparedStatement ownerPreparedStatement;
    private TsurugiJdbcConvertUtil convertUtil = null;

    public TsurugiJdbcParameterGenerator(TsurugiJdbcPreparedStatement ownerPreparedStatement) {
        this.ownerPreparedStatement = ownerPreparedStatement;
    }

    public void setConvertUtil(@Nonnull TsurugiJdbcConvertUtil convertUtil) {
        this.convertUtil = Objects.requireNonNull(convertUtil);
    }

    protected TsurugiJdbcConvertUtil getConvertUtil() {
        if (this.convertUtil == null) {
            this.convertUtil = getFactory().createConvertUtil(ownerPreparedStatement);
        }
        return this.convertUtil;
    }

    protected TsurugiJdbcFactory getFactory() {
        return ownerPreparedStatement.getFactory();
    }

    protected TsurugiJdbcExceptionHandler getExceptionHandler() {
        return getFactory().getExceptionHandler();
    }

    public Parameter create(String name, boolean value) throws SQLException {
        return Parameters.of(name, value);
    }

    public Parameter create(String name, byte value) throws SQLException {
        return Parameters.of(name, value);
    }

    public Parameter create(String name, short value) throws SQLException {
        return Parameters.of(name, value);
    }

    public Parameter create(String name, int value) throws SQLException {
        return Parameters.of(name, value);
    }

    public Parameter create(String name, long value) throws SQLException {
        return Parameters.of(name, value);
    }

    public Parameter create(String name, float value) throws SQLException {
        return Parameters.of(name, value);
    }

    public Parameter create(String name, double value) throws SQLException {
        return Parameters.of(name, value);
    }

    public Parameter create(String name, BigDecimal value) throws SQLException {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public Parameter create(String name, String value) throws SQLException {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public Parameter create(String name, byte[] value) throws SQLException {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public Parameter create(String name, LocalDate value) throws SQLException {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        return Parameters.of(name, value);
    }

    public Parameter create(String name, LocalTime value) throws SQLException {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        return Parameters.of(name, value);
    }

    public Parameter create(String name, LocalDateTime value) throws SQLException {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        return Parameters.of(name, value);
    }

    public Parameter create(String name, OffsetTime value) throws SQLException {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        return Parameters.of(name, value);
    }

    public Parameter create(String name, OffsetDateTime value) throws SQLException {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        return Parameters.of(name, value);
    }

    public Parameter create(String name, java.sql.Date value) throws SQLException {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        LocalDate x = convertUtil.convertToLocalDate(value);
        return Parameters.of(name, x);
    }

    public Parameter create(String name, java.sql.Time value) throws SQLException {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        LocalTime x = convertUtil.convertToLocalTime(value);
        return Parameters.of(name, x);
    }

    public Parameter create(String name, java.sql.Timestamp value) throws SQLException {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        LocalDateTime x = convertUtil.convertToLocalDateTime(value);
        return Parameters.of(name, x);
    }

    public Parameter createCharacterStream(String name, Reader value, int length) throws SQLException {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        try {
            String x = convertUtil.convertToString(value, length);
            return Parameters.of(name, x);
        } catch (Exception e) {
            throw getExceptionHandler().dataException("Create parameter of CharacterStream error", e);
        }
    }

    public Parameter createAsciiStream(String name, InputStream value, int length) throws SQLException {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        try {
            byte[] bytes = (length >= 0) ? value.readNBytes(length) : value.readAllBytes();
            var x = new String(bytes, StandardCharsets.UTF_8);
            return Parameters.of(name, x);
        } catch (Exception e) {
            throw getExceptionHandler().dataException("Create parameter of AsciiStream error", e);
        }
    }

    public Parameter createUnicodeStream(String name, InputStream value, int length) throws SQLException {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        try {
            byte[] bytes = value.readNBytes(length);
            var x = new String(bytes, StandardCharsets.UTF_16);
            return Parameters.of(name, x);
        } catch (Exception e) {
            throw getExceptionHandler().dataException("Create parameter of UnicodeStream error", e);
        }
    }

    public Parameter createBinaryStream(String name, InputStream value, int length) throws SQLException {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        try {
            byte[] x = (length >= 0) ? value.readNBytes(length) : value.readAllBytes();
            return create(name, x);
        } catch (Exception e) {
            throw getExceptionHandler().dataException("Create parameter of BinaryStream error", e);
        }
    }

    public Parameter create(String name, Object value, AtomType atomType) throws SQLException {
        if (value == null) {
            return Parameters.ofNull(name);
        }

        var util = getConvertUtil();
        switch (atomType) {
        case BOOLEAN:
            return create(name, util.convertToBoolean(value));
        case INT4:
            return create(name, util.convertToInt(value));
        case INT8:
            return create(name, util.convertToLong(value));
        case FLOAT4:
            return create(name, util.convertToFloat(value));
        case FLOAT8:
            return create(name, util.convertToDouble(value));
        case DECIMAL:
            return create(name, util.convertToDecimal(value));
        case CHARACTER:
            return create(name, util.convertToString(value));
        case OCTET:
            return create(name, util.convertToBytes(value));
        case DATE:
            return create(name, util.convertToLocalDate(value));
        case TIME_OF_DAY:
            return create(name, util.convertToLocalTime(value));
        case TIME_POINT:
            return create(name, util.convertToLocalDateTime(value));
        case TIME_OF_DAY_WITH_TIME_ZONE:
            return create(name, util.convertToOffsetTime(value));
        case TIME_POINT_WITH_TIME_ZONE:
            return create(name, util.convertToOffsetDateTime(value));
        default:
            var e = new UnsupportedOperationException(MessageFormat.format("Unsupported AtomType.{0}", atomType));
            throw getExceptionHandler().dataException("Create parameter error", e);
        }
    }

    private static final Map<Class<?>, AtomType> TYPE_MAP;
    static {
        var map = new LinkedHashMap<Class<?>, AtomType>(32);
        map.put(boolean.class, AtomType.BOOLEAN);
        map.put(byte.class, AtomType.INT4);
        map.put(short.class, AtomType.INT4);
        map.put(int.class, AtomType.INT4);
        map.put(long.class, AtomType.INT8);
        map.put(float.class, AtomType.FLOAT4);
        map.put(double.class, AtomType.FLOAT8);
        map.put(Boolean.class, AtomType.BOOLEAN);
        map.put(Byte.class, AtomType.INT4);
        map.put(Short.class, AtomType.INT4);
        map.put(Integer.class, AtomType.INT4);
        map.put(Long.class, AtomType.INT8);
        map.put(Float.class, AtomType.FLOAT4);
        map.put(Double.class, AtomType.FLOAT8);
        map.put(BigDecimal.class, AtomType.DECIMAL);

        map.put(String.class, AtomType.CHARACTER);
        map.put(byte[].class, AtomType.OCTET);

        map.put(java.sql.Date.class, AtomType.DATE);
        map.put(java.sql.Time.class, AtomType.TIME_OF_DAY);
        map.put(java.sql.Timestamp.class, AtomType.TIME_POINT);
        map.put(LocalDate.class, AtomType.DATE);
        map.put(LocalTime.class, AtomType.TIME_OF_DAY);
        map.put(LocalDateTime.class, AtomType.TIME_POINT);
        map.put(OffsetTime.class, AtomType.TIME_OF_DAY_WITH_TIME_ZONE);
        map.put(OffsetDateTime.class, AtomType.TIME_POINT_WITH_TIME_ZONE);

        map.put(java.sql.Blob.class, AtomType.BLOB);
        map.put(java.sql.Clob.class, AtomType.CLOB);
        map.put(TsurugiJdbcBlobReference.class, AtomType.BLOB);
        map.put(TsurugiJdbcClobReference.class, AtomType.CLOB);
        TYPE_MAP = map;
    }

    public AtomType toAtomType(Class<?> type) throws SQLException {
        var atomType = TYPE_MAP.get(type);
        if (atomType != null) {
            return atomType;
        }

        for (var entry : TYPE_MAP.entrySet()) {
            if (entry.getKey().isAssignableFrom(type)) {
                return entry.getValue();
            }
        }

        throw getExceptionHandler().dataTypeMismatchException("Unsupported type", type);
    }
}
