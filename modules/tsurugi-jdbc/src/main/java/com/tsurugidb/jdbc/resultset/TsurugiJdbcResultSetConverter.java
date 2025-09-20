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
package com.tsurugidb.jdbc.resultset;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
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
import com.tsurugidb.jdbc.factory.HasFactory;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.resultset.type.TsurugiJdbcBlobReference;
import com.tsurugidb.jdbc.resultset.type.TsurugiJdbcClobReference;
import com.tsurugidb.jdbc.util.TsurugiJdbcConvertUtil;

/**
 * Convert Tsubakuro value to specified value.
 */
public class TsurugiJdbcResultSetConverter {

    private final HasFactory ownerResultSet;
    private TsurugiJdbcConvertUtil convertUtil;

    public TsurugiJdbcResultSetConverter(HasFactory ownerResultSet) {
        this.ownerResultSet = ownerResultSet;
        var factory = ownerResultSet.getFactory();
        this.convertUtil = factory.createConvertUtil(ownerResultSet);
    }

    public void setConvertUtil(@Nonnull TsurugiJdbcConvertUtil convertUtil) {
        this.convertUtil = Objects.requireNonNull(convertUtil);
    }

    protected TsurugiJdbcFactory getFactory() {
        return ownerResultSet.getFactory();
    }

    protected TsurugiJdbcExceptionHandler getExceptionHandler() {
        return getFactory().getExceptionHandler();
    }

    public String convertToString(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToString(value);
    }

    public boolean convertToBoolean(Object value) throws SQLException {
        if (value == null) {
            return false;
        }

        return convertUtil.convertToBoolean(value);
    }

    public byte convertToByte(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }

        return convertUtil.convertToByte(value);
    }

    public short convertToShort(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }

        return convertUtil.convertToShort(value);
    }

    public int convertToInt(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }

        return convertUtil.convertToInt(value);
    }

    public long convertToLong(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }

        return convertUtil.convertToLong(value);
    }

    public float convertToFloat(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }

        return convertUtil.convertToFloat(value);
    }

    public double convertToDouble(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }

        return convertUtil.convertToDouble(value);
    }

    public BigDecimal convertToDecimal(Object value, int scale) throws SQLException {
        BigDecimal result = convertToDecimal(value);
        return result.setScale(scale);
    }

    public BigDecimal convertToDecimal(Object value) throws SQLException {
        if (value == null) {
            return BigDecimal.ZERO;
        }

        return convertUtil.convertToDecimal(value);
    }

    public byte[] convertToBytes(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToBytes(value);
    }

    public java.sql.Date convertToDate(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToDate(value);
    }

    public java.sql.Time convertToTime(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToTime(value);
    }

    public java.sql.Timestamp convertToTimestamp(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToTimestamp(value);
    }

    public LocalDate convertToLocalDate(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToLocalDate(value);
    }

    public LocalTime convertToLocalTime(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToLocalTime(value);
    }

    public LocalDateTime convertToLocalDateTime(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToLocalDateTime(value);
    }

    public OffsetTime convertToOffsetTime(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToOffsetTime(value);
    }

    public OffsetDateTime convertToOffsetDateTime(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToOffsetDateTime(value);
    }

    public Reader convertToCharacterStream(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToReader(value);
    }

    public InputStream convertToAsciiStream(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        String s = convertToString(value);
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return new ByteArrayInputStream(bytes);
    }

    public InputStream convertToUnicodeStream(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        String s = convertToString(value);
        byte[] bytes = s.getBytes(StandardCharsets.UTF_16);
        return new ByteArrayInputStream(bytes);
    }

    public InputStream convertToBinaryStream(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToInputStream(value);
    }

    public java.sql.Blob convertToBlob(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToBlob(value);
    }

    public java.sql.Clob convertToClob(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToClob(value);
    }

    public <T> T convertToType(Object value, Class<T> type) throws SQLException {
        var converter = findConveter(type); // First, check the type
        assert converter != null;

        if (value == null) {
            return null;
        }

        @SuppressWarnings("unchecked")
        T result = (T) converter.convert(this, value);
        return result;
    }

    @FunctionalInterface
    private interface Converter {
        public Object convert(TsurugiJdbcResultSetConverter converter, Object value) throws SQLException;
    }

    private static final Map<Class<?>, Converter> CONVERTER_MAP;
    static {
        var map = new LinkedHashMap<Class<?>, Converter>(32);
        map.put(boolean.class, TsurugiJdbcResultSetConverter::convertToBoolean);
        map.put(byte.class, TsurugiJdbcResultSetConverter::convertToByte);
        map.put(short.class, TsurugiJdbcResultSetConverter::convertToShort);
        map.put(int.class, TsurugiJdbcResultSetConverter::convertToInt);
        map.put(long.class, TsurugiJdbcResultSetConverter::convertToLong);
        map.put(float.class, TsurugiJdbcResultSetConverter::convertToFloat);
        map.put(double.class, TsurugiJdbcResultSetConverter::convertToDouble);
        map.put(Boolean.class, TsurugiJdbcResultSetConverter::convertToBoolean);
        map.put(Byte.class, TsurugiJdbcResultSetConverter::convertToByte);
        map.put(Short.class, TsurugiJdbcResultSetConverter::convertToShort);
        map.put(Integer.class, TsurugiJdbcResultSetConverter::convertToInt);
        map.put(Long.class, TsurugiJdbcResultSetConverter::convertToLong);
        map.put(Float.class, TsurugiJdbcResultSetConverter::convertToFloat);
        map.put(Double.class, TsurugiJdbcResultSetConverter::convertToDouble);
        map.put(BigDecimal.class, TsurugiJdbcResultSetConverter::convertToDecimal);

        map.put(String.class, TsurugiJdbcResultSetConverter::convertToString);
        map.put(byte[].class, TsurugiJdbcResultSetConverter::convertToBytes);

        map.put(java.sql.Date.class, TsurugiJdbcResultSetConverter::convertToDate);
        map.put(java.sql.Time.class, TsurugiJdbcResultSetConverter::convertToTime);
        map.put(java.sql.Timestamp.class, TsurugiJdbcResultSetConverter::convertToTimestamp);
        map.put(LocalDate.class, TsurugiJdbcResultSetConverter::convertToLocalDate);
        map.put(LocalTime.class, TsurugiJdbcResultSetConverter::convertToLocalTime);
        map.put(LocalDateTime.class, TsurugiJdbcResultSetConverter::convertToLocalDateTime);
        map.put(OffsetTime.class, TsurugiJdbcResultSetConverter::convertToOffsetTime);
        map.put(OffsetDateTime.class, TsurugiJdbcResultSetConverter::convertToOffsetDateTime);

        map.put(java.sql.Blob.class, TsurugiJdbcResultSetConverter::convertToBlob);
        map.put(java.sql.Clob.class, TsurugiJdbcResultSetConverter::convertToClob);
        map.put(TsurugiJdbcBlobReference.class, TsurugiJdbcResultSetConverter::convertToBlob);
        map.put(TsurugiJdbcClobReference.class, TsurugiJdbcResultSetConverter::convertToClob);
        CONVERTER_MAP = map;
    }

    protected Converter findConveter(Class<?> type) throws SQLException {
        var converter = CONVERTER_MAP.get(type);
        if (converter != null) {
            return converter;
        }

        for (var entry : CONVERTER_MAP.entrySet()) {
            if (entry.getKey().isAssignableFrom(type)) {
                return entry.getValue();
            }
        }

        throw getExceptionHandler().dataTypeMismatchException("Unsupported type", type);
    }
}
