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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.tsurugidb.jdbc.resultset.type.TsurugiJdbcBlobReference;
import com.tsurugidb.jdbc.resultset.type.TsurugiJdbcClobReference;

/**
 * Convert Tsubakuro value to specified value.
 */
public class TsurugiJdbcResultSetConverter {

    public String convertToString(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        try {
            var result = convertToString(value);
            if (result != null) {
                return result;
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            var factory = owner.getFactory();
            throw factory.getExceptionHandler().sqlException("convertToString error", e); // TODO DataException
        }

        throw new SQLException(); // TODO TsurugiJdbcExceptionHandler
    }

    protected String convertToString(@Nonnull Object value) throws SQLException {
        if (value instanceof String) {
            return (String) value;
        }

        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString();
        }

        if (value instanceof java.sql.Clob) {
            var reader = ((java.sql.Clob) value).getCharacterStream();
            try (var br = new BufferedReader(reader)) {
                var sb = new StringBuilder();
                var buffer = new char[1024];
                for (;;) {
                    int length = br.read(buffer);
                    if (length < 0) {
                        break;
                    }
                    sb.append(buffer, 0, length);
                }
                return sb.toString();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }

        if (value instanceof java.sql.Blob) {
            return null;
        }

        return value.toString();
    }

    public Reader convertToReader(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        try {
            var result = convertToReader(value);
            if (result != null) {
                return result;
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            var factory = owner.getFactory();
            throw factory.getExceptionHandler().sqlException("convertToString error", e); // TODO DataException
        }

        throw new SQLException(); // TODO TsurugiJdbcExceptionHandler
    }

    protected Reader convertToReader(@Nonnull Object value) throws SQLException {
        if (value instanceof java.sql.Clob) {
            return ((java.sql.Clob) value).getCharacterStream();
        }

        String s = convertToString(value);
        if (s != null) {
            return new StringReader(s);
        }

        return null;
    }

    public boolean convertToBoolean(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            return !((String) value).equals("0"); // TODO "1" ?
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() != 0; // TODO 1 ?
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public byte convertToByte(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        if (value instanceof Byte) {
            return (Byte) value;
        }
        if (value instanceof Number) {
            return ((Number) value).byteValue();
        }
        if (value instanceof String) {
            try {
                return Byte.parseByte((String) value);
            } catch (NumberFormatException e) {
                throw new SQLException(e); // TODO TsurugiSQLExceptionHandler
            }
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public short convertToShort(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        if (value instanceof Short) {
            return (Short) value;
        }
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }
        if (value instanceof String) {
            try {
                return Short.parseShort((String) value);
            } catch (NumberFormatException e) {
                throw new SQLException(e); // TODO TsurugiSQLExceptionHandler
            }
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public int convertToInt(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                throw new SQLException(e); // TODO TsurugiSQLExceptionHandler
            }
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public long convertToLong(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                throw new SQLException(e); // TODO TsurugiSQLExceptionHandler
            }
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public float convertToFloat(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        if (value instanceof Float) {
            return (Float) value;
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        if (value instanceof String) {
            try {
                return Float.parseFloat((String) value);
            } catch (NumberFormatException e) {
                throw new SQLException(e); // TODO TsurugiSQLExceptionHandler
            }
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public double convertToDouble(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        if (value instanceof Double) {
            return (Double) value;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                throw new SQLException(e); // TODO TsurugiSQLExceptionHandler
            }
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public BigDecimal convertToDecimal(TsurugiJdbcResultSet owner, Object value, int scale) throws SQLException {
        BigDecimal result = convertToDecimal(owner, value);
        return result.setScale(scale);
    }

    public BigDecimal convertToDecimal(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return BigDecimal.ZERO;
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Integer) {
            return BigDecimal.valueOf((Integer) value);
        }
        if (value instanceof Long) {
            return BigDecimal.valueOf((Long) value);
        }
        if (value instanceof Float) {
            return BigDecimal.valueOf((Float) value);
        }
        if (value instanceof Double) {
            return BigDecimal.valueOf((Double) value);
        }
        if (value instanceof Number) {
            return BigDecimal.valueOf(((Number) value).longValue());
        }
        if (value instanceof String) {
            try {
                return new BigDecimal((String) value);
            } catch (NumberFormatException e) {
                throw new SQLException(e); // TODO TsurugiSQLExceptionHandler
            }
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public byte[] convertToBytes(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        try {
            var result = convertToBytes(value);
            if (result != null) {
                return result;
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            var factory = owner.getFactory();
            throw factory.getExceptionHandler().sqlException("convertToString error", e); // TODO DataException
        }

        throw new SQLException(); // TODO TsurugiJdbcExceptionHandler
    }

    protected byte[] convertToBytes(@Nonnull Object value) throws SQLException {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }

        if (value instanceof java.sql.Blob) {
            try (var is = ((java.sql.Blob) value).getBinaryStream()) {
                return is.readAllBytes();
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }

        return null;
    }

    public InputStream convertToInputStream(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        try {
            var result = convertToInputStream(value);
            if (result != null) {
                return result;
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            var factory = owner.getFactory();
            throw factory.getExceptionHandler().sqlException("convertToString error", e); // TODO DataException
        }

        throw new SQLException(); // TODO TsurugiJdbcExceptionHandler
    }

    protected InputStream convertToInputStream(@Nonnull Object value) throws SQLException {
        if (value instanceof java.sql.Blob) {
            return ((java.sql.Blob) value).getBinaryStream();
        }

        if (value instanceof byte[]) {
            return new ByteArrayInputStream((byte[]) value);
        }

        return null;
    }

    public java.sql.Blob convertToBlob(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof java.sql.Blob) {
            return (java.sql.Blob) value;
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public java.sql.Clob convertToClob(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof java.sql.Clob) {
            return (java.sql.Clob) value;
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public java.sql.Date convertToDate(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof LocalDate) {
            return java.sql.Date.valueOf((LocalDate) value);
        }
        if (value instanceof LocalDateTime) {
            var timestamp = java.sql.Timestamp.valueOf((LocalDateTime) value);
            return new java.sql.Date(timestamp.getTime());
        }
        if (value instanceof OffsetDateTime) {
            var instant = ((OffsetDateTime) value).toInstant();
            var timestamp = java.sql.Timestamp.from(instant);
            return new java.sql.Date(timestamp.getTime());
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public java.sql.Time convertToTime(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof LocalTime) {
            return java.sql.Time.valueOf((LocalTime) value);
        }
        if (value instanceof OffsetTime) {
            var localTime = ((OffsetTime) value).toLocalTime();
            return java.sql.Time.valueOf(localTime);
        }
        if (value instanceof LocalDateTime) {
            var localTime = ((LocalDateTime) value).toLocalTime();
            return java.sql.Time.valueOf(localTime);
        }
        if (value instanceof OffsetDateTime) {
            var localTime = ((OffsetDateTime) value).toLocalTime();
            return java.sql.Time.valueOf(localTime);
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public java.sql.Timestamp convertToTimestamp(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof LocalDateTime) {
            return java.sql.Timestamp.valueOf((LocalDateTime) value);
        }
        if (value instanceof OffsetDateTime) {
            var instant = ((OffsetDateTime) value).toInstant();
            return java.sql.Timestamp.from(instant);
        }
        if (value instanceof LocalDate) {
            var localDateTime = ((LocalDate) value).atStartOfDay();
            return java.sql.Timestamp.valueOf(localDateTime);
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public LocalDate convertToLocalDate(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof LocalDate) {
            return (LocalDate) value;
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalDate();
        }
        if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value).toLocalDate();
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public LocalTime convertToLocalTime(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof LocalTime) {
            return (LocalTime) value;
        }
        if (value instanceof OffsetTime) {
            return ((OffsetTime) value).toLocalTime();
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalTime();
        }
        if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value).toLocalTime();
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public LocalDateTime convertToLocalDateTime(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        if (value instanceof LocalDate) {
            return LocalDateTime.of((LocalDate) value, LocalTime.MIN);
        }
        if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value).toLocalDateTime();
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public OffsetTime convertToOffsetTime(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof OffsetTime) {
            return (OffsetTime) value;
        }
        if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value).toOffsetTime();
        }
        if (value instanceof LocalTime) {
            return OffsetTime.of((LocalTime) value, ZoneOffset.UTC);
        }
        if (value instanceof LocalDateTime) {
            var localTime = ((LocalDateTime) value).toLocalTime();
            return OffsetTime.of(localTime, ZoneOffset.UTC);
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public OffsetDateTime convertToOffsetDateTime(TsurugiJdbcResultSet owner, Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof OffsetDateTime) {
            return (OffsetDateTime) value;
        }
        if (value instanceof LocalDateTime) {
            return OffsetDateTime.of((LocalDateTime) value, ZoneOffset.UTC);
        }
        if (value instanceof LocalDate) {
            var localDateTime = LocalDateTime.of((LocalDate) value, LocalTime.MIN);
            return OffsetDateTime.of(localDateTime, ZoneOffset.UTC);
        }
        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }

    public <T> T convertToType(TsurugiJdbcResultSet owner, Object value, Class<T> type) throws SQLException {
        var converter = findConveter(type);
        var object = converter.convert(this, owner, value);
        return cast(object);
    }

    @SuppressWarnings("unchecked")
    private static <T> T cast(Object value) {
        return (T) value;
    }

    @FunctionalInterface
    private interface Converter {
        public Object convert(TsurugiJdbcResultSetConverter converter, TsurugiJdbcResultSet owner, Object value) throws SQLException;
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
        map.put(java.sql.Blob.class, TsurugiJdbcResultSetConverter::convertToBlob);
        map.put(java.sql.Clob.class, TsurugiJdbcResultSetConverter::convertToClob);
        map.put(TsurugiJdbcBlobReference.class, TsurugiJdbcResultSetConverter::convertToBlob);
        map.put(TsurugiJdbcClobReference.class, TsurugiJdbcResultSetConverter::convertToClob);
        map.put(java.sql.Date.class, TsurugiJdbcResultSetConverter::convertToDate);
        map.put(java.sql.Time.class, TsurugiJdbcResultSetConverter::convertToTime);
        map.put(java.sql.Timestamp.class, TsurugiJdbcResultSetConverter::convertToTimestamp);
        map.put(LocalDate.class, TsurugiJdbcResultSetConverter::convertToLocalDate);
        map.put(LocalTime.class, TsurugiJdbcResultSetConverter::convertToLocalTime);
        map.put(LocalDateTime.class, TsurugiJdbcResultSetConverter::convertToLocalDateTime);
        map.put(OffsetTime.class, TsurugiJdbcResultSetConverter::convertToOffsetTime);
        map.put(OffsetDateTime.class, TsurugiJdbcResultSetConverter::convertToOffsetDateTime);
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

        throw new SQLException(); // TODO TsurugiSQLExceptionHandler
    }
}
