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
package com.tsurugidb.jdbc.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.factory.HasFactory;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;

public class TsurugiJdbcConvertUtil {

    private final HasFactory hasFactory;

    public TsurugiJdbcConvertUtil(@Nonnull HasFactory hasFactory) {
        this.hasFactory = Objects.requireNonNull(hasFactory);
    }

    protected TsurugiJdbcFactory getFactory() {
        return hasFactory.getFactory();
    }

    protected TsurugiJdbcExceptionHandler getExceptionHandler() {
        return getFactory().getExceptionHandler();
    }

    public boolean convertToBoolean(@Nonnull Object value) throws SQLException {
        try {
            return convertToBooleanMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToBoolean error", e);
        }
    }

    protected boolean convertToBooleanMain(@Nonnull Object value) throws Exception {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            return !((String) value).equals("0"); // TODO "1" ?
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() != 0; // TODO 1 ?
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToBoolean unsupported type", value.getClass());
    }

    public byte convertToByte(@Nonnull Object value) throws SQLException {
        try {
            return convertToByteMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToByte error", e);
        }
    }

    protected byte convertToByteMain(@Nonnull Object value) throws Exception {
        if (value instanceof Byte) {
            return (Byte) value;
        }
        if (value instanceof Number) {
            return ((Number) value).byteValue();
        }
        if (value instanceof String) {
            return Byte.parseByte((String) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToByte unsupported type", value.getClass());
    }

    public short convertToShort(@Nonnull Object value) throws SQLException {
        try {
            return convertToShortMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToShort error", e);
        }
    }

    protected short convertToShortMain(@Nonnull Object value) throws Exception {
        if (value instanceof Short) {
            return (Short) value;
        }
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }
        if (value instanceof String) {
            return Short.parseShort((String) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToShort unsupported type", value.getClass());
    }

    public int convertToInt(@Nonnull Object value) throws SQLException {
        try {
            return convertToIntMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToInt error", e);
        }
    }

    protected int convertToIntMain(@Nonnull Object value) throws Exception {
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToInt unsupported type", value.getClass());
    }

    public long convertToLong(@Nonnull Object value) throws SQLException {
        try {
            return convertToLongMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToLong error", e);
        }
    }

    protected long convertToLongMain(@Nonnull Object value) throws Exception {
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            return Long.parseLong((String) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToLong unsupported type", value.getClass());
    }

    public float convertToFloat(@Nonnull Object value) throws SQLException {
        try {
            return convertToFloatMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToFloat error", e);
        }
    }

    protected float convertToFloatMain(@Nonnull Object value) throws Exception {
        if (value instanceof Float) {
            return (Float) value;
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        if (value instanceof String) {
            return Float.parseFloat((String) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToFloat unsupported type", value.getClass());
    }

    public double convertToDouble(@Nonnull Object value) throws SQLException {
        try {
            return convertToDoubleMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToDouble error", e);
        }
    }

    protected double convertToDoubleMain(@Nonnull Object value) throws Exception {
        if (value instanceof Double) {
            return (Double) value;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            return Double.parseDouble((String) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToDouble unsupported type", value.getClass());
    }

    public BigDecimal convertToDecimal(@Nonnull Object value) throws SQLException {
        try {
            return convertToDecimalMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToDecimal error", e);
        }
    }

    protected BigDecimal convertToDecimalMain(@Nonnull Object value) throws Exception {
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
            return new BigDecimal((String) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToDecimal unsupported type", value.getClass());
    }

    public String convertToString(@Nonnull Object value) throws SQLException {
        try {
            return convertToStringMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToString error", e);
        }
    }

    protected String convertToStringMain(@Nonnull Object value) throws Exception {
        if (value instanceof String) {
            return (String) value;
        }

        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString();
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof Temporal) {
            return value.toString();
        }
        if (value instanceof java.sql.Date || value instanceof java.sql.Time || value instanceof java.sql.Timestamp) {
            return value.toString();
        }

        if (value instanceof java.sql.Clob) {
            try (var reader = ((java.sql.Clob) value).getCharacterStream()) {
                return convertToString(reader, -1);
            }
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToString unsupported type", value.getClass());
    }

    public String convertToString(@Nonnull Reader reader, int length) throws SQLException {
        try (var br = new BufferedReader(reader)) {
            var sb = new StringBuilder();
            var buffer = new char[1024];
            for (;;) {
                int len = br.read(buffer);
                if (len < 0) {
                    break;
                }
                sb.append(buffer, 0, len);

                if (length >= 0) {
                    if (sb.length() >= length) {
                        break;
                    }
                }
            }

            if (length >= 0 && sb.length() > length) {
                sb.setLength(length);
            }
            return sb.toString();
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToString error", e);
        }
    }

    public byte[] convertToBytes(@Nonnull Object value) throws SQLException {
        try {
            return convertToBytesMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToBytes error", e);
        }
    }

    protected byte[] convertToBytesMain(@Nonnull Object value) throws Exception {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }

        if (value instanceof java.sql.Blob) {
            try (var is = ((java.sql.Blob) value).getBinaryStream()) {
                return is.readAllBytes();
            }
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToBytes unsupported type", value.getClass());
    }

    public java.sql.Date convertToDate(@Nonnull Object value) throws SQLException {
        try {
            return convertToDateMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToDate error", e);
        }
    }

    protected java.sql.Date convertToDateMain(@Nonnull Object value) throws Exception {
        if (value instanceof java.sql.Date) {
            return (java.sql.Date) value;
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

        throw getExceptionHandler().dataTypeMismatchException("convertToDate unsupported type", value.getClass());
    }

    public java.sql.Time convertToTime(@Nonnull Object value) throws SQLException {
        try {
            return convertToTimeMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToTime error", e);
        }
    }

    protected java.sql.Time convertToTimeMain(@Nonnull Object value) throws Exception {
        if (value instanceof java.sql.Time) {
            return (java.sql.Time) value;
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

        throw getExceptionHandler().dataTypeMismatchException("convertToTime unsupported type", value.getClass());
    }

    public java.sql.Timestamp convertToTimestamp(@Nonnull Object value) throws SQLException {
        try {
            return convertToTimestampMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToTimestamp error", e);
        }
    }

    protected java.sql.Timestamp convertToTimestampMain(@Nonnull Object value) throws Exception {
        if (value instanceof java.sql.Timestamp) {
            return (java.sql.Timestamp) value;
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

        throw getExceptionHandler().dataTypeMismatchException("convertToTimestamp unsupported type", value.getClass());
    }

    public LocalDate convertToLocalDate(@Nonnull Object value) throws SQLException {
        try {
            return convertToLocalDateMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToLocalDate error", e);
        }
    }

    protected LocalDate convertToLocalDateMain(@Nonnull Object value) throws Exception {
        if (value instanceof LocalDate) {
            return (LocalDate) value;
        }
        if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate();
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalDate();
        }
        if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value).toLocalDate();
        }
        if (value instanceof java.sql.Timestamp) {
            var localDateTime = ((java.sql.Timestamp) value).toLocalDateTime();
            return localDateTime.toLocalDate();
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToLocalDate unsupported type", value.getClass());
    }

    public LocalTime convertToLocalTime(@Nonnull Object value) throws SQLException {
        try {
            return convertToLocalTimeMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToLocalTime error", e);
        }
    }

    protected LocalTime convertToLocalTimeMain(@Nonnull Object value) throws Exception {
        if (value instanceof LocalTime) {
            return (LocalTime) value;
        }
        if (value instanceof java.sql.Time) {
            return ((java.sql.Time) value).toLocalTime();
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
        if (value instanceof java.sql.Timestamp) {
            var localDateTime = ((java.sql.Timestamp) value).toLocalDateTime();
            return localDateTime.toLocalTime();
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToLocalTime unsupported type", value.getClass());
    }

    public LocalDateTime convertToLocalDateTime(@Nonnull Object value) throws SQLException {
        try {
            return convertToLocalDateTimeMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToLocalDateTime error", e);
        }
    }

    protected LocalDateTime convertToLocalDateTimeMain(@Nonnull Object value) throws Exception {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toLocalDateTime();
        }
        if (value instanceof LocalDate) {
            return LocalDateTime.of((LocalDate) value, LocalTime.MIN);
        }
        if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value).toLocalDateTime();
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToLocalDateTime unsupported type", value.getClass());
    }

    public OffsetTime convertToOffsetTime(@Nonnull Object value) throws SQLException {
        try {
            return convertToOffsetTimeMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToOffsetTime error", e);
        }
    }

    protected OffsetTime convertToOffsetTimeMain(@Nonnull Object value) throws Exception {
        if (value instanceof OffsetTime) {
            return (OffsetTime) value;
        }
        if (value instanceof java.sql.Time) {
            var localTime = ((java.sql.Time) value).toLocalTime();
            return OffsetTime.of(localTime, ZoneOffset.UTC);
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
        if (value instanceof java.sql.Timestamp) {
            var localDateTime = ((java.sql.Timestamp) value).toLocalDateTime();
            return OffsetTime.of(localDateTime.toLocalTime(), ZoneOffset.UTC);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToOffsetTime unsupported type", value.getClass());
    }

    public OffsetDateTime convertToOffsetDateTime(@Nonnull Object value) throws SQLException {
        try {
            return convertToOffsetDateTimeMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToOffsetDateTime error", e);
        }
    }

    protected OffsetDateTime convertToOffsetDateTimeMain(@Nonnull Object value) throws Exception {
        if (value instanceof OffsetDateTime) {
            return (OffsetDateTime) value;
        }
        if (value instanceof java.sql.Timestamp) {
            var timestamp = (java.sql.Timestamp) value;
            var localDateTime = timestamp.toLocalDateTime();
            return OffsetDateTime.of(localDateTime, ZoneOffset.UTC);
        }
        if (value instanceof LocalDateTime) {
            return OffsetDateTime.of((LocalDateTime) value, ZoneOffset.UTC);
        }
        if (value instanceof LocalDate) {
            var localDateTime = LocalDateTime.of((LocalDate) value, LocalTime.MIN);
            return OffsetDateTime.of(localDateTime, ZoneOffset.UTC);
        }
        if (value instanceof java.sql.Date) {
            var localDate = ((java.sql.Date) value).toLocalDate();
            var localDateTime = LocalDateTime.of(localDate, LocalTime.MIN);
            return OffsetDateTime.of(localDateTime, ZoneOffset.UTC);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToOffsetDateTime unsupported type", value.getClass());
    }

    public Reader convertToReader(@Nonnull Object value) throws SQLException {
        try {
            return convertToReaderMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToReader error", e);
        }
    }

    protected Reader convertToReaderMain(@Nonnull Object value) throws Exception {
        if (value instanceof Reader) {
            return (Reader) value;
        }
        if (value instanceof java.sql.Clob) {
            return ((java.sql.Clob) value).getCharacterStream();
        }

        try {
            String s = convertToStringMain(value);
            return new StringReader(s);
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToReader error", e);
        }
    }

    public InputStream convertToInputStream(@Nonnull Object value) throws SQLException {
        try {
            return convertToInputStreamMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToInputStream error", e);
        }
    }

    protected InputStream convertToInputStreamMain(@Nonnull Object value) throws Exception {
        if (value instanceof InputStream) {
            return (InputStream) value;
        }
        if (value instanceof byte[]) {
            return new ByteArrayInputStream((byte[]) value);
        }
        if (value instanceof java.sql.Blob) {
            return ((java.sql.Blob) value).getBinaryStream();
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToInputStream unsupported type", value.getClass());
    }

    public java.sql.Blob convertToBlob(@Nonnull Object value) throws SQLException {
        try {
            return convertToBlobMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToBlob error", e);
        }
    }

    protected java.sql.Blob convertToBlobMain(@Nonnull Object value) throws Exception {
        if (value instanceof java.sql.Blob) {
            return (java.sql.Blob) value;
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToBlob unsupported type", value.getClass());
    }

    public java.sql.Clob convertToClob(@Nonnull Object value) throws SQLException {
        try {
            return convertToClobMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToClob error", e);
        }
    }

    protected java.sql.Clob convertToClobMain(@Nonnull Object value) throws Exception {
        if (value instanceof java.sql.Clob) {
            return (java.sql.Clob) value;
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToClob unsupported type", value.getClass());
    }
}
