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
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Locale;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.factory.GetFactory;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;

/**
 * Tsurugi JDBC Convert Utility.
 */
public class TsurugiJdbcConvertUtil {

    private final GetFactory factoryHolder;

    /**
     * Creates a new instance.
     *
     * @param factoryHolder factory holder
     */
    public TsurugiJdbcConvertUtil(@Nonnull GetFactory factoryHolder) {
        this.factoryHolder = Objects.requireNonNull(factoryHolder);
    }

    /**
     * Get factory.
     *
     * @return factory
     */
    protected TsurugiJdbcFactory getFactory() {
        return factoryHolder.getFactory();
    }

    /**
     * Get exception handler.
     *
     * @return exception handler
     */
    protected TsurugiJdbcExceptionHandler getExceptionHandler() {
        return getFactory().getExceptionHandler();
    }

    /**
     * Convert to boolean.
     *
     * @param value value
     * @return boolean value
     * @throws SQLException if data convert error occurs
     */
    public boolean convertToBoolean(@Nonnull Object value) throws SQLException {
        try {
            return convertToBooleanMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToBoolean error", e);
        }
    }

    /**
     * Convert to boolean.
     *
     * @param value value
     * @return boolean value
     * @throws Exception if data convert error occurs
     */
    protected boolean convertToBooleanMain(@Nonnull Object value) throws Exception {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            return convertToBooleanFromString((String) value);
        }
        if (value instanceof Number) {
            return convertToBooleanFromNumber((Number) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToBoolean unsupported type", value.getClass());
    }

    /**
     * Convert to boolean from String.
     *
     * @param value value
     * @return boolean value
     * @throws SQLException if data convert error occurs
     */
    protected boolean convertToBooleanFromString(@Nonnull String value) throws SQLException {
        String v = value.toUpperCase(Locale.ENGLISH);
        switch (v) {
        case "TRUE":
            return true;
        case "FALSE":
            return false;
        default:
            throw getExceptionHandler().dataException("Cannot cast to boolean", null);
        }
    }

    /**
     * Convert to boolean from Number.
     *
     * @param value value
     * @return boolean value
     * @throws SQLException if data convert error occurs
     */
    protected boolean convertToBooleanFromNumber(@Nonnull Number value) throws SQLException {
        double v = value.doubleValue();
        if (v == 1d) {
            return true;
        }
        if (v == 0d) {
            return false;
        }

        throw getExceptionHandler().dataException("Cannot cast to boolean", null);
    }

    /**
     * Convert to byte.
     *
     * @param value value
     * @return byte value
     * @throws SQLException if data convert error occurs
     */
    public byte convertToByte(@Nonnull Object value) throws SQLException {
        try {
            return convertToByteMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToByte error", e);
        }
    }

    /**
     * Convert to byte.
     *
     * @param value value
     * @return byte value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to short.
     *
     * @param value value
     * @return short value
     * @throws SQLException if data convert error occurs
     */
    public short convertToShort(@Nonnull Object value) throws SQLException {
        try {
            return convertToShortMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToShort error", e);
        }
    }

    /**
     * Convert to short.
     *
     * @param value value
     * @return short value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to int.
     *
     * @param value value
     * @return int value
     * @throws SQLException if data convert error occurs
     */
    public int convertToInt(@Nonnull Object value) throws SQLException {
        try {
            return convertToIntMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToInt error", e);
        }
    }

    /**
     * Convert to int.
     *
     * @param value value
     * @return int value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to long.
     *
     * @param value value
     * @return long value
     * @throws SQLException if data convert error occurs
     */
    public long convertToLong(@Nonnull Object value) throws SQLException {
        try {
            return convertToLongMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToLong error", e);
        }
    }

    /**
     * Convert to long.
     *
     * @param value value
     * @return long value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to float.
     *
     * @param value value
     * @return float value
     * @throws SQLException if data convert error occurs
     */
    public float convertToFloat(@Nonnull Object value) throws SQLException {
        try {
            return convertToFloatMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToFloat error", e);
        }
    }

    /**
     * Convert to float.
     *
     * @param value value
     * @return float value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to double.
     *
     * @param value value
     * @return double value
     * @throws SQLException if data convert error occurs
     */
    public double convertToDouble(@Nonnull Object value) throws SQLException {
        try {
            return convertToDoubleMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToDouble error", e);
        }
    }

    /**
     * Convert to double.
     *
     * @param value value
     * @return double value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to BigDecimal.
     *
     * @param value value
     * @return BigDecimal value
     * @throws SQLException if data convert error occurs
     */
    public BigDecimal convertToDecimal(@Nonnull Object value) throws SQLException {
        try {
            return convertToDecimalMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToDecimal error", e);
        }
    }

    /**
     * Convert to BigDecimal.
     *
     * @param value value
     * @return BigDecimal value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to String.
     *
     * @param value value
     * @return String value
     * @throws SQLException if data convert error occurs
     */
    public String convertToString(@Nonnull Object value) throws SQLException {
        try {
            return convertToStringMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToString error", e);
        }
    }

    /**
     * Convert to String.
     *
     * @param value value
     * @return String value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to String.
     *
     * @param reader reader
     * @param length length limit, -1 for no limit
     * @return String value
     * @throws SQLException if data convert error occurs
     */
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

    /**
     * Convert to bytes.
     *
     * @param value value
     * @return byte[] value
     * @throws SQLException if data convert error occurs
     */
    public byte[] convertToBytes(@Nonnull Object value) throws SQLException {
        try {
            return convertToBytesMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToBytes error", e);
        }
    }

    /**
     * Convert to bytes.
     *
     * @param value value
     * @return byte[] value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to date.
     *
     * @param value value
     * @return date value
     * @throws SQLException if data convert error occurs
     */
    public java.sql.Date convertToDate(@Nonnull Object value) throws SQLException {
        try {
            return convertToDateMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToDate error", e);
        }
    }

    /**
     * Convert to date.
     *
     * @param value value
     * @return date value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to time.
     *
     * @param value value
     * @return time value
     * @throws SQLException if data convert error occurs
     */
    public java.sql.Time convertToTime(@Nonnull Object value) throws SQLException {
        try {
            return convertToTimeMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToTime error", e);
        }
    }

    /**
     * Convert to time.
     *
     * @param value value
     * @return time value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to timestamp.
     *
     * @param value value
     * @return timestamp value
     * @throws SQLException if data convert error occurs
     */
    public java.sql.Timestamp convertToTimestamp(@Nonnull Object value) throws SQLException {
        try {
            return convertToTimestampMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToTimestamp error", e);
        }
    }

    /**
     * Convert to timestamp.
     *
     * @param value value
     * @return timestamp value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to LocalDate.
     *
     * @param value value
     * @return local date value
     * @throws SQLException if data convert error occurs
     */
    public LocalDate convertToLocalDate(@Nonnull Object value) throws SQLException {
        try {
            return convertToLocalDateMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToLocalDate error", e);
        }
    }

    /**
     * Convert to LocalDate.
     *
     * @param value value
     * @return local date value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to LocalTime.
     *
     * @param value value
     * @return local time value
     * @throws SQLException if data convert error occurs
     */
    public LocalTime convertToLocalTime(@Nonnull Object value) throws SQLException {
        try {
            return convertToLocalTimeMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToLocalTime error", e);
        }
    }

    /**
     * Convert to LocalTime.
     *
     * @param value value
     * @return local time value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to LocalDateTime.
     *
     * @param value value
     * @return local date time value
     * @throws SQLException if data convert error occurs
     */
    public LocalDateTime convertToLocalDateTime(@Nonnull Object value) throws SQLException {
        try {
            return convertToLocalDateTimeMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToLocalDateTime error", e);
        }
    }

    /**
     * Convert to LocalDateTime.
     *
     * @param value value
     * @return local date time value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to OffsetTime.
     *
     * @param value value
     * @return offset time value
     * @throws SQLException if data convert error occurs
     */
    public OffsetTime convertToOffsetTime(@Nonnull Object value) throws SQLException {
        try {
            return convertToOffsetTimeMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToOffsetTime error", e);
        }
    }

    /**
     * Convert to OffsetTime.
     *
     * @param value value
     * @return offset time value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to OffsetDateTime.
     *
     * @param value value
     * @return offset date time value
     * @throws SQLException if data convert error occurs
     */
    public OffsetDateTime convertToOffsetDateTime(@Nonnull Object value) throws SQLException {
        try {
            return convertToOffsetDateTimeMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToOffsetDateTime error", e);
        }
    }

    /**
     * Convert to OffsetDateTime.
     *
     * @param value value
     * @return offset date time value
     * @throws Exception if data convert error occurs
     */
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

    /**
     * Convert to CharacterStream.
     *
     * @param value value
     * @return Reader value
     * @throws SQLException if data convert error occurs
     */
    public Reader convertToCharacterStream(@Nonnull Object value) throws SQLException {
        try {
            return convertToCharacterStreamMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToCharacterStream error", e);
        }
    }

    /**
     * Convert to Reader.
     *
     * @param value value
     * @return Reader value
     * @throws Exception if data convert error occurs
     */
    protected Reader convertToCharacterStreamMain(@Nonnull Object value) throws Exception {
        if (value instanceof Reader) {
            return (Reader) value;
        }
        if (value instanceof String) {
            return new StringReader((String) value);
        }
        if (value instanceof java.sql.Clob) {
            return ((java.sql.Clob) value).getCharacterStream();
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToCharacterStream unsupported type", value.getClass());
    }

    /**
     * Convert to AsciiStream.
     *
     * @param value value
     * @return InputStream value
     * @throws SQLException if data convert error occurs
     */
    public InputStream convertToAsciiStream(@Nonnull Object value) throws SQLException {
        try {
            return convertToAsciiStreamMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToAsciiStream error", e);
        }
    }

    /**
     * Convert to AsciiStream.
     *
     * @param value value
     * @return InputStream value
     * @throws Exception if data convert error occurs
     */
    protected InputStream convertToAsciiStreamMain(@Nonnull Object value) throws Exception {
        if (value instanceof InputStream) {
            return (InputStream) value;
        }
        if (value instanceof String) {
            byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
            return new ByteArrayInputStream(bytes);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToAsciiStream unsupported type", value.getClass());
    }

    /**
     * Convert to UnicodeStream.
     *
     * @param value value
     * @return InputStream value
     * @throws SQLException if data convert error occurs
     */
    public InputStream convertToUnicodeStream(@Nonnull Object value) throws SQLException {
        try {
            return convertToUnicodeStreamMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToUnicodeStream error", e);
        }
    }

    /**
     * Convert to UnicodeStream.
     *
     * @param value value
     * @return InputStream value
     * @throws Exception if data convert error occurs
     */
    protected InputStream convertToUnicodeStreamMain(@Nonnull Object value) throws Exception {
        if (value instanceof InputStream) {
            return (InputStream) value;
        }
        if (value instanceof String) {
            byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_16);
            return new ByteArrayInputStream(bytes);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToUnicodeStream unsupported type", value.getClass());
    }

    /**
     * Convert to BinaryStream.
     *
     * @param value value
     * @return InputStream value
     * @throws SQLException if data convert error occurs
     */
    public InputStream convertToBinaryStream(@Nonnull Object value) throws SQLException {
        try {
            return convertToBinaryStreamMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToBinaryStream error", e);
        }
    }

    /**
     * Convert to BinaryStream.
     *
     * @param value value
     * @return InputStream value
     * @throws Exception if data convert error occurs
     */
    protected InputStream convertToBinaryStreamMain(@Nonnull Object value) throws Exception {
        if (value instanceof InputStream) {
            return (InputStream) value;
        }
        if (value instanceof byte[]) {
            return new ByteArrayInputStream((byte[]) value);
        }
        if (value instanceof java.sql.Blob) {
            return ((java.sql.Blob) value).getBinaryStream();
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToBinaryStream unsupported type", value.getClass());
    }

    /**
     * Convert to Blob.
     *
     * @param value value
     * @return Blob value
     * @throws SQLException if data convert error occurs
     */
    public java.sql.Blob convertToBlob(@Nonnull Object value) throws SQLException {
        try {
            return convertToBlobMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToBlob error", e);
        }
    }

    /**
     * Convert to Blob.
     *
     * @param value value
     * @return Blob value
     * @throws Exception if data convert error occurs
     */
    protected java.sql.Blob convertToBlobMain(@Nonnull Object value) throws Exception {
        if (value instanceof java.sql.Blob) {
            return (java.sql.Blob) value;
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToBlob unsupported type", value.getClass());
    }

    /**
     * Convert to Clob.
     *
     * @param value value
     * @return Clob value
     * @throws SQLException if data convert error occurs
     */
    public java.sql.Clob convertToClob(@Nonnull Object value) throws SQLException {
        try {
            return convertToClobMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToClob error", e);
        }
    }

    /**
     * Convert to Clob.
     *
     * @param value value
     * @return Clob value
     * @throws Exception if data convert error occurs
     */
    protected java.sql.Clob convertToClobMain(@Nonnull Object value) throws Exception {
        if (value instanceof java.sql.Clob) {
            return (java.sql.Clob) value;
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToClob unsupported type", value.getClass());
    }
}
