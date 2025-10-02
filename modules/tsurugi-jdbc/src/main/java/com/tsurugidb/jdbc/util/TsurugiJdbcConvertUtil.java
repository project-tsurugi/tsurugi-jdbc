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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

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
        String v = value.toUpperCase(Locale.ENGLISH).trim();
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
            return Byte.parseByte(((String) value).trim());
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
            return Short.parseShort(((String) value).trim());
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
            return Integer.parseInt(((String) value).trim());
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
            return Long.parseLong(((String) value).trim());
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
            return Float.parseFloat(((String) value).trim());
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
            return Double.parseDouble(((String) value).trim());
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
            return new BigDecimal(((String) value).trim());
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
        if (value instanceof Boolean) {
            return value.toString();
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
            return toDate((LocalDate) value);
        }
        if (value instanceof LocalDateTime) {
            return toDate((LocalDateTime) value);
        }
        if (value instanceof OffsetDateTime) {
            return toDate((OffsetDateTime) value);
        }
        if (value instanceof java.sql.Timestamp) {
            return toDate((java.sql.Timestamp) value);
        }
        if (value instanceof java.sql.Time) {
            return toDate((java.sql.Time) value);
        }
        if (value instanceof ZonedDateTime) {
            return toDate((ZonedDateTime) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToDate unsupported type", value.getClass());
    }

    /**
     * Convert to Date.
     *
     * @param value value
     * @return date value
     */
    protected java.sql.Date toDate(java.sql.Time value) {
        return toDate(toOffsetDateTime(value));
    }

    /**
     * Convert to Date.
     *
     * @param value value
     * @return date value
     */
    protected java.sql.Date toDate(java.sql.Timestamp value) {
        return toDate(toOffsetDateTime(value));
    }

    /**
     * Convert to Date.
     *
     * @param value value
     * @return date value
     */
    protected java.sql.Date toDate(LocalDate value) {
        long epochDay = value.toEpochDay();
        return new java.sql.Date(TimeUnit.DAYS.toMillis(epochDay));
    }

    /**
     * Convert to Date.
     *
     * @param value value
     * @return date value
     */
    protected java.sql.Date toDate(LocalDateTime value) {
        return toDate(value.toLocalDate());
    }

    /**
     * Convert to Date.
     *
     * @param value value
     * @return date value
     */
    protected java.sql.Date toDate(OffsetDateTime value) {
        var zone = ZoneId.systemDefault(); // in java.sql.Date
        var zdt = value.atZoneSameInstant(zone);
        return toDate(zdt.toLocalDate());
    }

    /**
     * Convert to Date.
     *
     * @param value value
     * @return date value
     */
    protected java.sql.Date toDate(ZonedDateTime value) {
        var zone = ZoneId.systemDefault(); // in java.sql.Date
        var zdt = value.withZoneSameInstant(zone);
        return toDate(zdt.toLocalDate());
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
            return toTime((LocalTime) value);
        }
        if (value instanceof OffsetTime) {
            return toTime((OffsetTime) value);
        }
        if (value instanceof LocalDateTime) {
            return toTime((LocalDateTime) value);
        }
        if (value instanceof OffsetDateTime) {
            return toTime((OffsetDateTime) value);
        }
        if (value instanceof ZonedDateTime) {
            return toTime((ZonedDateTime) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToTime unsupported type", value.getClass());
    }

    /**
     * Convert to Time.
     *
     * @param value value
     * @return time value
     */
    protected java.sql.Time toTime(LocalTime value) {
        var zone = ZoneId.systemDefault(); // in java.sql.Time
        long epochSecond = value.atDate(LocalDate.EPOCH).atZone(zone).toEpochSecond();
        return new java.sql.Time(TimeUnit.SECONDS.toMillis(epochSecond));
    }

    /**
     * Convert to Time.
     *
     * @param value value
     * @return time value
     */
    protected java.sql.Time toTime(LocalDateTime value) {
        return toTime(value.toLocalTime());
    }

    /**
     * Convert to Time.
     *
     * @param value value
     * @return time value
     */
    protected java.sql.Time toTime(OffsetTime value) {
        long epochSecond = value.toEpochSecond(LocalDate.EPOCH);
        return new java.sql.Time(TimeUnit.SECONDS.toMillis(epochSecond));
    }

    /**
     * Convert to Time.
     *
     * @param value value
     * @return time value
     */
    protected java.sql.Time toTime(OffsetDateTime value) {
        return toTime(value.toOffsetTime());
    }

    /**
     * Convert to Time.
     *
     * @param value value
     * @return time value
     */
    protected java.sql.Time toTime(ZonedDateTime value) {
        return toTime(value.toOffsetDateTime().toOffsetTime());
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
            return toTimestamp((LocalDateTime) value);
        }
        if (value instanceof OffsetDateTime) {
            return toTimestamp((OffsetDateTime) value);
        }
        if (value instanceof LocalDate) {
            return toTimestamp((LocalDate) value);
        }
        if (value instanceof ZonedDateTime) {
            return toTimestamp((ZonedDateTime) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToTimestamp unsupported type", value.getClass());
    }

    /**
     * Convert to Timestamp.
     *
     * @param value value
     * @return timestamp value
     */
    protected java.sql.Timestamp toTimestamp(java.sql.Date value) {
        return toTimestamp(toOffsetDateTime(value));
    }

    /**
     * Convert to Timestamp.
     *
     * @param value value
     * @return timestamp value
     */
    protected java.sql.Timestamp toTimestamp(java.sql.Time value) {
        return toTimestamp(toOffsetDateTime(value));
    }

    /**
     * Convert to Timestamp.
     *
     * @param value value
     * @return timestamp value
     */
    protected java.sql.Timestamp toTimestamp(LocalDate value) {
        return toTimestamp(value.atStartOfDay());
    }

    /**
     * Convert to Timestamp.
     *
     * @param value value
     * @return timestamp value
     */
    protected java.sql.Timestamp toTimestamp(LocalDateTime value) {
        var zone = ZoneId.systemDefault();
        var zdt = value.atZone(zone);
        return toTimestamp(zdt);
    }

    /**
     * Convert to Timestamp.
     *
     * @param value value
     * @return timestamp value
     */
    protected java.sql.Timestamp toTimestamp(OffsetDateTime value) {
        long epochSecond = value.toEpochSecond();
        var timestamp = new java.sql.Timestamp(TimeUnit.SECONDS.toMillis(epochSecond));
        timestamp.setNanos(value.getNano());
        return timestamp;
    }

    /**
     * Convert to Timestamp.
     *
     * @param value value
     * @return timestamp value
     */
    protected java.sql.Timestamp toTimestamp(ZonedDateTime value) {
        long epochSecond = value.toEpochSecond();
        var timestamp = new java.sql.Timestamp(TimeUnit.SECONDS.toMillis(epochSecond));
        timestamp.setNanos(value.getNano());
        return timestamp;
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
            return toLocalDate(((java.sql.Date) value));
        }
        if (value instanceof LocalDateTime) {
            return toLocalDate((LocalDateTime) value);
        }
        if (value instanceof OffsetDateTime) {
            return toLocalDate((OffsetDateTime) value);
        }
        if (value instanceof java.sql.Timestamp) {
            return toLocalDate((java.sql.Timestamp) value);
        }
        if (value instanceof java.sql.Time) {
            return toLocalDate((java.sql.Time) value);
        }
        if (value instanceof ZonedDateTime) {
            return toLocalDate((ZonedDateTime) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToLocalDate unsupported type", value.getClass());
    }

    /**
     * Convert to LocalDate.
     *
     * @param value value
     * @return local date value
     */
    protected LocalDate toLocalDate(java.sql.Date value) {
        return toOffsetDateTime(value).toLocalDate();
    }

    /**
     * Convert to LocalDate.
     *
     * @param value value
     * @return local date value
     */
    protected LocalDate toLocalDate(java.sql.Time value) {
        return toOffsetDateTime(value).toLocalDate();
    }

    /**
     * Convert to LocalDate.
     *
     * @param value value
     * @return local date value
     */
    protected LocalDate toLocalDate(java.sql.Timestamp value) {
        return toOffsetDateTime(value).toLocalDate();
    }

    /**
     * Convert to LocalDate.
     *
     * @param value value
     * @return local date value
     */
    protected LocalDate toLocalDate(LocalDateTime value) {
        return value.toLocalDate();
    }

    /**
     * Convert to LocalDate.
     *
     * @param value value
     * @return local date value
     */
    protected LocalDate toLocalDate(OffsetDateTime value) {
        return value.toLocalDate();
    }

    /**
     * Convert to LocalDate.
     *
     * @param value value
     * @return local date value
     */
    protected LocalDate toLocalDate(ZonedDateTime value) {
        return value.toLocalDate();
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
            return toLocalTime((java.sql.Time) value);
        }
        if (value instanceof OffsetTime) {
            return toLocalTime((OffsetTime) value);
        }
        if (value instanceof LocalDateTime) {
            return toLocalTime((LocalDateTime) value);
        }
        if (value instanceof OffsetDateTime) {
            return toLocalTime((OffsetDateTime) value);
        }
        if (value instanceof java.sql.Timestamp) {
            return toLocalTime((java.sql.Timestamp) value);
        }
        if (value instanceof java.sql.Date) {
            return toLocalTime((java.sql.Date) value);
        }
        if (value instanceof ZonedDateTime) {
            return toLocalTime((ZonedDateTime) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToLocalTime unsupported type", value.getClass());
    }

    /**
     * Convert to LocalTime.
     *
     * @param value value
     * @return local time value
     */
    protected LocalTime toLocalTime(java.sql.Date value) {
        return toOffsetDateTime(value).toLocalTime();
    }

    /**
     * Convert to LocalTime.
     *
     * @param value value
     * @return local time value
     */
    protected LocalTime toLocalTime(java.sql.Time value) {
        return toOffsetDateTime(value).toLocalTime();
    }

    /**
     * Convert to LocalTime.
     *
     * @param value value
     * @return local time value
     */
    protected LocalTime toLocalTime(java.sql.Timestamp value) {
        return toOffsetDateTime(value).toLocalTime();
    }

    /**
     * Convert to LocalTime.
     *
     * @param value value
     * @return local time value
     */
    protected LocalTime toLocalTime(LocalDateTime value) {
        return value.toLocalTime();
    }

    /**
     * Convert to LocalTime.
     *
     * @param value value
     * @return local time value
     */
    protected LocalTime toLocalTime(OffsetTime value) {
        return value.toLocalTime();
    }

    /**
     * Convert to LocalTime.
     *
     * @param value value
     * @return local time value
     */
    protected LocalTime toLocalTime(OffsetDateTime value) {
        return value.toLocalTime();
    }

    /**
     * Convert to LocalTime.
     *
     * @param value value
     * @return local time value
     */
    protected LocalTime toLocalTime(ZonedDateTime value) {
        return value.toLocalTime();
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
            return toLocalDateTime((java.sql.Timestamp) value);
        }
        if (value instanceof LocalDate) {
            return toLocalDateTime((LocalDate) value);
        }
        if (value instanceof OffsetDateTime) {
            return toLocalDateTime((OffsetDateTime) value);
        }
        if (value instanceof java.sql.Date) {
            return toLocalDateTime((java.sql.Date) value);
        }
        if (value instanceof java.sql.Time) {
            return toLocalDateTime((java.sql.Time) value);
        }
        if (value instanceof ZonedDateTime) {
            return toLocalDateTime((ZonedDateTime) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToLocalDateTime unsupported type", value.getClass());
    }

    /**
     * Convert to LocalDateTime.
     *
     * @param value value
     * @return local date time value
     */
    protected LocalDateTime toLocalDateTime(java.sql.Date value) {
        return toOffsetDateTime(value).toLocalDateTime();
    }

    /**
     * Convert to LocalDateTime.
     *
     * @param value value
     * @return local date time value
     */
    protected LocalDateTime toLocalDateTime(java.sql.Time value) {
        return toOffsetDateTime(value).toLocalDateTime();
    }

    /**
     * Convert to LocalDateTime.
     *
     * @param value value
     * @return local date time value
     */
    protected LocalDateTime toLocalDateTime(java.sql.Timestamp value) {
        return toOffsetDateTime(value).toLocalDateTime();
    }

    /**
     * Convert to LocalDateTime.
     *
     * @param value value
     * @return local date time value
     */
    protected LocalDateTime toLocalDateTime(LocalDate value) {
        return LocalDateTime.of(value, LocalTime.MIN);
    }

    /**
     * Convert to LocalDateTime.
     *
     * @param value value
     * @return local date time value
     */
    protected LocalDateTime toLocalDateTime(OffsetDateTime value) {
        return value.toLocalDateTime();
    }

    /**
     * Convert to LocalDateTime.
     *
     * @param value value
     * @return local date time value
     */
    protected LocalDateTime toLocalDateTime(ZonedDateTime value) {
        return value.toLocalDateTime();
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
            return toOffsetTime((java.sql.Time) value);
        }
        if (value instanceof OffsetDateTime) {
            return toOffsetTime((OffsetDateTime) value);
        }
        if (value instanceof LocalTime) {
            return toOffsetTime((LocalTime) value);
        }
        if (value instanceof LocalDateTime) {
            return toOffsetTime((LocalDateTime) value);
        }
        if (value instanceof java.sql.Timestamp) {
            return toOffsetTime((java.sql.Timestamp) value);
        }
        if (value instanceof java.sql.Date) {
            return toOffsetTime((java.sql.Date) value);
        }
        if (value instanceof ZonedDateTime) {
            return toOffsetTime((ZonedDateTime) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToOffsetTime unsupported type", value.getClass());
    }

    /**
     * Convert to OffsetTime.
     *
     * @param value value
     * @return offset time value
     */
    protected OffsetTime toOffsetTime(java.sql.Date value) {
        return toOffsetDateTime(value).toOffsetTime();
    }

    /**
     * Convert to OffsetTime.
     *
     * @param value value
     * @return offset time value
     */
    protected OffsetTime toOffsetTime(java.sql.Time value) {
        return toOffsetDateTime(value).toOffsetTime();
    }

    /**
     * Convert to OffsetTime.
     *
     * @param value value
     * @return offset time value
     */
    protected OffsetTime toOffsetTime(java.sql.Timestamp value) {
        return toOffsetDateTime(value).toOffsetTime();
    }

    /**
     * Convert to OffsetTime.
     *
     * @param value value
     * @return offset time value
     */
    protected OffsetTime toOffsetTime(LocalTime value) {
        return OffsetTime.of(value, ZoneOffset.UTC);
    }

    /**
     * Convert to OffsetTime.
     *
     * @param value value
     * @return offset time value
     */
    protected OffsetTime toOffsetTime(LocalDateTime value) {
        return toOffsetTime(value.toLocalTime());
    }

    /**
     * Convert to OffsetTime.
     *
     * @param value value
     * @return offset time value
     */
    protected OffsetTime toOffsetTime(OffsetDateTime value) {
        return value.toOffsetTime();
    }

    /**
     * Convert to OffsetTime.
     *
     * @param value value
     * @return offset time value
     */
    protected OffsetTime toOffsetTime(ZonedDateTime value) {
        return value.toOffsetDateTime().toOffsetTime();
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
            return toOffsetDateTime((java.sql.Timestamp) value);
        }
        if (value instanceof LocalDateTime) {
            return toOffsetDateTime((LocalDateTime) value);
        }
        if (value instanceof LocalDate) {
            return toOffsetDateTime((LocalDate) value);
        }
        if (value instanceof java.sql.Date) {
            return toOffsetDateTime((java.sql.Date) value);
        }
        if (value instanceof java.sql.Time) {
            return toOffsetDateTime((java.sql.Time) value);
        }
        if (value instanceof ZonedDateTime) {
            return toOffsetDateTime((ZonedDateTime) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToOffsetDateTime unsupported type", value.getClass());
    }

    /**
     * Convert to OffsetDateTime.
     *
     * @param value value
     * @return offset date time value
     */
    protected OffsetDateTime toOffsetDateTime(java.sql.Date value) {
        return toZonedDateTime(value).toOffsetDateTime();
    }

    /**
     * Convert to OffsetDateTime.
     *
     * @param value value
     * @return offset date time value
     */
    protected OffsetDateTime toOffsetDateTime(java.sql.Time value) {
        return toZonedDateTime(value).toOffsetDateTime();
    }

    /**
     * Convert to OffsetDateTime.
     *
     * @param value value
     * @return offset date time value
     */
    protected OffsetDateTime toOffsetDateTime(java.sql.Timestamp value) {
        return toZonedDateTime(value).toOffsetDateTime();
    }

    /**
     * Convert to OffsetDateTime.
     *
     * @param value value
     * @return offset date time value
     */
    protected OffsetDateTime toOffsetDateTime(LocalDate value) {
        var ldt = value.atTime(LocalTime.MIN);
        return toOffsetDateTime(ldt);
    }

    /**
     * Convert to OffsetDateTime.
     *
     * @param value value
     * @return offset date time value
     */
    protected OffsetDateTime toOffsetDateTime(LocalDateTime value) {
        return OffsetDateTime.of(value, ZoneOffset.UTC);
    }

    /**
     * Convert to OffsetDateTime.
     *
     * @param value value
     * @return offset date time value
     */
    protected OffsetDateTime toOffsetDateTime(ZonedDateTime value) {
        return value.toOffsetDateTime();
    }

    /**
     * Convert to ZonedDateTime.
     *
     * @param value value
     * @return offset date time value
     * @throws SQLException if data convert error occurs
     */
    public ZonedDateTime convertToZonedDateTime(@Nonnull Object value) throws SQLException {
        try {
            return convertToZonedDateTimeMain(value);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw getExceptionHandler().dataException("convertToZonedDateTime error", e);
        }
    }

    /**
     * Convert to ZonedDateTime.
     *
     * @param value value
     * @return offset date time value
     * @throws Exception if data convert error occurs
     */
    protected ZonedDateTime convertToZonedDateTimeMain(@Nonnull Object value) throws Exception {
        if (value instanceof ZonedDateTime) {
            return (ZonedDateTime) value;
        }
        if (value instanceof OffsetDateTime) {
            return toZonedDateTime((OffsetDateTime) value);
        }
        if (value instanceof java.sql.Timestamp) {
            return toZonedDateTime((java.sql.Timestamp) value);
        }
        if (value instanceof LocalDateTime) {
            return toZonedDateTime((LocalDateTime) value);
        }
        if (value instanceof LocalDate) {
            return toZonedDateTime((LocalDate) value);
        }
        if (value instanceof java.sql.Date) {
            return toZonedDateTime((java.sql.Date) value);
        }
        if (value instanceof java.sql.Time) {
            return toZonedDateTime((java.sql.Time) value);
        }

        throw getExceptionHandler().dataTypeMismatchException("convertToZonedDateTime unsupported type", value.getClass());
    }

    /**
     * Convert to ZonedDateTime.
     *
     * @param value value
     * @return offset date time value
     */
    protected ZonedDateTime toZonedDateTime(java.sql.Date value) {
        long epochMilli = value.getTime();
        var instant = Instant.ofEpochMilli(epochMilli);
        var zone = ZoneId.systemDefault();
        return instant.atZone(zone);
    }

    /**
     * Convert to ZonedDateTime.
     *
     * @param value value
     * @return offset date time value
     */
    protected ZonedDateTime toZonedDateTime(java.sql.Time value) {
        long epochMilli = value.getTime();
        var instant = Instant.ofEpochMilli(epochMilli);
        var zone = ZoneId.systemDefault();
        return instant.atZone(zone);
    }

    /**
     * Convert to ZonedDateTime.
     *
     * @param value value
     * @return offset date time value
     */
    protected ZonedDateTime toZonedDateTime(java.sql.Timestamp value) {
        long epochMilli = value.getTime();
        var instant = Instant.ofEpochMilli(epochMilli);
        var zone = ZoneId.systemDefault();
        return instant.atZone(zone);
    }

    /**
     * Convert to ZonedDateTime.
     *
     * @param value value
     * @return offset date time value
     */
    protected ZonedDateTime toZonedDateTime(LocalDate value) {
        var ldt = value.atTime(LocalTime.MIN);
        return toZonedDateTime(ldt);
    }

    /**
     * Convert to ZonedDateTime.
     *
     * @param value value
     * @return offset date time value
     */
    protected ZonedDateTime toZonedDateTime(LocalDateTime value) {
        return ZonedDateTime.of(value, ZoneOffset.UTC);
    }

    /**
     * Convert to ZonedDateTime.
     *
     * @param value value
     * @return offset date time value
     */
    protected ZonedDateTime toZonedDateTime(OffsetDateTime value) {
        return value.toZonedDateTime();
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
