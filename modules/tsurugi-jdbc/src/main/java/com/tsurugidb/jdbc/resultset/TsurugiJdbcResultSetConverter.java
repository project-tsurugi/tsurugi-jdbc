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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.factory.GetFactory;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.resultset.type.TsurugiJdbcBlobReference;
import com.tsurugidb.jdbc.resultset.type.TsurugiJdbcClobReference;
import com.tsurugidb.jdbc.util.TsurugiJdbcConvertUtil;

/**
 * Tsurugi JDBC ResultSet Converter.
 * <p>
 * Convert Tsubakuro value to specified value.
 * </p>
 */
public class TsurugiJdbcResultSetConverter {

    private final GetFactory ownerResultSet;
    private TsurugiJdbcConvertUtil convertUtil;

    /**
     * Creates a new instance.
     *
     * @param ownerResultSet result set
     */
    public TsurugiJdbcResultSetConverter(GetFactory ownerResultSet) {
        this.ownerResultSet = ownerResultSet;
        var factory = ownerResultSet.getFactory();
        this.convertUtil = factory.createConvertUtil(ownerResultSet);
    }

    /**
     * Set convert utility.
     *
     * @param convertUtil convert utility
     */
    public void setConvertUtil(@Nonnull TsurugiJdbcConvertUtil convertUtil) {
        this.convertUtil = Objects.requireNonNull(convertUtil);
    }

    /**
     * Get factory.
     *
     * @return factory
     */
    protected TsurugiJdbcFactory getFactory() {
        return ownerResultSet.getFactory();
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
     * Convert to String.
     *
     * @param value value
     * @return String value
     * @throws SQLException if data convert error occurs
     */
    public String convertToString(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToString(value);
    }

    /**
     * Convert to boolean.
     *
     * @param value value
     * @return boolean value
     * @throws SQLException if data convert error occurs
     */
    public boolean convertToBoolean(Object value) throws SQLException {
        if (value == null) {
            return false;
        }

        return convertUtil.convertToBoolean(value);
    }

    /**
     * Convert to byte.
     *
     * @param value value
     * @return byte value
     * @throws SQLException if data convert error occurs
     */
    public byte convertToByte(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }

        return convertUtil.convertToByte(value);
    }

    /**
     * Convert to short.
     *
     * @param value value
     * @return short value
     * @throws SQLException if data convert error occurs
     */
    public short convertToShort(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }

        return convertUtil.convertToShort(value);
    }

    /**
     * Convert to int.
     *
     * @param value value
     * @return int value
     * @throws SQLException if data convert error occurs
     */
    public int convertToInt(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }

        return convertUtil.convertToInt(value);
    }

    /**
     * Convert to long.
     *
     * @param value value
     * @return long value
     * @throws SQLException if data convert error occurs
     */
    public long convertToLong(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }

        return convertUtil.convertToLong(value);
    }

    /**
     * Convert to float.
     *
     * @param value value
     * @return float value
     * @throws SQLException if data convert error occurs
     */
    public float convertToFloat(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }

        return convertUtil.convertToFloat(value);
    }

    /**
     * Convert to double.
     *
     * @param value value
     * @return double value
     * @throws SQLException if data convert error occurs
     */
    public double convertToDouble(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }

        return convertUtil.convertToDouble(value);
    }

    /**
     * Convert to decimal.
     *
     * @param value value
     * @param scale scale
     * @return decimal value
     * @throws SQLException if data convert error occurs
     */
    public BigDecimal convertToDecimal(Object value, int scale) throws SQLException {
        BigDecimal result = convertToDecimal(value);
        return result.setScale(scale);
    }

    /**
     * Convert to decimal.
     *
     * @param value value
     * @return decimal value
     * @throws SQLException if data convert error occurs
     */
    public BigDecimal convertToDecimal(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToDecimal(value);
    }

    /**
     * Convert to bytes.
     *
     * @param value value
     * @return byte[] value
     * @throws SQLException if data convert error occurs
     */
    public byte[] convertToBytes(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToBytes(value);
    }

    /**
     * Convert to Date.
     *
     * @param value value
     * @return date value
     * @throws SQLException if data convert error occurs
     */
    public java.sql.Date convertToDate(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToDate(value);
    }

    /**
     * Convert to Time.
     *
     * @param value value
     * @return time value
     * @throws SQLException if data convert error occurs
     */
    public java.sql.Time convertToTime(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToTime(value);
    }

    /**
     * Convert to Timestamp.
     *
     * @param value value
     * @return timestamp value
     * @throws SQLException if data convert error occurs
     */
    public java.sql.Timestamp convertToTimestamp(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToTimestamp(value);
    }

    /**
     * Convert to LocalDate.
     *
     * @param value value
     * @return local date value
     * @throws SQLException if data convert error occurs
     */
    public LocalDate convertToLocalDate(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToLocalDate(value);
    }

    /**
     * Convert to LocalTime.
     *
     * @param value value
     * @return local time value
     * @throws SQLException if data convert error occurs
     */
    public LocalTime convertToLocalTime(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToLocalTime(value);
    }

    /**
     * Convert to LocalDateTime.
     *
     * @param value value
     * @return local date time value
     * @throws SQLException if data convert error occurs
     */
    public LocalDateTime convertToLocalDateTime(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToLocalDateTime(value);
    }

    /**
     * Convert to OffsetTime.
     *
     * @param value value
     * @return offset time value
     * @throws SQLException if data convert error occurs
     */
    public OffsetTime convertToOffsetTime(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToOffsetTime(value);
    }

    /**
     * Convert to OffsetDateTime.
     *
     * @param value value
     * @return offset date time value
     * @throws SQLException if data convert error occurs
     */
    public OffsetDateTime convertToOffsetDateTime(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToOffsetDateTime(value);
    }

    /**
     * Convert to ZonedDateTime.
     *
     * @param value value
     * @return offset date time value
     * @throws SQLException if data convert error occurs
     */
    public ZonedDateTime convertToZonedDateTime(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToZonedDateTime(value);
    }

    /**
     * Convert to CharacterStream.
     *
     * @param value value
     * @return Reader value
     * @throws SQLException if data convert error occurs
     */
    public Reader convertToCharacterStream(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToCharacterStream(value);
    }

    /**
     * Convert to AsciiStream.
     *
     * @param value value
     * @return ascii InputStream value
     * @throws SQLException if data convert error occurs
     */
    public InputStream convertToAsciiStream(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToAsciiStream(value);
    }

    /**
     * Convert to UnicodeStream.
     *
     * @param value value
     * @return unicode InputStream value
     * @throws SQLException if data convert error occurs
     */
    public InputStream convertToUnicodeStream(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToUnicodeStream(value);
    }

    /**
     * Convert to BinaryStream.
     *
     * @param value value
     * @return binary InputStream value
     * @throws SQLException if data convert error occurs
     */
    public InputStream convertToBinaryStream(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToBinaryStream(value);
    }

    /**
     * Convert to Blob.
     *
     * @param value value
     * @return blob value
     * @throws SQLException if data convert error occurs
     */
    public java.sql.Blob convertToBlob(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToBlob(value);
    }

    /**
     * Convert to Clob.
     *
     * @param value value
     * @return clob value
     * @throws SQLException if data convert error occurs
     */
    public java.sql.Clob convertToClob(Object value) throws SQLException {
        if (value == null) {
            return null;
        }

        return convertUtil.convertToClob(value);
    }

    /**
     * Convert to specified type.
     *
     * @param <T>   target type
     * @param value value
     * @param type  target type
     * @return converted value
     * @throws SQLException if data convert error occurs
     */
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
        map.put(ZonedDateTime.class, TsurugiJdbcResultSetConverter::convertToZonedDateTime);

        map.put(java.sql.Blob.class, TsurugiJdbcResultSetConverter::convertToBlob);
        map.put(java.sql.Clob.class, TsurugiJdbcResultSetConverter::convertToClob);
        map.put(TsurugiJdbcBlobReference.class, TsurugiJdbcResultSetConverter::convertToBlob);
        map.put(TsurugiJdbcClobReference.class, TsurugiJdbcResultSetConverter::convertToClob);
        CONVERTER_MAP = map;
    }

    /**
     * Get converter.
     *
     * @param type target type
     * @return converter
     * @throws SQLException if type is not supported
     */
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
