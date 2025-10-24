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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;

class TsurugiJdbcConvertUtilTest {

    private static final TsurugiJdbcConvertUtil util = new TsurugiJdbcConvertUtil(() -> TsurugiJdbcFactory.getDefaultFactory());

    private static final ZoneId DEFAULT_ZONE = ZoneId.systemDefault();

    @Test
    void convertToBoolean() throws SQLException {
        assertTrue(util.convertToBoolean(true));
        assertFalse(util.convertToBoolean(false));

        assertTrue(util.convertToBoolean("true "));
        assertFalse(util.convertToBoolean("false "));
        assertTrue(util.convertToBoolean("TRUE "));
        assertFalse(util.convertToBoolean("FALSE "));

        assertTrue(util.convertToBoolean(1));
        assertFalse(util.convertToBoolean(0));
        assertTrue(util.convertToBoolean(1L));
        assertFalse(util.convertToBoolean(0L));
        assertTrue(util.convertToBoolean(1f));
        assertFalse(util.convertToBoolean(0f));
        assertTrue(util.convertToBoolean(1d));
        assertFalse(util.convertToBoolean(0d));
        assertTrue(util.convertToBoolean(BigDecimal.ONE));
        assertFalse(util.convertToBoolean(BigDecimal.ZERO));
    }

    @Test
    void convertToByte() throws SQLException {
        assertEquals(Byte.MIN_VALUE, util.convertToByte(Byte.MIN_VALUE));
        assertEquals((byte) 0, util.convertToByte((byte) 0));
        assertEquals((byte) 1, util.convertToByte((byte) 1));
        assertEquals(Byte.MAX_VALUE, util.convertToByte(Byte.MAX_VALUE));

        assertEquals((byte) 123, util.convertToByte(123));
        assertEquals((byte) 123, util.convertToByte(123L));
        assertEquals((byte) 123, util.convertToByte(123f));
        assertEquals((byte) 123, util.convertToByte(123d));
        assertEquals((byte) 123, util.convertToByte(BigDecimal.valueOf(123)));

        assertEquals((byte) 123, util.convertToByte("123 "));
    }

    @Test
    void convertToShort() throws SQLException {
        assertEquals(Short.MIN_VALUE, util.convertToShort(Short.MIN_VALUE));
        assertEquals((short) 0, util.convertToShort((short) 0));
        assertEquals((short) 1, util.convertToShort((short) 1));
        assertEquals(Short.MAX_VALUE, util.convertToShort(Short.MAX_VALUE));

        assertEquals((short) 123, util.convertToShort(123));
        assertEquals((short) 123, util.convertToShort(123L));
        assertEquals((short) 123, util.convertToShort(123f));
        assertEquals((short) 123, util.convertToShort(123d));
        assertEquals((short) 123, util.convertToShort(BigDecimal.valueOf(123)));

        assertEquals((short) 123, util.convertToShort("123 "));
    }

    @Test
    void convertToInt() throws SQLException {
        assertEquals(Integer.MIN_VALUE, util.convertToInt(Integer.MIN_VALUE));
        assertEquals(0, util.convertToInt(0));
        assertEquals(1, util.convertToInt(1));
        assertEquals(Integer.MAX_VALUE, util.convertToInt(Integer.MAX_VALUE));

        assertEquals(123, util.convertToInt(123));
        assertEquals(123, util.convertToInt(123L));
        assertEquals(123, util.convertToInt(123f));
        assertEquals(123, util.convertToInt(123d));
        assertEquals(123, util.convertToInt(BigDecimal.valueOf(123)));

        assertEquals(123, util.convertToInt("123 "));
    }

    @Test
    void convertToLong() throws SQLException {
        assertEquals(Long.MIN_VALUE, util.convertToLong(Long.MIN_VALUE));
        assertEquals(0, util.convertToLong(0L));
        assertEquals(1, util.convertToLong(1L));
        assertEquals(Long.MAX_VALUE, util.convertToLong(Long.MAX_VALUE));

        assertEquals(123, util.convertToLong(123));
        assertEquals(123, util.convertToLong(123L));
        assertEquals(123, util.convertToLong(123f));
        assertEquals(123, util.convertToLong(123d));
        assertEquals(123, util.convertToLong(BigDecimal.valueOf(123)));

        assertEquals(123, util.convertToLong("123 "));
    }

    @Test
    void convertToFloat() throws SQLException {
        assertEquals(Float.MIN_VALUE, util.convertToFloat(Float.MIN_VALUE));
        assertEquals(0f, util.convertToFloat(0f));
        assertEquals(1f, util.convertToFloat(1f));
        assertEquals(Float.MAX_VALUE, util.convertToFloat(Float.MAX_VALUE));
        assertEquals(Float.NaN, util.convertToFloat(Float.NaN));
        assertEquals(Float.NEGATIVE_INFINITY, util.convertToFloat(Float.NEGATIVE_INFINITY));
        assertEquals(Float.POSITIVE_INFINITY, util.convertToFloat(Float.POSITIVE_INFINITY));

        assertEquals(123, util.convertToFloat(123));
        assertEquals(123, util.convertToFloat(123L));
        assertEquals(123, util.convertToFloat(123f));
        assertEquals(123, util.convertToFloat(123d));
        assertEquals(123, util.convertToFloat(BigDecimal.valueOf(123)));

        assertEquals(123, util.convertToFloat("123 "));
    }

    @Test
    void convertToDouble() throws SQLException {
        assertEquals(Double.MIN_VALUE, util.convertToDouble(Double.MIN_VALUE));
        assertEquals(0d, util.convertToDouble(0d));
        assertEquals(1d, util.convertToDouble(1d));
        assertEquals(Double.MAX_VALUE, util.convertToDouble(Double.MAX_VALUE));
        assertEquals(Double.NaN, util.convertToDouble(Double.NaN));
        assertEquals(Double.NEGATIVE_INFINITY, util.convertToDouble(Double.NEGATIVE_INFINITY));
        assertEquals(Double.POSITIVE_INFINITY, util.convertToDouble(Double.POSITIVE_INFINITY));

        assertEquals(123, util.convertToDouble(123));
        assertEquals(123, util.convertToDouble(123L));
        assertEquals(123, util.convertToDouble(123f));
        assertEquals(123, util.convertToDouble(123d));
        assertEquals(123, util.convertToDouble(BigDecimal.valueOf(123)));

        assertEquals(123, util.convertToDouble("123 "));
    }

    @Test
    void convertToDecimal() throws SQLException {
        assertEquals(BigDecimal.ZERO, util.convertToDecimal(BigDecimal.ZERO));
        assertEquals(BigDecimal.ONE, util.convertToDecimal(BigDecimal.ONE));

        assertEquals(BigDecimal.valueOf(123), util.convertToDecimal((byte) 123));
        assertEquals(BigDecimal.valueOf(123), util.convertToDecimal((short) 123));
        assertEquals(BigDecimal.valueOf(123), util.convertToDecimal(123));
        assertEquals(BigDecimal.valueOf(123), util.convertToDecimal(123L));
        assertEquals(BigDecimal.valueOf(123d), util.convertToDecimal(123f));
        assertEquals(BigDecimal.valueOf(123d), util.convertToDecimal(123d));
        assertEquals(BigDecimal.valueOf(123), util.convertToDecimal(BigDecimal.valueOf(123)));

        assertEquals(BigDecimal.valueOf(123), util.convertToDecimal("123 "));
    }

    @Test
    void convertToString() throws SQLException {
        assertEquals("abc", util.convertToString("abc"));
        assertEquals("", util.convertToString(""));
        assertEquals(" ", util.convertToString(" "));

        assertEquals("123", util.convertToString(123));
        assertEquals("123", util.convertToString(123L));
        assertEquals("123.0", util.convertToString(123f));
        assertEquals("123.0", util.convertToString(123d));
        assertEquals("123", util.convertToString(BigDecimal.valueOf(123)));

        assertEquals("2025-10-17", util.convertToString(LocalDate.of(2025, 10, 17)));
        assertEquals("10:51:30", util.convertToString(LocalTime.of(10, 51, 30)));
        assertEquals("2025-10-17T10:51:30", util.convertToString(LocalDateTime.of(2025, 10, 17, 10, 51, 30)));

        assertEquals("2025-10-17", util.convertToString(java.sql.Date.valueOf(LocalDate.of(2025, 10, 17))));
        assertEquals("10:51:30", util.convertToString(java.sql.Time.valueOf(LocalTime.of(10, 51, 30))));
        assertEquals("2025-10-17 10:51:30.0", util.convertToString(java.sql.Timestamp.valueOf(LocalDateTime.of(2025, 10, 17, 10, 51, 30))));

        assertEquals("true", util.convertToString(true));
        assertEquals("false", util.convertToString(false));

        assertEquals("abc ", util.convertToString(new ClobTestMock("abc ")));
    }

    @Test
    void convertToBytes() throws SQLException {
        assertArrayEquals(new byte[] { 1, 2, 3, 0 }, util.convertToBytes(new byte[] { 1, 2, 3, 0 }));

        assertArrayEquals(new byte[] { 1, 2, 3, 0 }, util.convertToBytes(new BlobTestMock(new byte[] { 1, 2, 3, 0 })));
    }

    @Test
    void convertToDate() throws SQLException {
        {
            var value = java.sql.Date.valueOf(LocalDate.of(2025, 10, 17));
            assertSame(value, util.convertToDate(value));
        }

        {
            var value = LocalDate.of(2025, 10, 17);
            testConvertToDate(value, 2025, 10, 17, DEFAULT_ZONE);
        }
        {
            var value = LocalDateTime.of(2025, 10, 17, 23, 59, 59);
            testConvertToDate(value, 2025, 10, 17, DEFAULT_ZONE);
        }
        {
            var value = ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 59, 59), DEFAULT_ZONE).toOffsetDateTime();
            testConvertToDate(value, 2025, 10, 17, DEFAULT_ZONE);
        }
        {
            var value = OffsetDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 59, 59), ZoneOffset.UTC);
            testConvertToDate(value, 2025, 10, 17, ZoneId.of("UTC"));
        }
        {
            var value = ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 59, 59), DEFAULT_ZONE);
            testConvertToDate(value, 2025, 10, 17, DEFAULT_ZONE);
        }
        {
            var value = ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 59, 59), ZoneId.of("UTC"));
            testConvertToDate(value, 2025, 10, 17, ZoneId.of("UTC"));
        }

        {
            var value = java.sql.Timestamp.valueOf(LocalDateTime.of(2025, 10, 17, 23, 59, 59));
            testConvertToDate(value, 2025, 10, 17, DEFAULT_ZONE);
        }

        assertEquals(java.sql.Date.valueOf(LocalDate.of(1970, 1, 1)), util.convertToDate(LocalDate.of(1970, 1, 1)));
        assertEquals(java.sql.Date.valueOf(LocalDate.of(1969, 12, 31)), util.convertToDate(LocalDate.of(1969, 12, 31)));
        assertEquals(sqlDate(1, 1, 1), util.convertToDate(LocalDate.of(1, 1, 1)));
        assertEquals(sqlDate(0, 1, 1), util.convertToDate(LocalDate.of(0, 1, 1)));
        assertEquals(sqlDate(-1, 1, 1), util.convertToDate(LocalDate.of(-1, 1, 1)));

        {
            var value = "2025-10-17";
            testConvertToDate(value, 2025, 10, 17, DEFAULT_ZONE);
        }
    }

    private void testConvertToDate(Object value, int y, int m, int d, ZoneId zone) throws SQLException {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(zone));
        calendar.clear();
        calendar.set(y, m - 1, d);
        var expected = new java.sql.Date(calendar.getTimeInMillis());

        var actual = util.convertToDate(value);
        assertEquals(expected, actual);
    }

    private static java.sql.Date sqlDate(int year, int month, int day) {
        var zdt = ZonedDateTime.of(LocalDate.of(year, month, day), LocalTime.MIN, ZoneId.systemDefault());
        long epochSecond = zdt.toEpochSecond();
        return new java.sql.Date(TimeUnit.SECONDS.toMillis(epochSecond));
    }

    @Test
    void convertToDate_zone0() throws SQLException {
        var zone = ZoneId.systemDefault();
        var calendar = Calendar.getInstance(TimeZone.getTimeZone(zone));
        calendar.clear();
        calendar.set(2025, 10 - 1, 17);
        var expected = new java.sql.Date(calendar.getTimeInMillis());

        assertEquals(expected, util.convertToDate(LocalDate.of(2025, 10, 17), zone));
    }

    @ParameterizedTest
    @ValueSource(strings = { "UTC", "Asia/Tokyo" })
    void convertToDate_zone(String zoneId) throws SQLException {
        var zone = ZoneId.of(zoneId);
        {
            var value = java.sql.Date.valueOf(LocalDate.of(2025, 10, 17));
            assertSame(value, util.convertToDate(value, zone));
        }

        {
            var value = LocalDate.of(2025, 10, 17);
            testConvertToDate(value, zone, 2025, 10, 17, zone);
        }
        {
            var value = LocalDateTime.of(2025, 10, 17, 23, 59, 59);
            testConvertToDate(value, zone, 2025, 10, 17, zone);
        }
        {
            var value = ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 59, 59), DEFAULT_ZONE).toOffsetDateTime();
            testConvertToDate(value, zone, 2025, 10, 17, value.getOffset());
        }
        {
            var value = OffsetDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 59, 59), ZoneOffset.UTC);
            testConvertToDate(value, zone, 2025, 10, 17, ZoneOffset.UTC);
        }
        {
            var value = ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 59, 59), DEFAULT_ZONE);
            testConvertToDate(value, zone, 2025, 10, 17, DEFAULT_ZONE);
        }
        {
            var value = ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 59, 59), ZoneId.of("UTC"));
            testConvertToDate(value, zone, 2025, 10, 17, ZoneId.of("UTC"));
        }

        {
            var value = java.sql.Timestamp.valueOf(LocalDateTime.of(2025, 10, 17, 23, 59, 59));
            testConvertToDate(value, zone, 2025, 10, 17, zone);
        }

        {
            var value = LocalDate.of(1970, 1, 1);
            testConvertToDate(value, zone, 1970, 1, 1, zone);
        }
        {
            var value = LocalDate.of(1969, 12, 31);
            testConvertToDate(value, zone, 1969, 12, 31, zone);
        }

        assertEquals(sqlDate(1, 1, 1, zone), util.convertToDate(LocalDate.of(1, 1, 1), zone));
        assertEquals(sqlDate(0, 1, 1, zone), util.convertToDate(LocalDate.of(0, 1, 1), zone));
        assertEquals(sqlDate(-1, 1, 1, zone), util.convertToDate(LocalDate.of(-1, 1, 1), zone));
    }

    private void testConvertToDate(Object value, ZoneId zone, int y, int m, int d, ZoneId expectedZone) throws SQLException {
        var calendar = Calendar.getInstance(TimeZone.getTimeZone(expectedZone));
        calendar.clear();
        calendar.set(y, m - 1, d);
        var expected = new java.sql.Date(calendar.getTimeInMillis());

        assertEquals(expected, util.convertToDate(value, zone));
    }

    private static java.sql.Date sqlDate(int year, int month, int day, ZoneId zone) {
        var zdt = ZonedDateTime.of(LocalDate.of(year, month, day), LocalTime.MIN, zone);
        long epochSecond = zdt.toEpochSecond();
        return new java.sql.Date(TimeUnit.SECONDS.toMillis(epochSecond));
    }

    @Test
    void convertToTime() throws SQLException {
        var expected = java.sql.Time.valueOf(LocalTime.of(23, 30, 59));
        assertEquals(expected, util.convertToTime(java.sql.Time.valueOf(LocalTime.of(23, 30, 59))));

        assertEquals(expected, util.convertToTime(LocalTime.of(23, 30, 59)));
        assertEquals(expected, util.convertToTime(OffsetTime.of(23, 30, 59, 0, ZoneOffset.UTC)));
        assertEquals(expected, util.convertToTime(OffsetTime.of(23, 30, 59, 0, ZoneOffset.ofHours(+9))));
        assertEquals(expected, util.convertToTime(LocalDateTime.of(2025, 10, 17, 23, 30, 59)));
        assertEquals(expected, util.convertToTime(OffsetDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59), ZoneOffset.UTC)));
        assertEquals(expected, util.convertToTime(OffsetDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59), ZoneOffset.ofHours(+9))));
        assertEquals(expected, util.convertToTime(ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59), ZoneId.systemDefault())));
        assertEquals(expected, util.convertToTime(ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59), ZoneId.of("UTC"))));

        assertEquals(expected, util.convertToTime(java.sql.Timestamp.valueOf(LocalDateTime.of(2025, 10, 17, 23, 30, 59))));

        assertEquals(expected, util.convertToTime("23:30:59"));
    }

    @Test
    void convertToTimestamp() throws SQLException {
        var expected = java.sql.Timestamp.valueOf(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789));
        assertEquals(expected, util.convertToTimestamp(java.sql.Timestamp.valueOf(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789))));

        {
            var value = LocalDate.of(2025, 10, 17);
            testConvertToTimestamp(value, 2025, 10, 17, 0, 0, 0, 0, DEFAULT_ZONE);
        }
        {
            var value = LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789);
            testConvertToTimestamp(value, 2025, 10, 17, 23, 30, 59, 123456789, DEFAULT_ZONE);
        }
        {
            var value = ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), DEFAULT_ZONE).toOffsetDateTime();
            testConvertToTimestamp(value, 2025, 10, 17, 23, 30, 59, 123456789, value.getOffset());
        }
        {
            var value = OffsetDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneOffset.UTC);
            testConvertToTimestamp(value, 2025, 10, 17, 23, 30, 59, 123456789, ZoneOffset.UTC);
        }
        {
            var value = ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), DEFAULT_ZONE);
            testConvertToTimestamp(value, 2025, 10, 17, 23, 30, 59, 123456789, DEFAULT_ZONE);
        }
        {
            var value = ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneId.of("UTC"));
            testConvertToTimestamp(value, 2025, 10, 17, 23, 30, 59, 123456789, ZoneId.of("UTC"));
        }

        assertEquals(java.sql.Timestamp.valueOf(LocalDateTime.of(2025, 10, 17, 0, 0, 0, 0)), util.convertToTimestamp(java.sql.Date.valueOf(LocalDate.of(2025, 10, 17))));
        assertEquals(java.sql.Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0)), util.convertToTimestamp(java.sql.Date.valueOf(LocalDate.of(1970, 1, 1))));
        assertEquals(java.sql.Timestamp.valueOf(LocalDateTime.of(1969, 12, 31, 0, 0, 0, 0)), util.convertToTimestamp(java.sql.Date.valueOf(LocalDate.of(1969, 12, 31))));
        assertEquals(java.sql.Timestamp.valueOf(LocalDateTime.of(1, 1, 1, 0, 0, 0, 0)), util.convertToTimestamp(java.sql.Date.valueOf(LocalDate.of(1, 1, 1))));
        assertEquals(java.sql.Timestamp.valueOf(LocalDateTime.of(0, 1, 1, 0, 0, 0, 0)), util.convertToTimestamp(java.sql.Date.valueOf(LocalDate.of(0, 1, 1))));
        assertEquals(java.sql.Timestamp.valueOf(LocalDateTime.of(-1, 1, 1, 0, 0, 0, 0)), util.convertToTimestamp(java.sql.Date.valueOf(LocalDate.of(-1, 1, 1))));

        {
            var value = "2025-10-17 23:30:59.123456789";
            testConvertToTimestamp(value, 2025, 10, 17, 23, 30, 59, 123456789, DEFAULT_ZONE);
        }
    }

    private void testConvertToTimestamp(Object value, int year, int month, int day, int hour, int minute, int second, int nanos, ZoneId zone) throws SQLException {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(zone));
        calendar.clear();
        calendar.set(year, month - 1, day, hour, minute, second);
        var expected = new java.sql.Timestamp(calendar.getTimeInMillis());
        expected.setNanos(nanos);

        var actual = util.convertToTimestamp(value);
        assertEquals(expected, actual);
    }

    @ParameterizedTest
    @ValueSource(strings = { "UTC", "Asia/Tokyo" })
    void convertToTimestamp_zone(String zoneId) throws SQLException {
        var zone = ZoneId.of(zoneId);
        {
            var expected = java.sql.Timestamp.valueOf(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789));
            assertEquals(expected, util.convertToTimestamp(java.sql.Timestamp.valueOf(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789)), zone));
        }

        {
            var value = LocalDate.of(2025, 10, 17);
            testConvertToTimestamp(value, zone, 2025, 10, 17, 0, 0, 0, 0, zone);
        }
        {
            var value = LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789);
            testConvertToTimestamp(value, zone, 2025, 10, 17, 23, 30, 59, 123456789, zone);
        }
        {
            var value = ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), DEFAULT_ZONE).toOffsetDateTime();
            testConvertToTimestamp(value, zone, 2025, 10, 17, 23, 30, 59, 123456789, value.getOffset());
        }
        {
            var value = OffsetDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneOffset.UTC);
            testConvertToTimestamp(value, zone, 2025, 10, 17, 23, 30, 59, 123456789, ZoneOffset.UTC);
        }
        {
            var value = ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), DEFAULT_ZONE);
            testConvertToTimestamp(value, zone, 2025, 10, 17, 23, 30, 59, 123456789, DEFAULT_ZONE);
        }
        {
            var value = ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneId.of("UTC"));
            testConvertToTimestamp(value, zone, 2025, 10, 17, 23, 30, 59, 123456789, ZoneId.of("UTC"));
        }

        {
            var value = sqlDate(2025, 10, 17);
            testConvertToTimestamp(value, zone, 2025, 10, 17, 0, 0, 0, 0, zone);
        }
        {
            var value = sqlDate(1970, 1, 1);
            testConvertToTimestamp(value, zone, 1970, 1, 1, 0, 0, 0, 0, zone);
        }
        {
            var value = sqlDate(1969, 12, 31);
            testConvertToTimestamp(value, zone, 1969, 12, 31, 0, 0, 0, 0, zone);
        }
        {
            var value = sqlDate(1, 1, 1);
            var expected = new java.sql.Timestamp(sqlDate(1, 1, 1, zone).getTime());
            assertEquals(expected, util.convertToTimestamp(value, zone));
        }
        {
            var value = sqlDate(0, 1, 1);
            var expected = new java.sql.Timestamp(sqlDate(0, 1, 1, zone).getTime());
            assertEquals(expected, util.convertToTimestamp(value, zone));
        }
        {
            var value = sqlDate(-1, 1, 1);
            var expected = new java.sql.Timestamp(sqlDate(-1, 1, 1, zone).getTime());
            assertEquals(expected, util.convertToTimestamp(value, zone));
        }
    }

    private void testConvertToTimestamp(Object value, ZoneId zone, int year, int month, int day, int hour, int minute, int second, int nanos, ZoneId expectedZone) throws SQLException {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(expectedZone));
        calendar.clear();
        calendar.set(year, month - 1, day, hour, minute, second);
        var expected = new java.sql.Timestamp(calendar.getTimeInMillis());
        expected.setNanos(nanos);

        var actual = util.convertToTimestamp(value, zone);
        assertEquals(expected, actual);
    }

    @Test
    void convertToLocalDate() throws SQLException {
        var expected = LocalDate.of(2025, 10, 17);
        assertEquals(expected, util.convertToLocalDate(LocalDate.of(2025, 10, 17)));

        assertEquals(expected, util.convertToLocalDate(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789)));
        assertEquals(expected, util.convertToLocalDate(OffsetDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneOffset.UTC)));
        assertEquals(expected, util.convertToLocalDate(OffsetDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneOffset.ofHours(+9))));
        assertEquals(expected, util.convertToLocalDate(ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneId.systemDefault())));
        assertEquals(expected, util.convertToLocalDate(ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneId.of("UTC"))));

        assertEquals(expected, util.convertToLocalDate(java.sql.Date.valueOf(LocalDate.of(2025, 10, 17))));
        assertEquals(expected, util.convertToLocalDate(java.sql.Timestamp.valueOf(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789))));

        assertEquals(expected, util.convertToLocalDate("2025-10-17"));
        assertDateTimeError(() -> util.convertToLocalDate("23:30:59.123456789"));
        assertEquals(expected, util.convertToLocalDate("2025-10-17 23:30:59.123456789"));
        assertEquals(expected, util.convertToLocalDate("2025-10-17T23:30:59.123456789"));
        assertEquals(expected, util.convertToLocalDate("2025-10-17 23:30:59+00:00"));
        assertEquals(expected, util.convertToLocalDate("2025-10-17 23:30:59+09"));
        assertEquals(expected, util.convertToLocalDate("2025-10-17 23:30:59+09:00"));
        assertEquals(expected, util.convertToLocalDate("2025-10-17 23:30:59-09:00"));
        assertEquals(expected, util.convertToLocalDate("2025-10-17 23:30:59[Asia/Tokyo]"));
        assertEquals(expected, util.convertToLocalDate("2025-10-17 23:30:59+09:00[Asia/Tokyo]"));
    }

    @Test
    void convertToLocalTime() throws SQLException {
        var expected = LocalTime.of(23, 30, 59, 123456789);
        assertEquals(expected, util.convertToLocalTime(LocalTime.of(23, 30, 59, 123456789)));

        assertEquals(expected, util.convertToLocalTime(OffsetTime.of(23, 30, 59, 123456789, ZoneOffset.UTC)));
        assertEquals(expected, util.convertToLocalTime(OffsetTime.of(23, 30, 59, 123456789, ZoneOffset.ofHours(+9))));
        assertEquals(expected, util.convertToLocalTime(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789)));
        assertEquals(expected, util.convertToLocalTime(OffsetDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneOffset.UTC)));
        assertEquals(expected, util.convertToLocalTime(OffsetDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneOffset.ofHours(+9))));
        assertEquals(expected, util.convertToLocalTime(ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneId.systemDefault())));
        assertEquals(expected, util.convertToLocalTime(ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneId.of("UTC"))));

        assertEquals(expected, util.convertToLocalTime(java.sql.Timestamp.valueOf(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789))));

        assertEquals(LocalTime.MIN, util.convertToLocalTime("2025-10-17"));
        assertEquals(LocalTime.of(23, 59), util.convertToLocalTime("23:59:00"));
        assertEquals(LocalTime.of(23, 59), util.convertToLocalTime("23:59"));
        assertEquals(LocalTime.of(23, 30, 59, 100_000_000), util.convertToLocalTime("23:30:59.1"));
        assertEquals(LocalTime.of(23, 30, 59, 123_000_000), util.convertToLocalTime("23:30:59.123"));
        assertEquals(expected, util.convertToLocalTime("23:30:59.123456789"));
        assertEquals(expected, util.convertToLocalTime("23:30:59.123456789+00:00"));
        assertEquals(expected, util.convertToLocalTime("23:30:59.123456789+09:00"));
        assertEquals(expected, util.convertToLocalTime("23:30:59.123456789-09:00"));
        assertEquals(expected, util.convertToLocalTime("23:30:59.123456789[Asia/Tokyo]"));
        assertEquals(expected, util.convertToLocalTime("2025-10-17 23:30:59.123456789"));
        assertEquals(expected, util.convertToLocalTime("2025-10-17T23:30:59.123456789"));
        assertEquals(expected, util.convertToLocalTime("2025-10-17 23:30:59.123456789+09:00"));
        assertEquals(expected, util.convertToLocalTime("2025-10-17 23:30:59.123456789[Asia/Tokyo]"));
        assertEquals(expected, util.convertToLocalTime("2025-10-17 23:30:59.123456789+09:00[Asia/Tokyo]"));
    }

    @Test
    void convertToLocalDateTime() throws SQLException {
        var expected = LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789);
        assertEquals(expected, util.convertToLocalDateTime(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789)));

        assertEquals(LocalDateTime.of(2025, 10, 17, 0, 0, 0), util.convertToLocalDateTime(LocalDate.of(2025, 10, 17)));
        assertEquals(expected, util.convertToLocalDateTime(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789)));
        assertEquals(expected, util.convertToLocalDateTime(OffsetDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneOffset.UTC)));
        assertEquals(expected, util.convertToLocalDateTime(OffsetDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneOffset.ofHours(+9))));
        assertEquals(expected, util.convertToLocalDateTime(ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneId.systemDefault())));
        assertEquals(expected, util.convertToLocalDateTime(ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneId.of("UTC"))));

        assertEquals(LocalDateTime.of(2025, 10, 17, 0, 0, 0), util.convertToLocalDateTime(java.sql.Date.valueOf(LocalDate.of(2025, 10, 17))));
        assertEquals(expected, util.convertToLocalDateTime(java.sql.Timestamp.valueOf(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789))));

        assertEquals(LocalDateTime.of(2025, 10, 17, 0, 0, 0), util.convertToLocalDateTime("2025-10-17"));
        assertDateTimeError(() -> util.convertToLocalDateTime("23:30:59.123456789"));
        assertEquals(expected, util.convertToLocalDateTime("2025-10-17 23:30:59.123456789"));
        assertEquals(expected, util.convertToLocalDateTime("2025-10-17T23:30:59.123456789"));
        assertEquals(expected.withNano(0), util.convertToLocalDateTime("2025-10-17 23:30:59"));
        assertEquals(expected.withNano(100_000_000), util.convertToLocalDateTime("2025-10-17 23:30:59.1"));
        assertEquals(expected.withNano(123_000_000), util.convertToLocalDateTime("2025-10-17 23:30:59.123"));
        assertEquals(expected, util.convertToLocalDateTime("2025-10-17 23:30:59.123456789+00:00"));
        assertEquals(expected, util.convertToLocalDateTime("2025-10-17 23:30:59.123456789+09:00"));
        assertEquals(expected, util.convertToLocalDateTime("2025-10-17 23:30:59.123456789-09:00"));
        assertEquals(expected, util.convertToLocalDateTime("2025-10-17 23:30:59.123456789[Asia/Tokyo]"));
        assertEquals(expected, util.convertToLocalDateTime("2025-10-17 23:30:59.123456789+09:00[Asia/Tokyo]"));
    }

    @Test
    void convertToOffsetTime() throws SQLException {
        var defaultOffset = ZonedDateTime.of(LocalDate.EPOCH, LocalTime.MIN, DEFAULT_ZONE).getOffset();
        var expected = OffsetTime.of(23, 30, 59, 123456789, defaultOffset);
        assertEquals(expected, util.convertToOffsetTime(OffsetTime.of(23, 30, 59, 123456789, defaultOffset)));

        assertEquals(expected, util.convertToOffsetTime(LocalTime.of(23, 30, 59, 123456789)));
        assertEquals(expected, util.convertToOffsetTime(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789)));
        assertEquals(expected, util.convertToOffsetTime(OffsetDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), defaultOffset)));
        assertEquals(expected, util.convertToOffsetTime(ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneId.systemDefault())));

        assertEquals(OffsetTime.of(23, 30, 59, 0, defaultOffset), util.convertToOffsetTime(java.sql.Time.valueOf(LocalTime.of(23, 30, 59, 123456789))));
        assertEquals(expected, util.convertToOffsetTime(java.sql.Timestamp.valueOf(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789))));

        assertEquals(OffsetTime.of(0, 0, 0, 0, defaultOffset), util.convertToOffsetTime("2025-10-17"));
        assertEquals(expected, util.convertToOffsetTime("23:30:59.123456789"));
        assertEquals(expected.withOffsetSameLocal(ZoneOffset.ofHours(0)), util.convertToOffsetTime("23:30:59.123456789+00:00"));
        assertEquals(expected.withOffsetSameLocal(ZoneOffset.ofHours(+9)), util.convertToOffsetTime("23:30:59.123456789+09:00"));
        assertEquals(expected.withOffsetSameLocal(ZoneOffset.ofHours(-9)), util.convertToOffsetTime("23:30:59.123456789-09:00"));
        assertEquals(expected.withOffsetSameLocal(ZoneOffset.ofHours(+9)), util.convertToOffsetTime("23:30:59.123456789[Asia/Tokyo]"));
        assertEquals(expected, util.convertToOffsetTime("2025-10-17 23:30:59.123456789"));
        assertEquals(expected, util.convertToOffsetTime("2025-10-17T23:30:59.123456789"));
        assertEquals(expected.withOffsetSameLocal(ZoneOffset.ofHours(+9)), util.convertToOffsetTime("2025-10-17 23:30:59.123456789+09:00"));
        assertEquals(expected.withOffsetSameLocal(ZoneOffset.ofHours(+9)), util.convertToOffsetTime("2025-10-17 23:30:59.123456789[Asia/Tokyo]"));
        assertEquals(expected.withOffsetSameLocal(ZoneOffset.ofHours(+9)), util.convertToOffsetTime("2025-10-17 23:30:59.123456789+09:00[Asia/Tokyo]"));
    }

    @Test
    void convertToOffsetDateTime() throws SQLException {
        var expected = ZonedDateTime.of(2025, 10, 17, 23, 30, 59, 123456789, DEFAULT_ZONE).toOffsetDateTime();
        var defaultOffset = expected.getOffset();
        assertEquals(expected, util.convertToOffsetDateTime(OffsetDateTime.of(2025, 10, 17, 23, 30, 59, 123456789, defaultOffset)));

        assertEquals(OffsetDateTime.of(2025, 10, 17, 0, 0, 0, 0, defaultOffset), util.convertToOffsetDateTime(LocalDate.of(2025, 10, 17)));
        assertEquals(expected, util.convertToOffsetDateTime(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789)));
        assertEquals(expected, util.convertToOffsetDateTime(ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneId.systemDefault())));

        assertEquals(OffsetDateTime.of(2025, 10, 17, 0, 0, 0, 0, defaultOffset), util.convertToOffsetDateTime(java.sql.Date.valueOf(LocalDate.of(2025, 10, 17))));
        assertEquals(expected, util.convertToOffsetDateTime(java.sql.Timestamp.valueOf(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789))));

        assertEquals(OffsetDateTime.of(2025, 10, 17, 0, 0, 0, 0, defaultOffset), util.convertToOffsetDateTime("2025-10-17"));
        assertDateTimeError(() -> util.convertToOffsetDateTime("23:30:59.123456789"));
        assertEquals(expected, util.convertToOffsetDateTime("2025-10-17 23:30:59.123456789"));
        assertEquals(expected, util.convertToOffsetDateTime("2025-10-17T23:30:59.123456789"));
        assertEquals(expected.withOffsetSameLocal(ZoneOffset.ofHours(0)), util.convertToOffsetDateTime("2025-10-17 23:30:59.123456789Z"));
        assertEquals(expected.withOffsetSameLocal(ZoneOffset.ofHours(+9)), util.convertToOffsetDateTime("2025-10-17 23:30:59.123456789+09:00"));
        assertEquals(expected.withOffsetSameLocal(ZoneOffset.ofHours(+9)), util.convertToOffsetDateTime("2025-10-17 23:30:59.123456789[Asia/Tokyo]"));
        assertEquals(expected.withOffsetSameLocal(ZoneOffset.ofHours(+9)), util.convertToOffsetDateTime("2025-10-17 23:30:59.123456789+09:00[Asia/Tokyo]"));
    }

    @Test
    void convertToZonedDateTime() throws SQLException {
        var expected = ZonedDateTime.of(2025, 10, 17, 23, 30, 59, 123456789, ZoneId.systemDefault());
        assertEquals(expected, util.convertToZonedDateTime(ZonedDateTime.of(2025, 10, 17, 23, 30, 59, 123456789, ZoneId.systemDefault())));

        assertEquals(ZonedDateTime.of(2025, 10, 17, 0, 0, 0, 0, ZoneId.systemDefault()), util.convertToZonedDateTime(LocalDate.of(2025, 10, 17)));
        assertEquals(expected, util.convertToZonedDateTime(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789)));
        assertEquals(ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), DEFAULT_ZONE).toOffsetDateTime().toZonedDateTime(),
                util.convertToZonedDateTime(ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), DEFAULT_ZONE).toOffsetDateTime()));
        assertEquals(expected, util.convertToZonedDateTime(ZonedDateTime.of(LocalDate.of(2025, 10, 17), LocalTime.of(23, 30, 59, 123456789), ZoneId.systemDefault())));

        assertEquals(ZonedDateTime.of(2025, 10, 17, 0, 0, 0, 0, ZoneId.systemDefault()), util.convertToZonedDateTime(java.sql.Date.valueOf(LocalDate.of(2025, 10, 17))));
        assertEquals(expected, util.convertToZonedDateTime(java.sql.Timestamp.valueOf(LocalDateTime.of(2025, 10, 17, 23, 30, 59, 123456789))));

        assertEquals(ZonedDateTime.of(2025, 10, 17, 0, 0, 0, 0, DEFAULT_ZONE), util.convertToZonedDateTime("2025-10-17"));
        assertDateTimeError(() -> util.convertToZonedDateTime("23:30:59.123456789"));
        assertEquals(expected, util.convertToZonedDateTime("2025-10-17 23:30:59.123456789"));
        assertEquals(expected, util.convertToZonedDateTime("2025-10-17T23:30:59.123456789"));
        assertEquals(expected.withZoneSameLocal(ZoneOffset.ofHours(0)), util.convertToZonedDateTime("2025-10-17 23:30:59.123456789Z"));
        assertEquals(expected.withZoneSameLocal(ZoneOffset.ofHours(+9)), util.convertToZonedDateTime("2025-10-17 23:30:59.123456789+09:00"));
        assertEquals(expected.withZoneSameLocal(ZoneId.of("Asia/Tokyo")), util.convertToZonedDateTime("2025-10-17 23:30:59.123456789[Asia/Tokyo]"));
        assertEquals(expected.withZoneSameLocal(ZoneId.of("Asia/Tokyo")), util.convertToZonedDateTime("2025-10-17 23:30:59.123456789+09:00[Asia/Tokyo]"));
    }

    private static void assertDateTimeError(Executable executable) {
        var e = assertThrows(SQLDataException.class, executable);
        assertInstanceOf(DateTimeException.class, e.getCause());
    }

    @Test
    void convertToCharacterStream() throws SQLException {
        assertCharacterStream(util.convertToCharacterStream(new StringReader("abc ")));
        assertCharacterStream(util.convertToCharacterStream("abc "));
        assertCharacterStream(util.convertToCharacterStream(new ClobTestMock("abc ")));
    }

    private static void assertCharacterStream(Reader reader) {
        try (var br = new BufferedReader(reader)) {
            assertEquals("abc ", br.readLine());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void convertToAsciiStream() throws SQLException {
        assertAsciiStream(util.convertToAsciiStream(new ByteArrayInputStream("abc ".getBytes(StandardCharsets.UTF_8))));
        assertAsciiStream(util.convertToAsciiStream("abc "));
    }

    private static void assertAsciiStream(InputStream is) {
        try (is) {
            assertArrayEquals(new byte[] { 'a', 'b', 'c', ' ' }, is.readAllBytes());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void convertToUnicodeStream() throws SQLException {
        assertUnicodeStream(util.convertToUnicodeStream(new ByteArrayInputStream("あいうえお ".getBytes(StandardCharsets.UTF_16))));
        assertUnicodeStream(util.convertToUnicodeStream("あいうえお "));
    }

    private static void assertUnicodeStream(InputStream is) {
        try (is) {
            assertArrayEquals("あいうえお ".getBytes(StandardCharsets.UTF_16), is.readAllBytes());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void convertToBinaryStream() throws SQLException {
        assertBinaryStream(util.convertToBinaryStream(new ByteArrayInputStream(new byte[] { 1, 2, 3, 0 })));
        assertBinaryStream(util.convertToBinaryStream(new byte[] { 1, 2, 3, 0 }));
        assertBinaryStream(util.convertToBinaryStream(new BlobTestMock(new byte[] { 1, 2, 3, 0 })));
    }

    private static void assertBinaryStream(InputStream is) {
        try (is) {
            assertArrayEquals(new byte[] { 1, 2, 3, 0 }, is.readAllBytes());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void convertToBlob() throws SQLException {
        var blob = new BlobTestMock(new byte[] { 1, 2, 3, 0 });
        assertSame(blob, util.convertToBlob(blob));
    }

    @Test
    void convertToClob() throws SQLException {
        var clob = new ClobTestMock("abc");
        assertSame(clob, util.convertToClob(clob));
    }
}
