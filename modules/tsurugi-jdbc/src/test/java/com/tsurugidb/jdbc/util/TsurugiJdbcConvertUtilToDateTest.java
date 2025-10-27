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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;

class TsurugiJdbcConvertUtilToDateTest {

    private static final TsurugiJdbcConvertUtil util = new TsurugiJdbcConvertUtil(() -> TsurugiJdbcFactory.getDefaultFactory());

    @Test
    void toDate_sqlTime() {
        var time = LocalTime.of(1, 2, 3);
        var value = java.sql.Time.valueOf(time);

        {
            var actual = util.toDate(value, null);
            var expected = java.sql.Date.valueOf(LocalDate.EPOCH);
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toDate(value, zone);
            var expected = java.sql.Date.valueOf(ZonedDateTime.of(LocalDate.EPOCH, time, zone).toLocalDate());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toDate(value, zone);
            var expected = java.sql.Date.valueOf(ZonedDateTime.of(LocalDate.EPOCH, time, zone).toLocalDate());
            assertEquals(expected, actual);
        }
    }

    @Test
    void toDate_sqlTimestamp() {
        var ldt = LocalDateTime.of(2025, 10, 27, 23, 30, 59, 123456789);

        {
            var value = java.sql.Timestamp.valueOf(ldt);
            var actual = util.toDate(value, null);
            var expected = java.sql.Date.valueOf(ldt.toLocalDate());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var zdt = ZonedDateTime.of(ldt, zone);
            var value = java.sql.Timestamp.from(zdt.toInstant());
            var actual = util.toDate(value, zone);
            var expected = java.sql.Date.valueOf(zdt.toLocalDate());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var zdt = ZonedDateTime.of(ldt, zone);
            var value = java.sql.Timestamp.from(zdt.toInstant());
            var actual = util.toDate(value, zone);
            var expected = java.sql.Date.valueOf(zdt.toLocalDate());
            assertEquals(expected, actual);
        }
    }

    @Test
    void toDate_LocalDate() {
        var date = LocalDate.of(2025, 10, 27);
        var value = date;

        {
            var actual = util.toDate(value, null);
            var expected = java.sql.Date.valueOf(date);
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toDate(value, zone);
            var expected = java.sql.Date.valueOf(ZonedDateTime.of(date, LocalTime.MIN, zone).toLocalDate());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toDate(value, zone);
            var expected = java.sql.Date.valueOf(ZonedDateTime.of(date, LocalTime.MIN, zone).toLocalDate());
            assertEquals(expected, actual);
        }
    }

    @Test
    void toDate_LocalDateTime() {
        var ldt = LocalDateTime.of(2025, 10, 27, 23, 30, 59, 123456789);
        var value = ldt;

        {
            var actual = util.toDate(value, null);
            var expected = java.sql.Date.valueOf(ldt.toLocalDate());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toDate(value, zone);
            var expected = java.sql.Date.valueOf(ZonedDateTime.of(ldt, zone).toLocalDate());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toDate(value, zone);
            var expected = java.sql.Date.valueOf(ZonedDateTime.of(ldt, zone).toLocalDate());
            assertEquals(expected, actual);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 3, 9 })
    void toDate_OffsetDateTime(int offset) {
        var odt = OffsetDateTime.of(2025, 10, 27, 23, 30, 59, 123456789, ZoneOffset.ofHours(offset));
        var value = odt;

        {
            var actual = util.toDate(value);
            var expected = java.sql.Date.valueOf(odt.toLocalDate());
            assertEquals(expected, actual);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "UTC", "Asia/Tokyo", "GMT+03:00" })
    void toDate_ZonedDateTime(String id) {
        var zdt = ZonedDateTime.of(2025, 10, 27, 23, 30, 59, 123456789, ZoneId.of(id));
        var value = zdt;

        {
            var actual = util.toDate(value);
            var expected = java.sql.Date.valueOf(zdt.toLocalDate());
            assertEquals(expected, actual);
        }
    }

    @Test
    void toTime_sqlTimestamp() {
        var ldt = LocalDateTime.of(2025, 10, 27, 23, 30, 59, 123456789);

        {
            var value = java.sql.Timestamp.valueOf(ldt);
            var actual = util.toTime(value, null);
            var expected = java.sql.Time.valueOf(ldt.toLocalTime());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var zdt = ZonedDateTime.of(ldt, zone);
            var value = java.sql.Timestamp.from(zdt.toInstant());
            var actual = util.toTime(value, zone);
            var expected = java.sql.Time.valueOf(ldt.toLocalTime());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var zdt = ZonedDateTime.of(ldt, zone);
            var value = java.sql.Timestamp.from(zdt.toInstant());
            var actual = util.toTime(value, zone);
            var expected = java.sql.Time.valueOf(ldt.toLocalTime());
            assertEquals(expected, actual);
        }
    }

    @Test
    void toTime_LocalTime() {
        var time = LocalTime.of(1, 2, 3);
        var value = time;

        {
            var actual = util.toTime(value, null);
            var expected = java.sql.Time.valueOf(time);
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toTime(value, zone);
            var expected = java.sql.Time.valueOf(ZonedDateTime.of(LocalDate.EPOCH, time, zone).toLocalTime());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toTime(value, zone);
            var expected = java.sql.Time.valueOf(ZonedDateTime.of(LocalDate.EPOCH, time, zone).toLocalTime());
            assertEquals(expected, actual);
        }
    }

    @Test
    void toTimestamp_sqlDate() {
        var date = LocalDate.of(2025, 10, 27);
        var value = java.sql.Date.valueOf(date);

        {
            var actual = util.toTimestamp(value, null);
            var expected = java.sql.Timestamp.valueOf(date.atStartOfDay());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toTimestamp(value, zone);
            var expected = java.sql.Timestamp.from(ZonedDateTime.of(date, LocalTime.MIN, zone).toInstant());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toTimestamp(value, zone);
            var expected = java.sql.Timestamp.from(ZonedDateTime.of(date, LocalTime.MIN, zone).toInstant());
            assertEquals(expected, actual);
        }
    }

    @Test
    void toTimestamp_sqlTime() {
        var time = LocalTime.of(1, 2, 3);
        var value = java.sql.Time.valueOf(time);

        {
            var actual = util.toTimestamp(value, null);
            var expected = java.sql.Timestamp.valueOf(LocalDateTime.of(LocalDate.EPOCH, time));
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toTimestamp(value, zone);
            var expected = java.sql.Timestamp.from(ZonedDateTime.of(LocalDate.EPOCH, time, zone).toInstant());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toTimestamp(value, zone);
            var expected = java.sql.Timestamp.from(ZonedDateTime.of(LocalDate.EPOCH, time, zone).toInstant());
            assertEquals(expected, actual);
        }
    }

    @Test
    void toTimestamp_LocalDate() {
        var date = LocalDate.of(2025, 10, 27);
        var value = date;

        {
            var actual = util.toTimestamp(value, null);
            var expected = java.sql.Timestamp.valueOf(date.atStartOfDay());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toTimestamp(value, zone);
            var expected = java.sql.Timestamp.from(ZonedDateTime.of(date, LocalTime.MIN, zone).toInstant());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toTimestamp(value, zone);
            var expected = java.sql.Timestamp.from(ZonedDateTime.of(date, LocalTime.MIN, zone).toInstant());
            assertEquals(expected, actual);
        }
    }

    @Test
    void toTimestamp_LocalDateTime() {
        var ldt = LocalDateTime.of(2025, 10, 27, 23, 30, 59, 123456789);
        var value = ldt;

        {
            var actual = util.toTimestamp(value, null);
            var expected = java.sql.Timestamp.valueOf(ldt);
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toTimestamp(value, zone);
            var expected = java.sql.Timestamp.from(ZonedDateTime.of(ldt, zone).toInstant());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toTimestamp(value, zone);
            var expected = java.sql.Timestamp.from(ZonedDateTime.of(ldt, zone).toInstant());
            assertEquals(expected, actual);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 3, 9 })
    void toTimestamp_OffsetDateTime(int offset) {
        var odt = OffsetDateTime.of(2025, 10, 27, 23, 30, 59, 123456789, ZoneOffset.ofHours(offset));
        var value = odt;

        {
            var actual = util.toTimestamp(value);
            var expected = java.sql.Timestamp.from(odt.toInstant());
            assertEquals(expected, actual);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "UTC", "Asia/Tokyo", "GMT+03:00" })
    void toTimestamp_ZonedDateTime(String id) {
        var zdt = ZonedDateTime.of(2025, 10, 27, 23, 30, 59, 123456789, ZoneId.of(id));
        var value = zdt;

        {
            var actual = util.toTimestamp(value);
            var expected = java.sql.Timestamp.from(zdt.toInstant());
            assertEquals(expected, actual);
        }
    }

    @Test
    void toLocalDate_sqlDate() {
        var date = LocalDate.of(2025, 10, 27);
        var value = java.sql.Date.valueOf(date);

        {
            var actual = util.toLocalDate(value, null);
            var expected = date;
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toLocalDate(value, zone);
            var expected = ZonedDateTime.of(date, LocalTime.MIN, zone).toLocalDate();
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toLocalDate(value, zone);
            var expected = ZonedDateTime.of(date, LocalTime.MIN, zone).toLocalDate();
            assertEquals(expected, actual);
        }
    }

    @Test
    void toLocalDate_sqlTime() {
        var time = LocalTime.of(1, 2, 3);
        var value = java.sql.Time.valueOf(time);

        {
            var actual = util.toLocalDate(value, null);
            var expected = LocalDate.EPOCH;
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toLocalDate(value, zone);
            var expected = ZonedDateTime.of(LocalDate.EPOCH, time, zone).toLocalDate();
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toLocalDate(value, zone);
            var expected = ZonedDateTime.of(LocalDate.EPOCH, time, zone).toLocalDate();
            assertEquals(expected, actual);
        }
    }

    @Test
    void toLocalDate_sqlTimestamp() {
        var ldt = LocalDateTime.of(2025, 10, 27, 23, 30, 59, 123456789);

        {
            var value = java.sql.Timestamp.valueOf(ldt);
            var actual = util.toLocalDate(value, null);
            var expected = ldt.toLocalDate();
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var zdt = ZonedDateTime.of(ldt, zone);
            var value = java.sql.Timestamp.from(zdt.toInstant());
            var actual = util.toLocalDate(value, zone);
            var expected = ldt.toLocalDate();
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var zdt = ZonedDateTime.of(ldt, zone);
            var value = java.sql.Timestamp.from(zdt.toInstant());
            var actual = util.toLocalDate(value, zone);
            var expected = ldt.toLocalDate();
            assertEquals(expected, actual);
        }
    }

    @Test
    void toLocalTime_sqlDate() {
        var date = LocalDate.of(2025, 10, 27);
        var value = java.sql.Date.valueOf(date);

        {
            var actual = util.toLocalTime(value, null);
            var expected = LocalTime.MIN;
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toLocalTime(value, zone);
            var expected = ZonedDateTime.of(date, LocalTime.MIN, zone).toLocalTime();
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toLocalTime(value, zone);
            var expected = ZonedDateTime.of(date, LocalTime.MIN, zone).toLocalTime();
            assertEquals(expected, actual);
        }
    }

    @Test
    void toLocalTime_sqlTime() {
        var time = LocalTime.of(1, 2, 3);
        var value = java.sql.Time.valueOf(time);

        {
            var actual = util.toLocalTime(value, null);
            var expected = time;
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toLocalTime(value, zone);
            var expected = ZonedDateTime.of(LocalDate.EPOCH, time, zone).toLocalTime();
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toLocalTime(value, zone);
            var expected = ZonedDateTime.of(LocalDate.EPOCH, time, zone).toLocalTime();
            assertEquals(expected, actual);
        }
    }

    @Test
    void toLocalTime_sqlTimestamp() {
        var ldt = LocalDateTime.of(2025, 10, 27, 23, 30, 59, 123456789);

        {
            var value = java.sql.Timestamp.valueOf(ldt);
            var actual = util.toLocalTime(value, null);
            var expected = ldt.toLocalTime();
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var zdt = ZonedDateTime.of(ldt, zone);
            var value = java.sql.Timestamp.from(zdt.toInstant());
            var actual = util.toLocalTime(value, zone);
            var expected = ldt.toLocalTime();
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var zdt = ZonedDateTime.of(ldt, zone);
            var value = java.sql.Timestamp.from(zdt.toInstant());
            var actual = util.toLocalTime(value, zone);
            var expected = ldt.toLocalTime();
            assertEquals(expected, actual);
        }
    }

    @Test
    void toLocalDateTime_sqlDate() {
        var date = LocalDate.of(2025, 10, 27);
        var value = java.sql.Date.valueOf(date);

        {
            var actual = util.toLocalDateTime(value, null);
            var expected = LocalDateTime.of(date, LocalTime.MIN);
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toLocalDateTime(value, zone);
            var expected = ZonedDateTime.of(date, LocalTime.MIN, zone).toLocalDateTime();
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toLocalDateTime(value, zone);
            var expected = ZonedDateTime.of(date, LocalTime.MIN, zone).toLocalDateTime();
            assertEquals(expected, actual);
        }
    }

    @Test
    void toLocalDateTime_sqlTime() {
        var time = LocalTime.of(1, 2, 3);
        var value = java.sql.Time.valueOf(time);

        {
            var actual = util.toLocalDateTime(value, null);
            var expected = LocalDateTime.of(LocalDate.EPOCH, time);
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toLocalDateTime(value, zone);
            var expected = ZonedDateTime.of(LocalDate.EPOCH, time, zone).toLocalDateTime();
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toLocalDateTime(value, zone);
            var expected = ZonedDateTime.of(LocalDate.EPOCH, time, zone).toLocalDateTime();
            assertEquals(expected, actual);
        }
    }

    @Test
    void toLocalDateTime_sqlTimestamp() {
        var ldt = LocalDateTime.of(2025, 10, 27, 23, 30, 59, 123456789);

        {
            var value = java.sql.Timestamp.valueOf(ldt);
            var actual = util.toLocalDateTime(value, null);
            var expected = ldt;
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var zdt = ZonedDateTime.of(ldt, zone);
            var value = java.sql.Timestamp.from(zdt.toInstant());
            var actual = util.toLocalDateTime(value, zone);
            var expected = ldt;
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var zdt = ZonedDateTime.of(ldt, zone);
            var value = java.sql.Timestamp.from(zdt.toInstant());
            var actual = util.toLocalDateTime(value, zone);
            var expected = ldt;
            assertEquals(expected, actual);
        }
    }

    @Test
    void toZonedDateTime_sqlDate() {
        var date = LocalDate.of(2025, 10, 27);
        var value = java.sql.Date.valueOf(date);

        {
            var actual = util.toZonedDateTime(value, null);
            var expected = ZonedDateTime.of(date, LocalTime.MIN, ZoneId.systemDefault());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toZonedDateTime(value, zone);
            var expected = ZonedDateTime.of(date, LocalTime.MIN, zone);
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toZonedDateTime(value, zone);
            var expected = ZonedDateTime.of(date, LocalTime.MIN, zone);
            assertEquals(expected, actual);
        }
    }

    @Test
    void toZonedDateTime_sqlTime() {
        var time = LocalTime.of(1, 2, 3);
        var value = java.sql.Time.valueOf(time);

        {
            var actual = util.toZonedDateTime(value, null);
            var expected = ZonedDateTime.of(LocalDate.EPOCH, time, ZoneId.systemDefault());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toZonedDateTime(value, zone);
            var expected = ZonedDateTime.of(LocalDate.EPOCH, time, zone);
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toZonedDateTime(value, zone);
            var expected = ZonedDateTime.of(LocalDate.EPOCH, time, zone);
            assertEquals(expected, actual);
        }
    }

    @Test
    void toZonedDateTime_sqlTimestamp() {
        var ldt = LocalDateTime.of(2025, 10, 27, 23, 30, 59, 123456789);

        {
            var value = java.sql.Timestamp.valueOf(ldt);
            var actual = util.toZonedDateTime(value, null);
            var expected = ZonedDateTime.of(ldt, ZoneId.systemDefault());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var zdt = ZonedDateTime.of(ldt, zone);
            var value = java.sql.Timestamp.from(zdt.toInstant());
            var actual = util.toZonedDateTime(value, zone);
            var expected = zdt;
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var zdt = ZonedDateTime.of(ldt, zone);
            var value = java.sql.Timestamp.from(zdt.toInstant());
            var actual = util.toZonedDateTime(value, zone);
            var expected = zdt;
            assertEquals(expected, actual);
        }
    }

    @Test
    void toZonedDateTime_LocalDate() {
        var date = LocalDate.of(2025, 10, 27);
        var value = date;

        {
            var actual = util.toZonedDateTime(value, null);
            var expected = ZonedDateTime.of(date, LocalTime.MIN, ZoneId.systemDefault());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toZonedDateTime(value, zone);
            var expected = ZonedDateTime.of(date, LocalTime.MIN, zone);
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toZonedDateTime(value, zone);
            var expected = ZonedDateTime.of(date, LocalTime.MIN, zone);
            assertEquals(expected, actual);
        }
    }

    @Test
    void toZonedDateTime_LocalDateTime() {
        var ldt = LocalDateTime.of(2025, 10, 27, 23, 30, 59, 123456789);
        var value = ldt;

        {
            var actual = util.toZonedDateTime(value, null);
            var expected = ZonedDateTime.of(ldt, ZoneId.systemDefault());
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("UTC");
            var actual = util.toZonedDateTime(value, zone);
            var expected = ZonedDateTime.of(ldt, zone);
            assertEquals(expected, actual);
        }
        {
            var zone = ZoneId.of("Asia/Tokyo");
            var actual = util.toZonedDateTime(value, zone);
            var expected = ZonedDateTime.of(ldt, zone);
            assertEquals(expected, actual);
        }
    }
}
