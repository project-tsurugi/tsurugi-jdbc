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
package com.tsurugidb.jdbc.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;

/**
 * Tsurugi JDBC TIMESTAMP test.
 */
public class JdbcDbTypeSqlTimestampTest extends JdbcDbTypeTester<java.sql.Timestamp> {

    @Override
    protected String sqlType() {
        return "timestamp";
    }

    @Override
    protected List<java.sql.Timestamp> values() {
        var list = new ArrayList<java.sql.Timestamp>();
        list.add(timestamp(LocalDateTime.now()));
        list.add(timestamp(1969, 12, 31, 23, 59, 59, 999_999_999));
        list.add(timestamp(1970, 1, 1, 0, 0, 0));
        list.add(timestamp(2025, 2, 7, 12, 30, 59, 123456789));
        list.add(timestamp(9999, 12, 31, 23, 59, 59, 999_999_999));
        list.add(timestamp(-1, 1, 1, 0, 0, 0));
        list.add(timestamp(0, 1, 1, 0, 0, 0));
        list.add(timestamp(2025, 7, 3, 8, 17, 19, 210_000_000));
        list.add(null);
        return list;
    }

    private java.sql.Timestamp timestamp(int year, int month, int day, int hour, int minute, int second) {
        return timestamp(LocalDateTime.of(year, month, day, hour, minute, second));
    }

    private java.sql.Timestamp timestamp(int year, int month, int day, int hour, int minute, int second, int nano) {
        return timestamp(LocalDateTime.of(year, month, day, hour, minute, second, nano));
    }

    private java.sql.Timestamp timestamp(LocalDateTime value) {
        return toSqlTimestamp(value);
    }

    @Override
    protected TgBindVariable<java.sql.Timestamp> bindVariable(String name) {
        return new TgBindVariable<>(name, TgDataType.DATE_TIME) {
            @Override
            public TgBindParameter bind(java.sql.Timestamp value) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TgBindVariable<java.sql.Timestamp> clone(String name) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    protected TgBindParameter bindParameter(String name, java.sql.Timestamp value) {
        var timestamp = toLocalDateTime(value);
        return TgBindParameter.of(name, timestamp);
    }

    @Override
    protected java.sql.Timestamp get(TsurugiResultEntity entity, String name) {
        var value = entity.getDateTime(name);
        if (value == null) {
            return null;
        }
        return toSqlTimestamp(value);
    }

    @Override
    protected java.sql.Timestamp get(ResultSet rs, int columnIndex) throws SQLException {
        var value = rs.getTimestamp(columnIndex);
        if (rs.wasNull()) {
            assertNull(value);
        }
        return value;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, java.sql.Timestamp value) throws SQLException {
        if (value != null) {
            ps.setTimestamp(parameterIndex, value);
        } else {
            ps.setNull(parameterIndex, java.sql.Types.TIMESTAMP);
        }
    }

    @Override
    protected void assertException(java.sql.Timestamp expected, ValueType valueType, SQLDataException e) {
        switch (valueType) {
        case OBJECT:
        case STRING:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case LOCAL_DATE:
        case OFFSET_TIME:
        case OFFSET_DATE_TIME:
        case ZONED_DATE_TIME:
            fail(e);
            return;
        default:
            assertTrue(e.getMessage().contains("unsupported type"), () -> e.getMessage());
            return;
        }
    }

    @Override
    protected void assertValue(java.sql.Timestamp expected, ValueType valueType, Object actual) {
        switch (valueType) {
        case STRING:
            assertEquals(toLocalDateTime(expected).toString(), actual);
            return;
        case DATE:
            assertEquals(toSqlDate(expected), actual);
            return;
        case TIME:
            assertEquals(toSqlTime(expected), actual);
            return;
        case TIMESTAMP:
            assertEquals(expected, actual);
            return;
        case LOCAL_DATE:
            assertEquals(toLocalDateTime(expected).toLocalDate(), actual);
            return;
        case LOCAL_TIME:
            assertEquals(toLocalDateTime(expected).toLocalTime(), actual);
            return;
        case LOCAL_DATE_TIME:
        case OBJECT:
            assertEquals(toLocalDateTime(expected), actual);
            return;
        case OFFSET_TIME:
            assertEquals(toOffsetDateTime(expected).toOffsetTime(), actual);
            return;
        case OFFSET_DATE_TIME:
            assertEquals(toOffsetDateTime(expected), actual);
            return;
        case ZONED_DATE_TIME:
            assertEquals(toZonedDateTime(expected), actual);
            return;
        default:
            assertEquals(expected, actual, "valueType=" + valueType);
            return;
        }
    }

    private java.sql.Date toSqlDate(java.sql.Timestamp value) {
        var zdt = ZonedDateTime.ofInstant(value.toInstant(), ZoneId.systemDefault()).truncatedTo(ChronoUnit.DAYS);
        long epochSecond = zdt.toEpochSecond();
        return new java.sql.Date(TimeUnit.SECONDS.toMillis(epochSecond));
    }

    private java.sql.Time toSqlTime(java.sql.Timestamp value) {
        var zdt = toZonedDateTime(value);
        return java.sql.Time.valueOf(zdt.toLocalTime());
    }

    private java.sql.Timestamp toSqlTimestamp(LocalDateTime value) {
        var zdt = value.atZone(ZoneId.systemDefault());
        long epochSecond = zdt.toEpochSecond();
        var timestamp = new java.sql.Timestamp(TimeUnit.SECONDS.toMillis(epochSecond));
        timestamp.setNanos(value.getNano());
        return timestamp;
    }

    private LocalDateTime toLocalDateTime(java.sql.Timestamp value) {
        if (value == null) {
            return null;
        }
        return toZonedDateTime(value).toLocalDateTime();
    }

    private OffsetDateTime toOffsetDateTime(java.sql.Timestamp value) {
        return toZonedDateTime(value).toOffsetDateTime();
    }

    private ZonedDateTime toZonedDateTime(java.sql.Timestamp value) {
        var instant = Instant.ofEpochMilli(value.getTime());
        return instant.atZone(ZoneId.systemDefault()).withNano(value.getNanos());
    }
}
