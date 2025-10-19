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
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;

/**
 * Tsurugi JDBC TIMESTAMP test.
 */
public class JdbcDbTypeTimestampTest extends JdbcDbTypeTester<LocalDateTime> {

    @Override
    protected String sqlType() {
        return "timestamp";
    }

    @Override
    protected List<LocalDateTime> values() {
        var list = new ArrayList<LocalDateTime>();
        list.add(LocalDateTime.now());
        list.add(LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_999));
        list.add(LocalDateTime.of(1970, 1, 1, 0, 0, 0));
        list.add(LocalDateTime.of(2025, 2, 7, 12, 30, 59, 123456789));
        list.add(LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_999));
        list.add(LocalDateTime.of(-1, 1, 1, 0, 0, 0));
        list.add(LocalDateTime.of(0, 1, 1, 0, 0, 0));
        list.add(LocalDateTime.of(2025, 7, 3, 8, 17, 19, 210_000_000));
        list.add(null);
        return list;
    }

    @Override
    protected TgBindVariable<LocalDateTime> bindVariable(String name) {
        return TgBindVariable.ofDateTime(name);
    }

    @Override
    protected TgBindParameter bindParameter(String name, LocalDateTime value) {
        return TgBindParameter.of(name, value);
    }

    @Override
    protected LocalDateTime get(TsurugiResultEntity entity, String name) {
        return entity.getDateTime(name);
    }

    @Override
    protected LocalDateTime get(ResultSet rs, int columnIndex) throws SQLException {
        LocalDateTime value = (LocalDateTime) rs.getObject(columnIndex);
        if (rs.wasNull()) {
            assertNull(value);
        }
        return value;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, LocalDateTime value) throws SQLException {
        if (value != null) {
            ps.setObject(parameterIndex, value);
        } else {
            ps.setNull(parameterIndex, java.sql.Types.TIMESTAMP);
        }
    }

    @Override
    protected void assertException(LocalDateTime expected, ValueType valueType, SQLDataException e) {
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
    protected void assertValue(LocalDateTime expected, ValueType valueType, Object actual) {
        switch (valueType) {
        case STRING:
            assertEquals(expected.toString(), actual);
            return;
        case DATE:
            assertEquals(toSqlDate(expected), actual);
            return;
        case TIME:
            assertEquals(toSqlTime(expected), actual);
            return;
        case TIMESTAMP:
            assertEquals(toSqlTimestamp(expected), actual);
            return;
        case LOCAL_DATE:
            assertEquals(expected.toLocalDate(), actual);
            return;
        case LOCAL_TIME:
            assertEquals(expected.toLocalTime(), actual);
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

    private java.sql.Date toSqlDate(LocalDateTime value) {
        var zdt = toZonedDateTime(value).truncatedTo(ChronoUnit.DAYS);
        long epochSecond = zdt.toEpochSecond();
        return new java.sql.Date(TimeUnit.SECONDS.toMillis(epochSecond));
    }

    private java.sql.Time toSqlTime(LocalDateTime value) {
        var zdt = toZonedDateTime(value);
        return java.sql.Time.valueOf(zdt.toLocalTime());
    }

    private java.sql.Timestamp toSqlTimestamp(LocalDateTime value) {
        var zdt = toZonedDateTime(value);
        long epochSecond = zdt.toEpochSecond();
        var timestamp = new java.sql.Timestamp(TimeUnit.SECONDS.toMillis(epochSecond));
        timestamp.setNanos(value.getNano());
        return timestamp;
    }

    private OffsetDateTime toOffsetDateTime(LocalDateTime value) {
        return toZonedDateTime(value).toOffsetDateTime();
    }

    private ZonedDateTime toZonedDateTime(LocalDateTime value) {
        return value.atZone(ZoneId.systemDefault());
    }
}
