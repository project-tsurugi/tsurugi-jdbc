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
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;

/**
 * Tsurugi JDBC TIMESTAMP WITH TIME ZONE test.
 */
public class JdbcDbTypeTimestampTzTest extends JdbcDbTypeTester<OffsetDateTime> {

    @Override
    protected String sqlType() {
        return "timestamp with time zone";
    }

    private static final OffsetDateTime NOW = OffsetDateTime.now();

    @Override
    protected List<OffsetDateTime> values() {
        var list = new ArrayList<OffsetDateTime>();
        list.add(NOW);
        for (var offset : List.of(ZoneOffset.UTC, ZoneOffset.ofHours(9))) {
            list.add(OffsetDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_999, offset));
            list.add(OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, offset));
            list.add(OffsetDateTime.of(2025, 2, 7, 12, 30, 59, 123456789, offset));
            list.add(OffsetDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_999, offset));
            list.add(OffsetDateTime.of(-1, 1, 1, 0, 0, 0, 0, offset));
            list.add(OffsetDateTime.of(0, 1, 1, 0, 0, 0, 0, offset));
        }
        list.add(null);
        return list;
    }

    @Override
    protected TgBindVariable<OffsetDateTime> bindVariable(String name) {
        return TgBindVariable.ofOffsetDateTime(name);
    }

    @Override
    protected TgBindParameter bindParameter(String name, OffsetDateTime value) {
        return TgBindParameter.of(name, value);
    }

    @Override
    protected OffsetDateTime get(TsurugiResultEntity entity, String name) {
        return entity.getOffsetDateTime(name);
    }

    @Override
    protected OffsetDateTime get(ResultSet rs, int columnIndex) throws SQLException {
        OffsetDateTime value = (OffsetDateTime) rs.getObject(columnIndex);
        if (rs.wasNull()) {
            assertNull(value);
        }
        return value;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, OffsetDateTime value) throws SQLException {
        if (value != null) {
            ps.setObject(parameterIndex, value);
        } else {
            ps.setNull(parameterIndex, java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
        }
    }

    @Override
    protected void assertValueList(List<OffsetDateTime> expected, List<OffsetDateTime> actual) {
        try {
            assertEquals(expected.size(), actual.size());
            for (int i = 0; i < actual.size(); i++) {
                var e = expectedTimestamp(expected.get(i));
                var a = actual.get(i);
                assertEquals(e, a);
            }
        } catch (Throwable e) {
            LOG.error("{}\nexpected={}\nactual=  {}", e.getMessage(), expected, actual);
            throw e;
        }
    }

    private static OffsetDateTime expectedTimestamp(OffsetDateTime value) {
        if (value == null) {
            return null;
        }
        return value.withOffsetSameInstant(ZoneOffset.UTC);
    }

    @Override
    protected void assertException(OffsetDateTime expected, ValueType valueType, SQLDataException e) {
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
    protected void assertValue(OffsetDateTime expected, ValueType valueType, Object actual) {
        expected = expectedTimestamp(expected);

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
        case LOCAL_DATE_TIME:
            assertEquals(expected.toLocalDateTime(), actual);
            return;
        case OFFSET_TIME:
            assertEquals(expected.toOffsetTime(), actual);
            return;
        case OFFSET_DATE_TIME:
        case OBJECT:
            assertEquals(expected, actual);
            return;
        case ZONED_DATE_TIME:
            assertEquals(expected.toZonedDateTime(), actual);
            return;
        default:
            assertEquals(expected, actual, "valueType=" + valueType);
            return;
        }
    }

    private java.sql.Date toSqlDate(OffsetDateTime value) {
        var odt = value.truncatedTo(ChronoUnit.DAYS);
        long epochSecond = odt.toEpochSecond();
        return new java.sql.Date(TimeUnit.SECONDS.toMillis(epochSecond));
    }

    private java.sql.Time toSqlTime(OffsetDateTime value) {
        return java.sql.Time.valueOf(value.toLocalTime());
    }

    private java.sql.Timestamp toSqlTimestamp(OffsetDateTime value) {
        return java.sql.Timestamp.from(value.toInstant());
    }
}
