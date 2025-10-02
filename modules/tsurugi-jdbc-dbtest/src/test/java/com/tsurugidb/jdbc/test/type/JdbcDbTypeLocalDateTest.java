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
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;

/**
 * Tsurugi JDBC DATE test.
 */
public class JdbcDbTypeLocalDateTest extends JdbcDbTypeTester<LocalDate> {

    @Override
    protected String sqlType() {
        return "date";
    }

    @Override
    protected List<LocalDate> values() {
        var list = new ArrayList<LocalDate>();
        list.add(LocalDate.now());
        list.add(LocalDate.of(1970, 1, 1));
        list.add(LocalDate.of(-1, 1, 1));
        list.add(LocalDate.of(0, 1, 1));
        list.add(LocalDate.of(1, 1, 1));
        list.add(LocalDate.of(9999, 12, 31));
        list.add(null);
        return list;
    }

    @Override
    protected TgBindVariable<LocalDate> bindVariable(String name) {
        return TgBindVariable.ofDate(name);
    }

    @Override
    protected TgBindParameter bindParameter(String name, LocalDate value) {
        return TgBindParameter.of(name, value);
    }

    @Override
    protected LocalDate get(TsurugiResultEntity entity, String name) {
        return entity.getDate(name);
    }

    @Override
    protected LocalDate get(ResultSet rs, int columnIndex) throws SQLException {
        LocalDate value = (LocalDate) rs.getObject(columnIndex);
        if (rs.wasNull()) {
            assertNull(value);
        }
        return value;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, LocalDate value) throws SQLException {
        if (value != null) {
            ps.setObject(parameterIndex, value);
        } else {
            ps.setNull(parameterIndex, java.sql.Types.DATE);
        }
    }

    @Override
    protected void assertException(LocalDate expected, ValueType valueType, SQLDataException e) {
        switch (valueType) {
        case OBJECT:
        case STRING:
        case DATE:
        case TIMESTAMP:
        case LOCAL_DATE:
        case LOCAL_DATE_TIME:
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
    protected void assertValue(LocalDate expected, ValueType valueType, Object actual) {
        switch (valueType) {
        case STRING:
            assertEquals(expected.toString(), actual);
            return;
        case DATE:
            assertEquals(toSqlDate(expected), actual);
            return;
        case TIMESTAMP:
            assertEquals(toSqlTimestamp(expected), actual);
            return;
        case LOCAL_DATE:
            assertEquals(expected, actual);
            return;
        case LOCAL_DATE_TIME:
            assertEquals(expected.atStartOfDay(), actual);
            return;
        case OFFSET_DATE_TIME:
            assertEquals(expected.atStartOfDay().atOffset(ZoneOffset.UTC), actual);
            return;
        case ZONED_DATE_TIME:
            assertEquals(expected.atStartOfDay().atZone(ZoneId.of("Z")), actual);
            return;
        default:
            assertEquals(expected, actual, "valueType=" + valueType);
            return;
        }
    }

    private java.sql.Date toSqlDate(LocalDate value) {
        long epochDay = value.toEpochDay();
        return new java.sql.Date(TimeUnit.DAYS.toMillis(epochDay));
    }

    private java.sql.Timestamp toSqlTimestamp(LocalDate value) {
        var zone = ZoneId.systemDefault();
        var zdt = value.atStartOfDay(zone);
        long epochSecond = zdt.toEpochSecond();
        return new java.sql.Timestamp(TimeUnit.SECONDS.toMillis(epochSecond));
    }
}
