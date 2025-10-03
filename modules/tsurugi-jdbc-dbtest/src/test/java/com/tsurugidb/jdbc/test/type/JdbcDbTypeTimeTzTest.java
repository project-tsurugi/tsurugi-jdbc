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
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;

/**
 * Tsurugi JDBC TIME WITH TIME ZONE test.
 */
public class JdbcDbTypeTimeTzTest extends JdbcDbTypeTester<OffsetTime> {

    @Override
    protected String sqlType() {
        return "time with time zone";
    }

    private static final OffsetTime NOW = OffsetTime.now();

    @Override
    protected List<OffsetTime> values() {
        var offset = ZoneOffset.ofHours(9);

        var list = new ArrayList<OffsetTime>();
        list.add(NOW);
        list.add(OffsetTime.of(0, 0, 0, 0, ZoneOffset.UTC));
        list.add(OffsetTime.of(1, 30, 59, 123456789, ZoneOffset.UTC));
        list.add(OffsetTime.of(23, 59, 59, 999_999_999, ZoneOffset.UTC));
        list.add(OffsetTime.of(0, 0, 0, 0, offset));
        list.add(OffsetTime.of(1, 30, 59, 123456789, offset));
        list.add(OffsetTime.of(23, 59, 59, 999_999_999, offset));
        list.add(OffsetTime.of(17, 12, 31, 450_000_000, offset));
        list.add(OffsetTime.of(17, 12, 31, 450_000, offset));
        list.add(null);
        return list;
    }

    @Override
    protected TgBindVariable<OffsetTime> bindVariable(String name) {
        return TgBindVariable.ofOffsetTime(name);
    }

    @Override
    protected TgBindParameter bindParameter(String name, OffsetTime value) {
        return TgBindParameter.of(name, value);
    }

    @Override
    protected OffsetTime get(TsurugiResultEntity entity, String name) {
        return entity.getOffsetTime(name);
    }

    @Override
    protected OffsetTime get(ResultSet rs, int columnIndex) throws SQLException {
        OffsetTime value = (OffsetTime) rs.getObject(columnIndex);
        if (rs.wasNull()) {
            assertNull(value);
        }
        return value;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, OffsetTime value) throws SQLException {
        if (value != null) {
            ps.setObject(parameterIndex, value);
        } else {
            ps.setNull(parameterIndex, java.sql.Types.TIME_WITH_TIMEZONE);
        }
    }

    @Override
    protected void assertValueList(List<OffsetTime> expected, List<OffsetTime> actual) {
        try {
            assertEquals(expected.size(), actual.size());
            for (int i = 0; i < actual.size(); i++) {
                assertEquals(expectedTime(expected.get(i)), actual.get(i));
            }
        } catch (Throwable e) {
            LOG.error("{}\nexpected={}\nactual=  {}", e.getMessage(), expected, actual);
            throw e;
        }
    }

    private static OffsetTime expectedTime(OffsetTime value) {
        if (value == null) {
            return null;
        }
        return value.withOffsetSameInstant(ZoneOffset.UTC);
    }

    @Override
    protected void assertException(OffsetTime expected, ValueType valueType, SQLDataException e) {
        switch (valueType) {
        case OBJECT:
        case STRING:
        case TIME:
        case LOCAL_TIME:
        case OFFSET_TIME:
            fail(e);
            return;
        default:
            assertTrue(e.getMessage().contains("unsupported type"), () -> e.getMessage());
            return;
        }
    }

    @Override
    protected void assertValue(OffsetTime expected, ValueType valueType, Object actual) {
        switch (valueType) {
        case STRING:
            assertEquals(expectedTime(expected).toString(), actual);
            return;
        case TIME:
            assertEquals(toSqlTime(expectedTime(expected)), actual);
            return;
        case LOCAL_TIME:
            assertEquals(expectedTime(expected).toLocalTime(), actual);
            return;
        case OFFSET_TIME:
        case OBJECT:
            assertEquals(expectedTime(expected), actual);
            return;
        default:
            assertEquals(expected, actual, "valueType=" + valueType);
            return;
        }
    }

    private java.sql.Time toSqlTime(OffsetTime value) {
        var zdt = value.atDate(LocalDate.EPOCH).atZoneSameInstant(ZoneId.systemDefault());
        return java.sql.Time.valueOf(zdt.toLocalTime());
    }
}
