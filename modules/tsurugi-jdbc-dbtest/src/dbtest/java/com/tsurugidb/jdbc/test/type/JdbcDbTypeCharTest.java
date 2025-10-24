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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;

/**
 * Tsurugi JDBC CHAR test.
 */
public class JdbcDbTypeCharTest extends JdbcDbTypeTester<String> {

    private static final int LENGTH = 32;

    @Override
    protected String sqlType() {
        return "char(" + LENGTH + ")";
    }

    @Override
    protected List<String> values() {
        var list = new ArrayList<String>();
        list.add("");
        list.add("abc");
        list.add("true");
        list.add("false");
        list.add(Byte.toString(Byte.MIN_VALUE));
        list.add(Byte.toString(Byte.MAX_VALUE));
        list.add(Short.toString(Short.MIN_VALUE));
        list.add(Short.toString(Short.MAX_VALUE));
        list.add(Integer.toString(Integer.MIN_VALUE));
        list.add(Integer.toString(Integer.MAX_VALUE));
        list.add(Long.toString(Long.MIN_VALUE));
        list.add(Long.toString(Long.MAX_VALUE));
        list.add("-9223372036854775809"); // Long.MIN_VALUE - 1
        list.add("9223372036854775808"); // Long.MAX_VALUE + 1
        list.add("123.4");
        list.add("12345e-1");
        list.add("NaN");
        list.add("2025-10-20");
        list.add("23:30:59");
        list.add("2025-10-20 23:30:59.123");
        list.add(null);
        return list;
    }

    @Override
    protected void modifyParameterMetaDataExcepted(ExpectedColumn expected) {
        expected.dataType(JDBCType.VARCHAR).typeBaseName("VARCHAR");
    }

    @Override
    protected TgBindVariable<String> bindVariable(String name) {
        return TgBindVariable.ofString(name);
    }

    @Override
    protected TgBindParameter bindParameter(String name, String value) {
        return TgBindParameter.of(name, value);
    }

    @Override
    protected String get(TsurugiResultEntity entity, String name) {
        return entity.getString(name);
    }

    @Override
    protected String get(ResultSet rs, int columnIndex) throws SQLException {
        String value = rs.getString(columnIndex);
        if (rs.wasNull()) {
            assertNull(value);
        }
        return value;
    }

    @Override
    protected void setParameter(PreparedStatement ps, int parameterIndex, String value) throws SQLException {
        if (value != null) {
            ps.setString(parameterIndex, value);
        } else {
            ps.setNull(parameterIndex, java.sql.Types.CHAR);
        }
    }

    @Override
    protected void assertException(String expected, ValueType valueType, SQLDataException e) {
        switch (valueType) {
        case BOOLEAN:
            if (expected.equalsIgnoreCase("true") || expected.equalsIgnoreCase("false")) {
                fail(e);
            } else {
                assertTrue(e.getMessage().contains("Cannot cast to boolean"));
            }
            return;
        case BYTE:
            try {
                Byte.parseByte(expected);
            } catch (NumberFormatException ignore) {
                assertTrue(e.getMessage().contains("convertToByte error"));
                return;
            }
            fail(e);
            return;
        case SHORT:
            try {
                Short.parseShort(expected);
            } catch (NumberFormatException ignore) {
                assertTrue(e.getMessage().contains("convertToShort error"));
                return;
            }
            fail(e);
            return;
        case INT:
            try {
                Integer.parseInt(expected);
            } catch (NumberFormatException ignore) {
                assertTrue(e.getMessage().contains("convertToInt error"));
                return;
            }
            fail(e);
            return;
        case LONG:
            try {
                Long.parseLong(expected);
            } catch (NumberFormatException ignore) {
                assertTrue(e.getMessage().contains("convertToLong error"));
                return;
            }
            fail(e);
            return;
        case FLOAT:
            try {
                Float.parseFloat(expected);
            } catch (NumberFormatException ignore) {
                assertTrue(e.getMessage().contains("convertToFloat error"));
                return;
            }
            fail(e);
            return;
        case DOUBLE:
            try {
                Double.parseDouble(expected);
            } catch (NumberFormatException ignore) {
                assertTrue(e.getMessage().contains("convertToDouble error"));
                return;
            }
            fail(e);
            return;
        case DECIMAL:
            try {
                new BigDecimal(expected);
            } catch (NumberFormatException ignore) {
                assertTrue(e.getMessage().contains("convertToDecimal error"));
                return;
            }
            fail(e);
            return;
        case DATE:
            try {
                LocalDate.parse(expected);
            } catch (DateTimeParseException ignore) {
                assertTrue(e.getMessage().contains("convertToDate error"));
                return;
            }
            fail(e);
            return;
        case TIME:
            try {
                LocalTime.parse(expected);
            } catch (DateTimeParseException ignore) {
                assertTrue(e.getMessage().contains("convertToTime error"));
                return;
            }
            fail(e);
            return;
        case TIMESTAMP:
            try {
                LocalDateTime.parse(expected);
            } catch (DateTimeParseException ignore) {
                assertTrue(e.getMessage().contains("convertToTimestamp error"));
                return;
            }
            fail(e);
            return;
        case LOCAL_DATE:
            try {
                LocalDate.parse(expected);
            } catch (DateTimeParseException ignore) {
                assertTrue(e.getMessage().contains("convertToLocalDate error"));
                return;
            }
            fail(e);
            return;
        case LOCAL_TIME:
            try {
                LocalTime.parse(expected);
            } catch (DateTimeParseException ignore) {
                assertTrue(e.getMessage().contains("convertToLocalTime error"));
                return;
            }
            fail(e);
            return;
        case LOCAL_DATE_TIME:
            try {
                LocalDateTime.parse(expected);
            } catch (DateTimeParseException ignore) {
                assertTrue(e.getMessage().contains("convertToLocalDateTime error"));
                return;
            }
            fail(e);
            return;
        case OFFSET_TIME:
            try {
                OffsetTime.parse(expected);
            } catch (DateTimeParseException ignore) {
                assertTrue(e.getMessage().contains("convertToOffsetTime error"));
                return;
            }
            fail(e);
            return;
        case OFFSET_DATE_TIME:
            try {
                LocalDateTime.parse(expected);
            } catch (DateTimeParseException ignore) {
                assertTrue(e.getMessage().contains("convertToOffsetDateTime error"));
                return;
            }
            fail(e);
            return;
        case ZONED_DATE_TIME:
            try {
                ZonedDateTime.parse(expected);
            } catch (DateTimeParseException ignore) {
                assertTrue(e.getMessage().contains("convertToZonedDateTime error"));
                return;
            }
            fail(e);
            return;
        default:
            assertTrue(e.getMessage().contains("unsupported type"), () -> "valueType=" + valueType + ", " + e.getMessage());
            return;
        }
    }

    @Override
    protected void assertValue(String expected, ValueType valueType, Object actual) {
        switch (valueType) {
        case OBJECT:
        case STRING:
            assertEquals(expectedString(expected), actual);
            return;
        case BOOLEAN:
            assertEquals(Boolean.valueOf(expected), actual);
            return;
        case BYTE:
            assertEquals(Byte.parseByte(expected), actual);
            return;
        case SHORT:
            assertEquals(Short.parseShort(expected), actual);
            return;
        case INT:
            assertEquals(Integer.parseInt(expected), actual);
            return;
        case LONG:
            assertEquals(Long.parseLong(expected), actual);
            return;
        case FLOAT:
            assertEquals(Float.parseFloat(expected), actual);
            return;
        case DOUBLE:
            assertEquals(Double.parseDouble(expected), actual);
            return;
        case DECIMAL:
            assertEquals(new BigDecimal(expected), actual);
            return;
        case DATE:
            assertEquals(java.sql.Date.valueOf(localDate(expected)), actual);
            return;
        case TIME:
            assertEquals(java.sql.Time.valueOf(localTime(expected)), actual);
            return;
        case TIMESTAMP:
            assertEquals(java.sql.Timestamp.valueOf(localDateTime(expected)), actual);
            return;
        case LOCAL_DATE:
            assertEquals(localDate(expected), actual);
            return;
        case LOCAL_TIME:
            assertEquals(localTime(expected), actual);
            return;
        case LOCAL_DATE_TIME:
            assertEquals(localDateTime(expected), actual);
            return;
        case OFFSET_TIME: {
            var offset = ZoneId.systemDefault().getRules().getOffset(Instant.now());
            assertEquals(OffsetTime.of(localTime(expected), offset), actual);
        }
            return;
        case OFFSET_DATE_TIME: {
            var offset = ZoneId.systemDefault().getRules().getOffset(Instant.now());
            assertEquals(OffsetDateTime.of(localDateTime(expected), offset), actual);
        }
            return;
        case ZONED_DATE_TIME:
            assertEquals(ZonedDateTime.of(localDateTime(expected), ZoneId.systemDefault()), actual);
            return;
        case ASCII_STREAM:
            try {
                var bytes = ((InputStream) actual).readAllBytes();
                String a = new String(bytes, StandardCharsets.UTF_8);
                assertEquals(expectedString(expected), a);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            return;
        case UNICODE_STREAM:
            try {
                var bytes = ((InputStream) actual).readAllBytes();
                String a = new String(bytes, StandardCharsets.UTF_16);
                assertEquals(expectedString(expected), a);
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
            return;
        case CHARACTER_STREAM:
            String a = readAllString((Reader) actual);
            assertEquals(expectedString(expected), a);
            return;
        default:
            assertEquals(expectedString(expected), actual, "valueType=" + valueType);
            return;
        }
    }

    private static LocalDate localDate(String value) {
        try {
            return LocalDate.parse(value);
        } catch (DateTimeParseException e) {
            return LocalDateTime.parse(value.replace(' ', 'T')).toLocalDate();
        }
    }

    private static LocalTime localTime(String value) {
        try {
            return LocalTime.parse(value);
        } catch (DateTimeParseException e) {
            try {
                return LocalDateTime.parse(value.replace(' ', 'T')).toLocalTime();
            } catch (DateTimeParseException e1) {
                if (LocalDate.parse(value) != null) {
                    return LocalTime.MIN;
                }
                throw e;
            }
        }
    }

    private static LocalDateTime localDateTime(String value) {
        try {
            return LocalDateTime.parse(value.replace(' ', 'T'));
        } catch (DateTimeParseException e) {
            try {
                return LocalDate.parse(value).atStartOfDay();
            } catch (DateTimeParseException e1) {
                return LocalDateTime.of(LocalDate.EPOCH, LocalTime.parse(value));
            }
        }
    }

    private String readAllString(Reader reader) {
        var sb = new StringBuffer();
        var buffer = new char[64];
        try (reader) {
            for (;;) {
                int length = reader.read(buffer);
                if (length < 0) {
                    break;
                }
                sb.append(buffer, 0, length);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
        return sb.toString();
    }

    @Override
    protected void assertValueList(List<String> expected, List<String> actual) {
        try {
            assertEquals(expected.size(), actual.size());
            for (int i = 0; i < actual.size(); i++) {
                String e = expectedString(expected.get(i));
                assertEquals(e, actual.get(i));
            }
        } catch (Throwable e) {
            LOG.error("{}\nexpected={}\nactual=  {}", e.getMessage(), expected, actual);
            throw e;
        }
    }

    private static final String FILL = " ".repeat(LENGTH);

    private static String expectedString(String value) {
        if (value == null) {
            return null;
        }
        return (value + FILL).substring(0, LENGTH);
    }
}
