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
package com.tsurugidb.jdbc.property;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

class TsurugiJdbcPropertyEnumTest {

    private enum TestEnum {
        FOO, BAR, ZZZ
    }

    @Test
    void name() {
        TsurugiJdbcPropertyEnum<TestEnum> property = new TsurugiJdbcPropertyEnum<>(TestEnum.class, "foo").description("test");

        assertEquals("foo", property.name());
        assertEquals("test", property.description());
        assertNull(property.value());
        assertNull(property.getStringValue());
    }

    @Test
    void defaultValue() {
        TsurugiJdbcPropertyEnum<TestEnum> property = new TsurugiJdbcPropertyEnum<>(TestEnum.class, "foo").defaultValue(TestEnum.BAR);
        assertEquals("BAR", property.getStringDefaultValue());

        assertEquals(TestEnum.BAR, property.value());
        assertEquals("BAR", property.getStringDefaultValue());

        property.setValue(TestEnum.FOO);
        assertEquals(TestEnum.FOO, property.value());
        assertEquals("BAR", property.getStringDefaultValue());

        property.setValue(TestEnum.ZZZ);
        assertEquals(TestEnum.ZZZ, property.value());
        assertEquals("BAR", property.getStringDefaultValue());

        property.setValue(null);
        assertEquals(TestEnum.BAR, property.value());
        assertEquals("BAR", property.getStringDefaultValue());
    }

    @Test
    void changeEvent() {
        int[] count = { 0 };
        TsurugiJdbcPropertyEnum<TestEnum> property = new TsurugiJdbcPropertyEnum<>(TestEnum.class, "foo").changeEvent(v -> count[0]++);
        assertEquals(0, count[0]);

        property.setValue(TestEnum.FOO);
        assertEquals(1, count[0]);

        property.setStringValue(null);
        assertEquals(2, count[0]);
    }

    @Test
    void setValue() {
        var property = new TsurugiJdbcPropertyEnum<>(TestEnum.class, "foo");
        assertNull(property.value());
        assertNull(property.getStringDefaultValue());

        property.setValue(TestEnum.FOO);
        assertEquals(TestEnum.FOO, property.value());
        assertNull(property.getStringDefaultValue());

        property.setValue(TestEnum.ZZZ);
        assertEquals(TestEnum.ZZZ, property.value());
        assertNull(property.getStringDefaultValue());
    }

    @Test
    void setStringValue() {
        var property = new TsurugiJdbcPropertyEnum<>(TestEnum.class, "foo");
        assertNull(property.value());
        assertNull(property.getStringDefaultValue());

        property.setStringValue("FOO");
        assertEquals(TestEnum.FOO, property.value());
        assertNull(property.getStringDefaultValue());

        property.setStringValue("ZZZ");
        assertEquals(TestEnum.ZZZ, property.value());
        assertNull(property.getStringDefaultValue());
    }

    @Test
    void setFrom() {
        int[] count = { 0 };
        int[] fromCount = { 0 };
        var property = new TsurugiJdbcPropertyEnum<>(TestEnum.class, "foo").description("zzz").defaultValue(TestEnum.FOO).changeEvent(v -> count[0]++);
        var fromProperty = new TsurugiJdbcPropertyEnum<>(TestEnum.class, "from").description("test").defaultValue(TestEnum.ZZZ).changeEvent(v -> fromCount[0]++);
        fromProperty.setValue(TestEnum.BAR);
        assertEquals(0, count[0]);
        assertEquals(1, fromCount[0]);

        property.setFrom(fromProperty);

        assertEquals("foo", property.name());
        assertEquals("test", property.description());
        assertEquals(TestEnum.BAR, property.value());
        assertEquals("ZZZ", property.getStringDefaultValue());
        assertEquals(1, count[0]);
        assertEquals(1, fromCount[0]);

        property.setValue(TestEnum.FOO);
        assertEquals(2, count[0]);
        assertEquals(1, fromCount[0]);
    }

    @Test
    void isPresentValue() {
        var property = new TsurugiJdbcPropertyEnum<>(TestEnum.class, "foo");
        assertFalse(property.isPresentValue());

        property.setValue(TestEnum.FOO);
        assertTrue(property.isPresentValue());

        property.setStringValue(null);
        assertFalse(property.isPresentValue());
    }

    @Test
    void getStringValue() {
        var property = new TsurugiJdbcPropertyEnum<>(TestEnum.class, "foo");
        assertNull(property.value());
        assertEquals(null, property.getStringValue());

        property.setValue(TestEnum.FOO);
        assertEquals("FOO", property.getStringValue());
    }

    @Test
    void ifPresent() {
        {
            var property = new TsurugiJdbcPropertyEnum<>(TestEnum.class, "foo");
            property.ifPresent(v -> {
                fail();
            });

            property.setValue(TestEnum.FOO);
            TestEnum[] value = { null };
            property.ifPresent(v -> {
                value[0] = v;
            });
            assertEquals(TestEnum.FOO, value[0]);

            property.setValue(null);
            property.ifPresent(v -> {
                fail();
            });
        }
        {
            var property = new TsurugiJdbcPropertyEnum<>(TestEnum.class, "foo").defaultValue(TestEnum.FOO);

            TestEnum[] value = { null };
            property.ifPresent(v -> {
                value[0] = v;
            });
            assertEquals(TestEnum.FOO, value[0]);
        }
    }

    @Test
    void getStringDefaultValue() {
        var property = new TsurugiJdbcPropertyEnum<>(TestEnum.class, "foo").defaultValue(TestEnum.BAR);
        assertEquals("BAR", property.getStringDefaultValue());
    }

    @Test
    void getChoice() {
        var property = new TsurugiJdbcPropertyEnum<>(TestEnum.class, "foo");
        assertArrayEquals(new String[] { "FOO", "BAR", "ZZZ" }, property.getChoice());
    }
}
