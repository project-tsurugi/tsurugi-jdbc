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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

class TsurugiJdbcPropertyIntTest {

    @Test
    void name() {
        TsurugiJdbcPropertyInt property = new TsurugiJdbcPropertyInt("foo").description("test");

        assertEquals("foo", property.name());
        assertEquals("test", property.description());
        assertEquals(OptionalInt.empty(), property.value());
        assertNull(property.getStringValue());
    }

    @Test
    void defaultValue() {
        TsurugiJdbcPropertyInt property = new TsurugiJdbcPropertyInt("foo").defaultValue(123);
        assertEquals("123", property.getStringDefaultValue());

        assertEquals(OptionalInt.of(123), property.value());
        assertEquals("123", property.getStringDefaultValue());

        property.setValue(456);
        assertEquals(OptionalInt.of(456), property.value());
        assertEquals("123", property.getStringDefaultValue());

        property.setValue(789);
        assertEquals(OptionalInt.of(789), property.value());
        assertEquals("123", property.getStringDefaultValue());

        property.setStringValue(null);
        assertEquals(OptionalInt.of(123), property.value());
        assertEquals("123", property.getStringDefaultValue());
    }

    @Test
    void changeEvent() {
        int[] count = { 0 };
        TsurugiJdbcPropertyInt property = new TsurugiJdbcPropertyInt("foo").changeEvent(v -> count[0]++);
        assertEquals(0, count[0]);

        property.setValue(123);
        assertEquals(1, count[0]);

        property.setStringValue(null);
        assertEquals(2, count[0]);
    }

    @Test
    void setValue() {
        var property = new TsurugiJdbcPropertyInt("foo");
        assertEquals(OptionalInt.empty(), property.value());
        assertNull(property.getStringDefaultValue());

        property.setValue(123);
        assertEquals(OptionalInt.of(123), property.value());
        assertNull(property.getStringDefaultValue());

        property.setValue(456);
        assertEquals(OptionalInt.of(456), property.value());
        assertNull(property.getStringDefaultValue());
    }

    @Test
    void setStringValue() {
        var property = new TsurugiJdbcPropertyInt("foo");
        assertEquals(OptionalInt.empty(), property.value());
        assertNull(property.getStringDefaultValue());

        property.setStringValue("123");
        assertEquals(OptionalInt.of(123), property.value());
        assertNull(property.getStringDefaultValue());

        property.setStringValue("456");
        assertEquals(OptionalInt.of(456), property.value());
        assertNull(property.getStringDefaultValue());
    }

    @Test
    void setFrom() {
        int[] count = { 0 };
        int[] fromCount = { 0 };
        var property = new TsurugiJdbcPropertyInt("foo").description("zzz").defaultValue(123).changeEvent(v -> count[0]++);
        var fromProperty = new TsurugiJdbcPropertyInt("from").description("test").defaultValue(456).changeEvent(v -> fromCount[0]++);
        fromProperty.setValue(789);
        assertEquals(0, count[0]);
        assertEquals(1, fromCount[0]);

        property.setFrom(fromProperty);

        assertEquals("foo", property.name());
        assertEquals("test", property.description());
        assertEquals(OptionalInt.of(789), property.value());
        assertEquals("456", property.getStringDefaultValue());
        assertEquals(1, count[0]);
        assertEquals(1, fromCount[0]);

        property.setValue(999);
        assertEquals(2, count[0]);
        assertEquals(1, fromCount[0]);
    }

    @Test
    void isPresentValue() {
        var property = new TsurugiJdbcPropertyInt("foo");
        assertFalse(property.isPresentValue());

        property.setValue(123);
        assertTrue(property.isPresentValue());

        property.setStringValue(null);
        assertFalse(property.isPresentValue());
    }

    @Test
    void getStringValue() {
        var property = new TsurugiJdbcPropertyInt("foo");
        assertEquals(OptionalInt.empty(), property.value());
        assertEquals(null, property.getStringValue());

        property.setValue(123);
        assertEquals("123", property.getStringValue());
    }

    @Test
    void ifPresent() {
        {
            var property = new TsurugiJdbcPropertyInt("foo");
            property.ifPresent(v -> {
                fail();
            });

            property.setValue(123);
            int[] value = { -1 };
            property.ifPresent(v -> {
                value[0] = v;
            });
            assertEquals(123, value[0]);

            property.setStringValue(null);
            property.ifPresent(v -> {
                fail();
            });
        }
        {
            var property = new TsurugiJdbcPropertyInt("foo").defaultValue(456);

            int[] value = { -1 };
            property.ifPresent(v -> {
                value[0] = v;
            });
            assertEquals(456, value[0]);
        }
    }

    @Test
    void getStringDefaultValue() {
        var property = new TsurugiJdbcPropertyInt("foo").defaultValue(456);
        assertEquals("456", property.getStringDefaultValue());
    }

    @Test
    void getChoice() {
        var property = new TsurugiJdbcPropertyInt("foo");
        assertNull(property.getChoice());
    }
}
