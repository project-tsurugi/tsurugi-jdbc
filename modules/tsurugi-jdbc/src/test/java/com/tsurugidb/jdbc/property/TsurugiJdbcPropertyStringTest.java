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

import org.junit.jupiter.api.Test;

class TsurugiJdbcPropertyStringTest {

    @Test
    void name() {
        TsurugiJdbcPropertyString property = new TsurugiJdbcPropertyString("foo").description("test");

        assertEquals("foo", property.name());
        assertEquals("test", property.description());
        assertNull(property.value());
        assertNull(property.getStringValue());
    }

    @Test
    void defaultValue() {
        TsurugiJdbcPropertyString property = new TsurugiJdbcPropertyString("foo").defaultValue("zzz");
        assertEquals("zzz", property.getStringDefaultValue());

        property.setValue("Foo");
        assertEquals("Foo", property.value());
        assertEquals("zzz", property.getStringDefaultValue());

        property.setValue("bar");
        assertEquals("bar", property.value());
        assertEquals("zzz", property.getStringDefaultValue());

        property.setStringValue(null);
        assertEquals("zzz", property.value());
        assertEquals("zzz", property.getStringDefaultValue());
    }

    @Test
    void changeEvent() {
        int[] count = { 0 };
        TsurugiJdbcPropertyString property = new TsurugiJdbcPropertyString("foo").changeEvent(v -> count[0]++);
        assertEquals(0, count[0]);

        property.setValue("abc");
        assertEquals(1, count[0]);

        property.setStringValue(null);
        assertEquals(2, count[0]);
    }

    @Test
    void setValue() {
        var property = new TsurugiJdbcPropertyString("foo");
        assertNull(property.value());
        assertNull(property.getStringDefaultValue());

        property.setValue("abc");
        assertEquals("abc", property.value());
        assertNull(property.getStringDefaultValue());

        property.setValue("def");
        assertEquals("def", property.value());
        assertNull(property.getStringDefaultValue());
    }

    @Test
    void setStringValue() {
        var property = new TsurugiJdbcPropertyString("foo");
        assertNull(property.value());
        assertNull(property.getStringDefaultValue());

        property.setStringValue("abc");
        assertEquals("abc", property.value());
        assertNull(property.getStringDefaultValue());

        property.setStringValue("def");
        assertEquals("def", property.value());
        assertNull(property.getStringDefaultValue());
    }

    @Test
    void setFrom() {
        int[] count = { 0 };
        int[] fromCount = { 0 };
        var property = new TsurugiJdbcPropertyString("foo").description("zzz").defaultValue("bar").changeEvent(v -> count[0]++);
        var fromProperty = new TsurugiJdbcPropertyString("from").description("test").defaultValue("Zzz").changeEvent(v -> fromCount[0]++);
        fromProperty.setValue("abc");
        assertEquals(0, count[0]);
        assertEquals(1, fromCount[0]);

        property.setFrom(fromProperty);

        assertEquals("foo", property.name());
        assertEquals("test", property.description());
        assertEquals("abc", property.value());
        assertEquals("Zzz", property.getStringDefaultValue());
        assertEquals(1, count[0]);
        assertEquals(1, fromCount[0]);

        property.setValue("def");
        assertEquals(2, count[0]);
        assertEquals(1, fromCount[0]);
    }

    @Test
    void isPresentValue() {
        var property = new TsurugiJdbcPropertyString("foo");
        assertFalse(property.isPresentValue());

        property.setValue("abc");
        assertTrue(property.isPresentValue());

        property.setStringValue(null);
        assertFalse(property.isPresentValue());
    }

    @Test
    void getStringValue() {
        var property = new TsurugiJdbcPropertyString("foo");
        assertNull(property.value());
        assertEquals(null, property.getStringValue());

        property.setValue("abc");
        assertEquals("abc", property.getStringValue());
    }

    @Test
    void ifPresent() {
        {
            var property = new TsurugiJdbcPropertyString("foo");
            property.ifPresent(v -> {
                fail();
            });

            property.setValue("abc");
            String[] value = { null };
            property.ifPresent(v -> {
                value[0] = v;
            });
            assertEquals("abc", value[0]);

            property.setStringValue(null);
            property.ifPresent(v -> {
                fail();
            });
        }
        {
            var property = new TsurugiJdbcPropertyString("foo").defaultValue("abc");

            String[] value = { null };
            property.ifPresent(v -> {
                value[0] = v;
            });
            assertEquals("abc", value[0]);
        }
    }

    @Test
    void getStringDefaultValue() {
        var property = new TsurugiJdbcPropertyString("foo").defaultValue("zzz");
        assertEquals("zzz", property.getStringDefaultValue());
    }

    @Test
    void getChoice() {
        var property = new TsurugiJdbcPropertyString("foo");
        assertNull(property.getChoice());
    }
}
