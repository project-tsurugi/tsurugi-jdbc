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

import java.util.List;

import org.junit.jupiter.api.Test;

class TsurugiJdbcPropertyStringListTest {

    @Test
    void name() {
        TsurugiJdbcPropertyStringList property = new TsurugiJdbcPropertyStringList("foo").description("test");

        assertEquals("foo", property.name());
        assertEquals("test", property.description());
        assertNull(property.value());
        assertNull(property.getStringValue());
    }

    @Test
    void defaultValue() {
        TsurugiJdbcPropertyStringList property = new TsurugiJdbcPropertyStringList("foo").defaultValue(List.of("zzz"));
        assertEquals("zzz", property.getStringDefaultValue());

        property.setValue(List.of("Foo"));
        assertEquals(List.of("Foo"), property.value());
        assertEquals("zzz", property.getStringDefaultValue());

        property.setValue(List.of("bar1", "bar2"));
        assertEquals(List.of("bar1", "bar2"), property.value());
        assertEquals("zzz", property.getStringDefaultValue());

        property.setStringValue(null);
        assertEquals(List.of("zzz"), property.value());
        assertEquals("zzz", property.getStringDefaultValue());
    }

    @Test
    void changeEvent() {
        int[] count = { 0 };
        TsurugiJdbcPropertyStringList property = new TsurugiJdbcPropertyStringList("foo").changeEvent(v -> count[0]++);
        assertEquals(0, count[0]);

        property.setValue(List.of("abc"));
        assertEquals(1, count[0]);

        property.setStringValue(null);
        assertEquals(2, count[0]);
    }

    @Test
    void setValue() {
        var property = new TsurugiJdbcPropertyStringList("foo");
        assertNull(property.value());
        assertNull(property.getStringDefaultValue());

        property.setValue(List.of("abc"));
        assertEquals(List.of("abc"), property.value());
        assertNull(property.getStringDefaultValue());

        property.setValue(List.of("def"));
        assertEquals(List.of("def"), property.value());
        assertNull(property.getStringDefaultValue());
    }

    @Test
    void setStringValue() {
        var property = new TsurugiJdbcPropertyStringList("foo");
        assertNull(property.value());
        assertNull(property.getStringDefaultValue());

        property.setStringValue("abc");
        assertEquals(List.of("abc"), property.value());
        assertNull(property.getStringDefaultValue());

        property.setStringValue("def, ghi");
        assertEquals(List.of("def", "ghi"), property.value());
        assertNull(property.getStringDefaultValue());
    }

    @Test
    void setFrom() {
        int[] count = { 0 };
        int[] fromCount = { 0 };
        var property = new TsurugiJdbcPropertyStringList("foo").description("zzz").defaultValue(List.of("bar")).changeEvent(v -> count[0]++);
        var fromProperty = new TsurugiJdbcPropertyStringList("from").description("test").defaultValue(List.of("Zzz")).changeEvent(v -> fromCount[0]++);
        fromProperty.setValue(List.of("abc"));
        assertEquals(0, count[0]);
        assertEquals(1, fromCount[0]);

        property.setFrom(fromProperty);

        assertEquals("foo", property.name());
        assertEquals("test", property.description());
        assertEquals(List.of("abc"), property.value());
        assertEquals("Zzz", property.getStringDefaultValue());
        assertEquals(1, count[0]);
        assertEquals(1, fromCount[0]);

        property.setValue(List.of("def"));
        assertEquals(2, count[0]);
        assertEquals(1, fromCount[0]);
    }

    @Test
    void isPresentValue() {
        var property = new TsurugiJdbcPropertyStringList("foo");
        assertFalse(property.isPresentValue());

        property.setValue(List.of("abc"));
        assertTrue(property.isPresentValue());

        property.setStringValue(null);
        assertFalse(property.isPresentValue());
    }

    @Test
    void getStringValue() {
        var property = new TsurugiJdbcPropertyStringList("foo");
        assertNull(property.value());
        assertEquals(null, property.getStringValue());

        property.setValue(List.of("abc", "def"));
        assertEquals("abc, def", property.getStringValue());
    }

    @Test
    void ifPresent() {
        {
            var property = new TsurugiJdbcPropertyStringList("foo");
            property.ifPresent(v -> {
                fail();
            });

            property.setValue(List.of("abc"));
            Object[] value = { null };
            property.ifPresent(v -> {
                value[0] = v;
            });
            assertEquals(List.of("abc"), value[0]);

            property.setStringValue(null);
            property.ifPresent(v -> {
                fail();
            });
        }
        {
            var property = new TsurugiJdbcPropertyStringList("foo").defaultValue(List.of("abc"));

            Object[] value = { null };
            property.ifPresent(v -> {
                value[0] = v;
            });
            assertEquals(List.of("abc"), value[0]);
        }
    }

    @Test
    void getStringDefaultValue() {
        var property = new TsurugiJdbcPropertyStringList("foo").defaultValue(List.of("zzz1", "zzz2"));
        assertEquals("zzz1, zzz2", property.getStringDefaultValue());
    }

    @Test
    void getChoice() {
        var property = new TsurugiJdbcPropertyStringList("foo");
        assertNull(property.getChoice());
    }
}
