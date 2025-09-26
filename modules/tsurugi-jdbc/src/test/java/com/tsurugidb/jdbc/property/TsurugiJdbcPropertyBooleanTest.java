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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TsurugiJdbcPropertyBooleanTest {

    @Test
    void name() {
        TsurugiJdbcPropertyBoolean property = new TsurugiJdbcPropertyBoolean("foo").description("test");

        assertEquals("foo", property.name());
        assertEquals("test", property.description());
        assertFalse(property.value());
        assertEquals("false", property.getStringValue());
    }

    @Test
    void defaultValue() {
        TsurugiJdbcPropertyBoolean property = new TsurugiJdbcPropertyBoolean("foo").defaultValue(true);
        assertEquals("true", property.getStringDefaultValue());

        assertTrue(property.value());
        assertEquals("true", property.getStringDefaultValue());

        property.setValue(false);
        assertFalse(property.value());
        assertEquals("true", property.getStringDefaultValue());

        property.setValue(true);
        assertTrue(property.value());
        assertEquals("true", property.getStringDefaultValue());

        property.setStringValue(null);
        assertTrue(property.value());
        assertEquals("true", property.getStringDefaultValue());
    }

    @Test
    void changeEvent() {
        int[] count = { 0 };
        TsurugiJdbcPropertyBoolean property = new TsurugiJdbcPropertyBoolean("foo").changeEvent(v -> count[0]++);
        assertEquals(0, count[0]);

        property.setValue(true);
        assertEquals(1, count[0]);

        property.setStringValue(null);
        assertEquals(2, count[0]);
    }

    @Test
    void setValue() {
        var property = new TsurugiJdbcPropertyBoolean("foo");
        assertFalse(property.value());
        assertEquals("false", property.getStringDefaultValue());

        property.setValue(true);
        assertTrue(property.value());
        assertEquals("false", property.getStringDefaultValue());

        property.setValue(false);
        assertFalse(property.value());
        assertEquals("false", property.getStringDefaultValue());
    }

    @Test
    void setStringValue() {
        var property = new TsurugiJdbcPropertyBoolean("foo");
        assertFalse(property.value());
        assertEquals("false", property.getStringDefaultValue());

        property.setStringValue("true");
        assertTrue(property.value());
        assertEquals("false", property.getStringDefaultValue());

        property.setStringValue("false");
        assertFalse(property.value());
        assertEquals("false", property.getStringDefaultValue());
    }

    @Test
    void setFrom() {
        int[] count = { 0 };
        int[] fromCount = { 0 };
        var property = new TsurugiJdbcPropertyBoolean("foo").description("zzz").defaultValue(false).changeEvent(v -> count[0]++);
        var fromProperty = new TsurugiJdbcPropertyBoolean("from").description("test").defaultValue(true).changeEvent(v -> fromCount[0]++);
        fromProperty.setValue(true);
        assertEquals(0, count[0]);
        assertEquals(1, fromCount[0]);

        property.setFrom(fromProperty);

        assertEquals("foo", property.name());
        assertEquals("test", property.description());
        assertTrue(property.value());
        assertEquals("true", property.getStringDefaultValue());
        assertEquals(1, count[0]);
        assertEquals(1, fromCount[0]);

        property.setValue(false);
        assertEquals(2, count[0]);
        assertEquals(1, fromCount[0]);
    }

    @Test
    void isPresentValue() {
        var property = new TsurugiJdbcPropertyBoolean("foo");
        assertFalse(property.isPresentValue());

        property.setValue(false);
        assertTrue(property.isPresentValue());

        property.setStringValue(null);
        assertFalse(property.isPresentValue());
    }

    @Test
    void getStringValue() {
        var property = new TsurugiJdbcPropertyBoolean("foo");
        assertFalse(property.value());
        assertEquals("false", property.getStringValue());
    }

    @Test
    void getStringDefaultValue() {
        var property = new TsurugiJdbcPropertyBoolean("foo").defaultValue(true);
        assertEquals("true", property.getStringDefaultValue());
    }

    @Test
    void getChoice() {
        var property = new TsurugiJdbcPropertyBoolean("foo");
        assertArrayEquals(new String[] { "true", "false" }, property.getChoice());
    }
}
