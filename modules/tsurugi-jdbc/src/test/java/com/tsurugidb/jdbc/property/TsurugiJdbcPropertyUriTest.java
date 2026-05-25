/*
 * Copyright 2025-2026 Project Tsurugi.
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

import java.net.URI;

import org.junit.jupiter.api.Test;

class TsurugiJdbcPropertyUriTest {

    private static final String STR_DEFAULT = "test:///default:52345";
    private static final String STR_VALUE = "test:value";
    private static final String STR_VALUE2 = "test:value2";

    private static final URI URI_DEFAULT = URI.create(STR_DEFAULT);
    private static final URI URI_VALUE = URI.create(STR_VALUE);
    private static final URI URI_VALUE2 = URI.create(STR_VALUE2);

    @Test
    void name() {
        TsurugiJdbcPropertyUri property = new TsurugiJdbcPropertyUri("foo").description("test");

        assertEquals("foo", property.name());
        assertEquals("test", property.description());
        assertNull(property.value());
        assertNull(property.getStringValue());
    }

    @Test
    void defaultValue() {
        TsurugiJdbcPropertyUri property = new TsurugiJdbcPropertyUri("foo").defaultValue(URI_DEFAULT);
        assertEquals(STR_DEFAULT, property.getStringDefaultValue());

        property.setValue(URI_VALUE);
        assertEquals(URI_VALUE, property.value());
        assertEquals(STR_DEFAULT, property.getStringDefaultValue());

        property.setValue(URI_VALUE2);
        assertEquals(URI_VALUE2, property.value());
        assertEquals(STR_DEFAULT, property.getStringDefaultValue());

        property.setStringValue(null);
        assertEquals(URI_DEFAULT, property.value());
        assertEquals(STR_DEFAULT, property.getStringDefaultValue());
    }

    @Test
    void changeEvent() {
        int[] count = { 0 };
        TsurugiJdbcPropertyUri property = new TsurugiJdbcPropertyUri("foo").changeEvent(v -> count[0]++);
        assertEquals(0, count[0]);

        property.setValue(URI_VALUE);
        assertEquals(1, count[0]);

        property.setStringValue(null);
        assertEquals(2, count[0]);
    }

    @Test
    void setValue() {
        var property = new TsurugiJdbcPropertyUri("foo");
        assertNull(property.value());
        assertNull(property.getStringDefaultValue());

        property.setValue(URI_VALUE);
        assertEquals(URI_VALUE, property.value());
        assertNull(property.getStringDefaultValue());

        property.setValue(URI_VALUE2);
        assertEquals(URI_VALUE2, property.value());
        assertNull(property.getStringDefaultValue());
    }

    @Test
    void setStringValue() {
        var property = new TsurugiJdbcPropertyUri("foo");
        assertNull(property.value());
        assertNull(property.getStringDefaultValue());

        property.setStringValue(STR_VALUE);
        assertEquals(URI_VALUE, property.value());
        assertNull(property.getStringDefaultValue());

        property.setStringValue(STR_VALUE2);
        assertEquals(URI_VALUE2, property.value());
        assertNull(property.getStringDefaultValue());
    }

    @Test
    void setFrom() {
        var fromDefaultStr = "test:///fromDefault:12345";
        var fromDefaultUri = URI.create(fromDefaultStr);

        int[] count = { 0 };
        int[] fromCount = { 0 };
        var property = new TsurugiJdbcPropertyUri("foo").description("zzz").defaultValue(URI_DEFAULT).changeEvent(v -> count[0]++);
        var fromProperty = new TsurugiJdbcPropertyUri("from").description("test").defaultValue(fromDefaultUri).changeEvent(v -> fromCount[0]++);
        fromProperty.setValue(URI_VALUE);
        assertEquals(0, count[0]);
        assertEquals(1, fromCount[0]);

        property.setFrom(fromProperty);

        assertEquals("foo", property.name());
        assertEquals("test", property.description());
        assertEquals(URI_VALUE, property.value());
        assertEquals(fromDefaultStr, property.getStringDefaultValue());
        assertEquals(1, count[0]);
        assertEquals(1, fromCount[0]);

        property.setValue(URI_VALUE2);
        assertEquals(2, count[0]);
        assertEquals(1, fromCount[0]);
    }

    @Test
    void isPresentValue() {
        var property = new TsurugiJdbcPropertyUri("foo");
        assertFalse(property.isPresentValue());

        property.setValue(URI_VALUE);
        assertTrue(property.isPresentValue());

        property.setStringValue(null);
        assertFalse(property.isPresentValue());
    }

    @Test
    void getStringValue() {
        var property = new TsurugiJdbcPropertyUri("foo");
        assertNull(property.value());
        assertEquals(null, property.getStringValue());

        property.setValue(URI_VALUE);
        assertEquals(STR_VALUE, property.getStringValue());
    }

    @Test
    void ifPresent() {
        {
            var property = new TsurugiJdbcPropertyUri("foo");
            property.ifPresent(v -> {
                fail();
            });

            property.setValue(URI_VALUE);
            URI[] value = { null };
            property.ifPresent(v -> {
                value[0] = v;
            });
            assertEquals(URI_VALUE, value[0]);

            property.setStringValue(null);
            property.ifPresent(v -> {
                fail();
            });
        }
        {
            var property = new TsurugiJdbcPropertyUri("foo").defaultValue(URI_DEFAULT);

            URI[] value = { null };
            property.ifPresent(v -> {
                value[0] = v;
            });
            assertEquals(URI_DEFAULT, value[0]);
        }
    }

    @Test
    void getStringDefaultValue() {
        var property = new TsurugiJdbcPropertyUri("foo").defaultValue(URI_DEFAULT);
        assertEquals(STR_DEFAULT, property.getStringDefaultValue());
    }

    @Test
    void getChoice() {
        var property = new TsurugiJdbcPropertyUri("foo");
        assertNull(property.getChoice());
    }
}
