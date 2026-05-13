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
package com.tsurugidb.jdbc.statement.type;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class TsurugiJdbcClobTest {

    @Test
    void empty() throws Exception {
        var target = new TsurugiJdbcClob(32);
        assertEquals(0, target.length());

        assertEquals(0, target.getSubString(1, 32).length());

        try (var reader = target.getCharacterStream()) {
            assertEquals(-1, reader.read());
        }
        try (var reader = target.getCharacterStream(1, 32)) {
            assertEquals(-1, reader.read());
        }
        try (var is = target.getAsciiStream()) {
            assertEquals(-1, is.read());
        }

        assertEquals(1, target.position("", 1));
        assertEquals(-1, target.position("", 2));
        assertEquals(-1, target.position("a", 1));

        {
            var empty = new TsurugiJdbcClob(32);
            assertEquals(1, target.position(empty, 1));
        }
    }

    @Test
    void empty_setString() throws Exception {
        {
            var target = new TsurugiJdbcClob(32);
            target.setString(1, "abc");
            assertEquals("abc", target.getSubString(1, 32));
        }
        {
            var target = new TsurugiJdbcClob(32);
            target.setString(2, "abcde", 1, 3);
            assertEquals("\0bcd", target.getSubString(1, 32));
        }
    }

    @Test
    void setString() throws Exception {
        var target = new TsurugiJdbcClob(2);
        target.setString(1, "abc");
        assertEquals(3, target.length());
        assertEquals("abc", target.getSubString(1, 32));

        target.setString(6, "fg");
        assertEquals(7, target.length());
        assertEquals("abc\0\0fg", target.getSubString(1, 32));

        target.setString(4, "CDEFG", 1, 3);
        assertEquals(7, target.length());
        assertEquals("abcDEFg", target.getSubString(1, 32));

        target.setString(6, "wxyz", 1, 3);
        assertEquals(8, target.length());
        assertEquals("abcDExyz", target.getSubString(1, 32));
    }

    @Test
    void getSubString() throws Exception {
        var target = new TsurugiJdbcClob(32);
        target.setString(1, "abcde");

        var expectedList = "abcde";

        int length = 1;
        for (int i = 1; i <= 5; i++) {
            String expected = toSubstring(expectedList, i - 1, length);
            assertEquals(expected, target.getSubString(i, length));
        }

        length = 2;
        for (int i = 1; i <= 5; i++) {
            String expected = toSubstring(expectedList, i - 1, length);
            assertEquals(expected, target.getSubString(i, length));
        }

        length = 3;
        for (int i = 1; i <= 5; i++) {
            String expected = toSubstring(expectedList, i - 1, length);
            assertEquals(expected, target.getSubString(i, length));
        }

        length = 5;
        for (int i = 1; i <= 5; i++) {
            String expected = toSubstring(expectedList, i - 1, length);
            assertEquals(expected, target.getSubString(i, length));
        }

        length = 6;
        for (int i = 1; i <= 5; i++) {
            String expected = toSubstring(expectedList, i - 1, length);
            assertEquals(expected, target.getSubString(i, length));
        }
    }

    private static String toSubstring(String list, int beginIndex, int size) {
        int endIndex = Math.min(beginIndex + size, list.length());
        return list.substring(beginIndex, endIndex);
    }

    @Test
    void getCharacterStream() throws Exception {
        var target = new TsurugiJdbcClob(32);
        target.setString(1, "abcde");

        var expectedList = "abcde";

        int length = 1;
        for (int i = 1; i <= 5; i++) {
            String expected = toSubstring(expectedList, i - 1, length);
            try (var reader = new BufferedReader(target.getCharacterStream(i, length))) {
                String actual = reader.readLine();
                assertEquals(expected, actual);
            }
        }

        length = 2;
        for (int i = 1; i <= 5; i++) {
            String expected = toSubstring(expectedList, i - 1, length);
            try (var reader = new BufferedReader(target.getCharacterStream(i, length))) {
                String actual = reader.readLine();
                assertEquals(expected, actual);
            }
        }

        length = 3;
        for (int i = 1; i <= 5; i++) {
            String expected = toSubstring(expectedList, i - 1, length);
            try (var reader = new BufferedReader(target.getCharacterStream(i, length))) {
                String actual = reader.readLine();
                assertEquals(expected, actual);
            }
        }

        length = 5;
        for (int i = 1; i <= 5; i++) {
            String expected = toSubstring(expectedList, i - 1, length);
            try (var reader = new BufferedReader(target.getCharacterStream(i, length))) {
                String actual = reader.readLine();
                assertEquals(expected, actual);
            }
        }

        length = 6;
        for (int i = 1; i <= 5; i++) {
            String expected = toSubstring(expectedList, i - 1, length);
            try (var reader = new BufferedReader(target.getCharacterStream(i, length))) {
                String actual = reader.readLine();
                assertEquals(expected, actual);
            }
        }
    }

    @Test
    void getAsciiStream() throws Exception {
        var target = new TsurugiJdbcClob(32);
        target.setString(1, "abcde");

        try (var is = target.getAsciiStream()) {
            String actual = new String(is.readAllBytes());
            assertEquals("abcde", actual);
        }
    }

    @Test
    void position() throws Exception {
        var target = new TsurugiJdbcClob(32);
        target.setString(1, "abacdabe");

        for (int i = 1; i <= 8; i++) {
            assertEquals(i, target.position("", i));
        }

        assertEquals(1, target.position("a", 1));
        assertEquals(3, target.position("a", 2));
        assertEquals(3, target.position("a", 3));
        assertEquals(6, target.position("a", 4));
        assertEquals(6, target.position("a", 5));
        assertEquals(6, target.position("a", 6));
        assertEquals(-1, target.position("a", 7));

        assertEquals(1, target.position("ab", 1));
        assertEquals(6, target.position("ab", 2));

        assertEquals(3, target.position("ac", 1));

        {
            var pattern = new TsurugiJdbcClob(32);
            pattern.setString(1, "ab");
            assertEquals(1, target.position(pattern, 1));
            assertEquals(6, target.position(pattern, 2));
        }
    }

    @Test
    void setCharacterStream() throws Exception {
        var target = new TsurugiJdbcClob(32);
        try (var writer = target.setCharacterStream(1)) {
            writer.write('A');
            writer.write("abc");
            writer.write("12345", 1, 3);
            writer.write("defgh".toCharArray(), 1, 3);
        }
        assertEquals("Aabc234efg", target.getSubString(1, 32));
    }

    @Test
    void setAsciiStream() throws Exception {
        var target = new TsurugiJdbcClob(32);
        try (var is = target.setAsciiStream(1)) {
            is.write((int) 'A');
            is.write("abc".getBytes(StandardCharsets.UTF_8));
        }
        assertEquals("Aabc", target.getSubString(1, 32));
    }

    @Test
    void truncate() throws Exception {
        {
            var target = new TsurugiJdbcClob(32);
            target.truncate(0);
            assertEquals(0, target.length());
        }
        {
            var target = new TsurugiJdbcClob(32);
            target.setString(1, "abc");

            target.truncate(0);
            assertEquals(0, target.length());
        }
    }
}
