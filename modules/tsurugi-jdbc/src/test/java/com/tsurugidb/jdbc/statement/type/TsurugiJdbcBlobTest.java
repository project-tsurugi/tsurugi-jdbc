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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

class TsurugiJdbcBlobTest {

    @Test
    void empty() throws Exception {
        var target = new TsurugiJdbcBlob(32);
        assertEquals(0, target.length());

        assertEquals(0, target.getBytes(1, 32).length);

        try (var is = target.getBinaryStream()) {
            assertEquals(-1, is.read());
        }
        try (var is = target.getBinaryStream(1, 32)) {
            assertEquals(-1, is.read());
        }

        assertEquals(1, target.position(new byte[0], 1));
        assertEquals(-1, target.position(new byte[0], 2));
        assertEquals(-1, target.position(new byte[] { 1 }, 1));

        {
            var empty = new TsurugiJdbcBlob(0);
            assertEquals(1, target.position(empty, 1));
        }
    }

    @Test
    void empty_setBytes() throws Exception {
        {
            var target = new TsurugiJdbcBlob(32);
            target.setBytes(1, new byte[] { 1, 2, 3 });
            assertArrayEquals(new byte[] { 1, 2, 3 }, target.getBytes(1, 32));
        }
        {
            var target = new TsurugiJdbcBlob(32);
            target.setBytes(2, new byte[] { 1, 2, 3, 4, 5 }, 1, 3);
            assertArrayEquals(new byte[] { 0, 2, 3, 4 }, target.getBytes(1, 32));
        }
    }

    @Test
    void setBytes() throws Exception {
        var target = new TsurugiJdbcBlob(2);
        target.setBytes(1, new byte[] { 1, 2, 3 });
        assertEquals(3, target.length());
        assertArrayEquals(new byte[] { 1, 2, 3 }, target.getBytes(1, 32));

        target.setBytes(6, new byte[] { 66, 77 });
        assertEquals(7, target.length());
        assertArrayEquals(new byte[] { 1, 2, 3, 0, 0, 66, 77 }, target.getBytes(1, 32));

        target.setBytes(4, new byte[] { 13, 14, 15, 16, 17 }, 1, 3);
        assertEquals(7, target.length());
        assertArrayEquals(new byte[] { 1, 2, 3, 14, 15, 16, 77 }, target.getBytes(1, 32));

        target.setBytes(6, new byte[] { 25, 26, 27, 28 }, 1, 3);
        assertEquals(8, target.length());
        assertArrayEquals(new byte[] { 1, 2, 3, 14, 15, 26, 27, 28 }, target.getBytes(1, 32));
    }

    @Test
    void getBytes() throws Exception {
        var target = new TsurugiJdbcBlob(32);
        target.setBytes(1, new byte[] { 1, 2, 3, 4, 5 });

        var expectedList = List.of((byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5);

        int length = 1;
        for (int i = 1; i <= 5; i++) {
            byte[] expected = toByteArray(expectedList, i - 1, length);
            assertArrayEquals(expected, target.getBytes(i, length));
        }

        length = 2;
        for (int i = 1; i <= 5; i++) {
            byte[] expected = toByteArray(expectedList, i - 1, length);
            assertArrayEquals(expected, target.getBytes(i, length));
        }

        length = 3;
        for (int i = 1; i <= 5; i++) {
            byte[] expected = toByteArray(expectedList, i - 1, length);
            assertArrayEquals(expected, target.getBytes(i, length));
        }

        length = 5;
        for (int i = 1; i <= 5; i++) {
            byte[] expected = toByteArray(expectedList, i - 1, length);
            assertArrayEquals(expected, target.getBytes(i, length));
        }

        length = 6;
        for (int i = 1; i <= 5; i++) {
            byte[] expected = toByteArray(expectedList, i - 1, length);
            assertArrayEquals(expected, target.getBytes(i, length));
        }
    }

    private static byte[] toByteArray(List<Byte> list, int index, int size) {
        int lastIndex = Math.min(index + size, list.size());
        var subList = list.subList(index, lastIndex);

        var array = new byte[subList.size()];
        for (int i = 0; i < subList.size(); i++) {
            array[i] = subList.get(i);
        }
        return array;
    }

    @Test
    void getBinaryStream() throws Exception {
        var target = new TsurugiJdbcBlob(32);
        target.setBytes(1, new byte[] { 1, 2, 3, 4, 5 });

        var expectedList = List.of((byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5);

        int length = 1;
        for (int i = 1; i <= 5; i++) {
            byte[] expected = toByteArray(expectedList, i - 1, length);
            try (var is = target.getBinaryStream(i, length)) {
                byte[] actual = is.readAllBytes();
                assertArrayEquals(expected, actual);
            }
        }

        length = 2;
        for (int i = 1; i <= 5; i++) {
            byte[] expected = toByteArray(expectedList, i - 1, length);
            try (var is = target.getBinaryStream(i, length)) {
                byte[] actual = is.readAllBytes();
                assertArrayEquals(expected, actual);
            }
        }

        length = 3;
        for (int i = 1; i <= 5; i++) {
            byte[] expected = toByteArray(expectedList, i - 1, length);
            try (var is = target.getBinaryStream(i, length)) {
                byte[] actual = is.readAllBytes();
                assertArrayEquals(expected, actual);
            }
        }

        length = 5;
        for (int i = 1; i <= 5; i++) {
            byte[] expected = toByteArray(expectedList, i - 1, length);
            try (var is = target.getBinaryStream(i, length)) {
                byte[] actual = is.readAllBytes();
                assertArrayEquals(expected, actual);
            }
        }

        length = 6;
        for (int i = 1; i <= 5; i++) {
            byte[] expected = toByteArray(expectedList, i - 1, length);
            try (var is = target.getBinaryStream(i, length)) {
                byte[] actual = is.readAllBytes();
                assertArrayEquals(expected, actual);
            }
        }
    }

    @Test
    void position() throws Exception {
        var target = new TsurugiJdbcBlob(32);
        target.setBytes(1, new byte[] { 1, 2, 1, 3, 4, 1, 2, 5 });

        for (int i = 1; i <= 8; i++) {
            assertEquals(i, target.position(new byte[0], i));
        }

        assertEquals(1, target.position(new byte[] { 1 }, 1));
        assertEquals(3, target.position(new byte[] { 1 }, 2));
        assertEquals(3, target.position(new byte[] { 1 }, 3));
        assertEquals(6, target.position(new byte[] { 1 }, 4));
        assertEquals(6, target.position(new byte[] { 1 }, 5));
        assertEquals(6, target.position(new byte[] { 1 }, 6));
        assertEquals(-1, target.position(new byte[] { 1 }, 7));

        assertEquals(1, target.position(new byte[] { 1, 2 }, 1));
        assertEquals(6, target.position(new byte[] { 1, 2 }, 2));

        assertEquals(3, target.position(new byte[] { 1, 3 }, 1));

        {
            var pattern = new TsurugiJdbcBlob(32);
            pattern.setBytes(1, new byte[] { 1, 2 });
            assertEquals(1, target.position(pattern, 1));
            assertEquals(6, target.position(pattern, 2));
        }
    }

    @Test
    void setBinaryStream() throws Exception {
        var target = new TsurugiJdbcBlob(32);
        try (var os = target.setBinaryStream(1)) {
            os.write(0x7f);
            os.write(new byte[] { 1, 2, 3 });
        }
        assertArrayEquals(new byte[] { 0x7f, 1, 2, 3 }, target.getBytes(1, 32));
    }

    @Test
    void truncate() throws Exception {
        {
            var target = new TsurugiJdbcBlob(32);
            target.truncate(0);
            assertEquals(0, target.length());
        }
        {
            var target = new TsurugiJdbcBlob(32);
            target.setBytes(1, new byte[] { 1, 2, 3 });

            target.truncate(0);
            assertEquals(0, target.length());
        }
    }
}
