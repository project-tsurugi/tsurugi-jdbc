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
package com.tsurugidb.jdbc.util.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

class CloseableSetTest {

    static class MockCloseable implements Closeable {
        private boolean closed = false;

        @Override
        public void close() throws IOException {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }

    @Test
    void close0() throws IOException {
        var target = new CloseableSet();

        target.close();
    }

    @Test
    void close1() throws IOException {
        var target = new CloseableSet();

        for (int count = 1; count < 3; count++) {
            var list = Stream.generate(() -> new MockCloseable()).limit(count).collect(Collectors.toList());
            list.forEach(target::add);

            target.close();

            for (var closeable : list) {
                assertTrue(closeable.isClosed());
            }
        }
    }

    @Test
    void exception1() {
        var target = new CloseableSet();

        var exception1 = new IOException("exception1");
        target.add(() -> {
            throw exception1;
        });

        var thrown = assertThrows(IOException.class, target::close);
        assertEquals(exception1, thrown);
    }

    @Test
    void exception2() {
        var target = new CloseableSet();

        var exception1 = new IOException("exception1");
        var exception2 = new IOException("exception2");
        target.add(() -> {
            throw exception1;
        });
        target.add(() -> {
            throw exception2;
        });

        var thrown = assertThrows(IOException.class, target::close);
        assertEquals(exception1, thrown);
        assertArrayEquals(new Throwable[] { exception2 }, thrown.getSuppressed());
    }
}
