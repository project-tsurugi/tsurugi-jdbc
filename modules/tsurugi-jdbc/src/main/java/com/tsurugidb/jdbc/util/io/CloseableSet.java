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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A set of closeable resources that can be closed together.
 *
 * @since 0.5.0
 */
public class CloseableSet implements Closeable {
    private final List<Closeable> closeables = new ArrayList<>();

    /**
     * Adds a closeable resource to the set.
     *
     * @param closeable the closeable resource to add
     */
    public synchronized void add(Closeable closeable) {
        closeables.add(Objects.requireNonNull(closeable));
    }

    @Override
    public synchronized void close() throws IOException {
        IOException exception = null;

        for (Closeable closeable : this.closeables) {
            try {
                closeable.close();
            } catch (IOException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
            } catch (Exception e) {
                if (exception == null) {
                    exception = new IOException("Failed to close resource", e);
                } else {
                    exception.addSuppressed(e);
                }
            }
        }

        closeables.clear();

        if (exception != null) {
            throw exception;
        }
    }
}
