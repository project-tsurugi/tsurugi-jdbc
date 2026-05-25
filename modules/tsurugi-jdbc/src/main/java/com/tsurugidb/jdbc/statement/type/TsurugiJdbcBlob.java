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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

/**
 * Tsurugi JDBC Blob.
 *
 * @since 0.5.0
 */
public class TsurugiJdbcBlob implements Blob {

    private byte[] buffer;
    private int length;

    /**
     * Creates a new instance.
     *
     * @param capacity initial capacity
     */
    public TsurugiJdbcBlob(int capacity) {
        this.buffer = new byte[capacity];
        this.length = 0;
    }

    /**
     * Creates a new instance.
     *
     * @param buffer buffer
     */
    public TsurugiJdbcBlob(byte[] buffer) {
        this.buffer = buffer;
        this.length = buffer.length;
    }

    @Override
    public long length() throws SQLException {
        checkValid();
        return this.length;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        checkValid();

        int index = (int) pos - 1;
        if (index == 0 && this.length == 0) {
            return new byte[0];
        }
        if (index < 0 || index >= this.length) {
            throw new SQLException("Invalid position: " + pos);
        }

        int len = Math.min(length, this.length - index);
        if (len < 0) {
            throw new SQLException("Invalid length: " + length);
        }

        byte[] result = new byte[len];
        System.arraycopy(this.buffer, index, result, 0, len);
        return result;
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        checkValid();
        return new ByteArrayInputStream(this.buffer, 0, this.length);
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        checkValid();

        int index = (int) pos - 1;
        if (index == 0 && this.length == 0) {
            return new ByteArrayInputStream(new byte[0]);
        }
        if (index < 0 || index >= this.length) {
            throw new SQLException("Invalid position: " + pos);
        }

        long len = Math.min(length, this.length - index);
        if (len < 0) {
            throw new SQLException("Invalid length: " + length);
        }

        return new ByteArrayInputStream(this.buffer, index, (int) len);
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        checkValid();

        if (start < 1) {
            throw new SQLException("Invalid start position: " + start);
        }

        int index = (int) start - 1;
        if (pattern.length == 0) {
            if (this.length == 0 && index == 0) {
                return 1;
            }
            if (index < this.length) {
                return start;
            }
            return -1;
        }

        loop: for (int i = index; i < this.length; i++) {
            for (int j = 0; j < pattern.length; j++) {
                if (i + j >= this.length || this.buffer[i + j] != pattern[j]) {
                    continue loop;
                }
            }
            return i + 1;
        }
        return -1;
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        checkValid();
        return position(pattern.getBytes(1, (int) pattern.length()), start);
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        checkValid();
        return setBytes(pos, bytes, 0, bytes.length);
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        checkValid();

        int index = (int) pos - 1;
        if (index < 0) {
            throw new SQLException("Invalid position: " + pos);
        }

        ensureCapacity(index + len);

        System.arraycopy(bytes, offset, this.buffer, index, len);
        this.length = Math.max(this.length, index + len);
        return len;
    }

    private void ensureCapacity(int minCapacity) {
        if (minCapacity > this.buffer.length) {
            int newCapacity = Math.max(this.buffer.length * 2, minCapacity);
            byte[] newBuffer = new byte[newCapacity];
            System.arraycopy(this.buffer, 0, newBuffer, 0, this.length);

            this.buffer = newBuffer;
        }
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        checkValid();

        int index = (int) pos - 1;
        if (index < 0) {
            throw new SQLException("Invalid position: " + pos);
        }

        return new TsurugiJdbcBlobOutputStream(index);
    }

    class TsurugiJdbcBlobOutputStream extends OutputStream {
        private int index;

        public TsurugiJdbcBlobOutputStream(int index) {
            this.index = index;
        }

        @Override
        public void write(int b) throws IOException {
            checkValid();
            ensureCapacity(index + 1);

            TsurugiJdbcBlob.this.buffer[index] = (byte) b;
            this.index++;

            TsurugiJdbcBlob.this.length = Math.max(TsurugiJdbcBlob.this.length, index);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            checkValid();
            ensureCapacity(index + len);

            System.arraycopy(b, off, TsurugiJdbcBlob.this.buffer, index, len);
            this.index += len;

            TsurugiJdbcBlob.this.length = Math.max(TsurugiJdbcBlob.this.length, index);
        }

        private void checkValid() throws IOException {
            try {
                TsurugiJdbcBlob.this.checkValid();
            } catch (SQLException e) {
                throw new IOException("Blob has been freed", e);
            }
        }
    }

    @Override
    public void truncate(long len) throws SQLException {
        checkValid();

        if (len < 0 || len > this.length) {
            throw new SQLException("Invalid length: " + len);
        }

        this.length = (int) len;
    }

    @Override
    public void free() throws SQLException {
        if (this.buffer != null) {
            this.buffer = null;
            this.length = 0;
        }
    }

    private void checkValid() throws SQLException {
        if (this.buffer == null) {
            throw new SQLException("Blob has been freed");
        }
    }
}
