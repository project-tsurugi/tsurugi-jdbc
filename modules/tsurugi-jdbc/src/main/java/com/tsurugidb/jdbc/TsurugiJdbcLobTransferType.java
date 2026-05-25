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
package com.tsurugidb.jdbc;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.tsubakuro.common.BlobTransferType;

/**
 * Tsurugi JDBC Large Object Transfer Type.
 *
 * @since 0.5.0
 */
public enum TsurugiJdbcLobTransferType {
    /** Default transfer type */
    DEFAULT(BlobTransferType.DEFAULT),
    /** Does not use transfer type */
    NOT_USE(BlobTransferType.DOES_NOT_USE),
    /** Privileged transfer type */
    PRIVILEGED(BlobTransferType.PRIVILEGED),
    /** Blob Relay transfer type */
    RELAY(BlobTransferType.RELAY),

    ;

    private final BlobTransferType lowLobTransferType;

    TsurugiJdbcLobTransferType(BlobTransferType lowLobTransferType) {
        this.lowLobTransferType = lowLobTransferType;
    }

    /**
     * Get low-level large object transfer type.
     *
     * @return large object transfer type
     */
    @TsurugiJdbcInternal
    public BlobTransferType getLowLobTransferType() {
        return this.lowLobTransferType;
    }
}
