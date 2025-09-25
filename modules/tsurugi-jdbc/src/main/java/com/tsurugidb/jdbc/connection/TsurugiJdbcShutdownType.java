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
package com.tsurugidb.jdbc.connection;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.tsubakuro.common.ShutdownType;

/**
 * Tsurugi JDBC Shutdown Type.
 */
public enum TsurugiJdbcShutdownType {
    /** NOTHING */
    NOTHING(null),
    /** GRACEFUL */
    GRACEFUL(ShutdownType.GRACEFUL),
    /** FORCEFUL */
    FORCEFUL(ShutdownType.FORCEFUL),

    ;

    private final ShutdownType lowType;

    TsurugiJdbcShutdownType(ShutdownType lowType) {
        this.lowType = lowType;
    }

    /**
     * Get low-level shutdown type.
     *
     * @return shutdown type
     */
    @TsurugiJdbcInternal
    public ShutdownType getLowShutdownType() {
        return this.lowType;
    }
}
