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
package com.tsurugidb.jdbc.statement;

import static com.tsurugidb.jdbc.TsurugiConfig.DEFAULT_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiConfig.EXECUTE_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiConfig.QUERY_TIMEOUT;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnectionConfig;
import com.tsurugidb.jdbc.property.TsurugiJdbcProperties;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyInt;

/**
 * Tsurugi JDBC Statement Configuration.
 */
public class TsurugiJdbcStatementConfig {

    /**
     * Create statement configuration.
     *
     * @param from connection configuration
     * @return statement configuration
     */
    public static TsurugiJdbcStatementConfig of(TsurugiJdbcConnectionConfig from) {
        var config = new TsurugiJdbcStatementConfig();
        config.properties.copyFrom(from.getInternalProperties());
        return config;
    }

    private final TsurugiJdbcPropertyInt executeTimeout = new TsurugiJdbcPropertyInt(EXECUTE_TIMEOUT);
    private final TsurugiJdbcPropertyInt queryTimeout = new TsurugiJdbcPropertyInt(QUERY_TIMEOUT);
    private final TsurugiJdbcPropertyInt defaultTimeout = new TsurugiJdbcPropertyInt(DEFAULT_TIMEOUT);

    private final TsurugiJdbcProperties properties = TsurugiJdbcProperties.of(//
            executeTimeout, //
            queryTimeout, //
            defaultTimeout);

    /**
     * Get internal properties.
     *
     * @return properties
     */
    @TsurugiJdbcInternal
    public TsurugiJdbcProperties getInternalProperties() {
        return this.properties;
    }

    /**
     * Get execute timeout.
     *
     * @return execute timeout [seconds]
     */
    public int getExecuteTimeout() {
        return executeTimeout.value().orElse(getDefaultTimeout());
    }

    /**
     * Set SELECT timeout.
     *
     * @param timeout SELECT timeout [seconds]
     */
    public void setQueryTimeout(int timeout) {
        queryTimeout.setValue(timeout);
    }

    /**
     * Get SELECT timeout.
     *
     * @return SELECT timeout [seconds]
     */
    public int getQueryTimeout() {
        return queryTimeout.value().orElse(getDefaultTimeout());
    }

    // Common

    /**
     * Set default timeout.
     *
     * @return default timeout [seconds]
     */
    public int getDefaultTimeout() {
        return defaultTimeout.value().orElse(0);
    }
}
