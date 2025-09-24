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
package com.tsurugidb.jdbc.resultset;

import static com.tsurugidb.jdbc.TsurugiConfig.DEFAULT_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiConfig.QUERY_TIMEOUT;

import com.tsurugidb.jdbc.property.TsurugiJdbcProperties;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyInt;
import com.tsurugidb.jdbc.statement.TsurugiJdbcStatementConfig;

public class TsurugiJdbcResultSetConfig {

    public static TsurugiJdbcResultSetConfig of(TsurugiJdbcStatementConfig from) {
        var config = new TsurugiJdbcResultSetConfig();
        config.properties.copyFrom(from.getInternalProperties());
        return config;
    }

    private final TsurugiJdbcPropertyInt queryTimeout = new TsurugiJdbcPropertyInt(QUERY_TIMEOUT);
    private final TsurugiJdbcPropertyInt defaultTimeout = new TsurugiJdbcPropertyInt(DEFAULT_TIMEOUT);

    private final TsurugiJdbcProperties properties = TsurugiJdbcProperties.of( //
            queryTimeout, //
            defaultTimeout);

    public int getQueryTimeout() {
        return queryTimeout.value().orElse(getDefaultTimeout());
    }

    // Common

    public int getDefaultTimeout() {
        return defaultTimeout.value().orElse(0);
    }
}
