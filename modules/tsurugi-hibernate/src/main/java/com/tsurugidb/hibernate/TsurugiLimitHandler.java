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
package com.tsurugidb.hibernate;

import org.hibernate.dialect.pagination.AbstractLimitHandler;
import org.hibernate.query.spi.Limit;
import org.hibernate.sql.ast.spi.ParameterMarkerStrategy;

/**
 * Tsurugi LimitHandler.
 */
public class TsurugiLimitHandler extends AbstractLimitHandler {

    /**
     * Tsurugi LimitHandler instance.
     */
    public static final TsurugiLimitHandler INSTANCE = new TsurugiLimitHandler();

    @Override
    public String processSql(String sql, Limit limit) {
        return processSql(sql, -1, null, limit);
    }

    private String processSql(String sql, int jdbcParameterCount, ParameterMarkerStrategy parameterMarkerStrategy, Limit limit) {
        boolean hasLimit = hasMaxRows(limit);
        if (!hasLimit) {
            return sql;
        }

        var sb = new StringBuilder(sql);
        sb.append(" limit ");
        sb.append(limit.getMaxRows());
        return sb.toString();
    }

    @Override
    public final boolean supportsLimit() {
        return true;
    }

    @Override
    public boolean supportsOffset() {
        return false;
    }

    @Override
    public boolean supportsLimitOffset() {
        return false;
    }

    @Override
    public final boolean supportsVariableLimit() {
        return false;
    }
}
