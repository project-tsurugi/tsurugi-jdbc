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
package com.tsurugidb.hibernate.type;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.OffsetTime;

import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.BasicBinder;
import org.hibernate.type.descriptor.jdbc.TimeUtcAsOffsetTimeJdbcType;

/**
 * Tsurugi TimeUtc JdbcType.
 */
@SuppressWarnings("serial")
public class TsurugiTimeUtcJdbcType extends TimeUtcAsOffsetTimeJdbcType {

    /** instance */
    public static final TsurugiTimeUtcJdbcType INSTANCE = new TsurugiTimeUtcJdbcType();

    /**
     * Creates a new instance.
     */
    public TsurugiTimeUtcJdbcType() {
    }

    @Override
    public <X> ValueBinder<X> getBinder(final JavaType<X> javaType) {
        return new BasicBinder<>(javaType, this) {
            @Override
            protected void doBind(PreparedStatement st, X value, int index, WrapperOptions options) throws SQLException {
                final OffsetTime offsetTime = javaType.unwrap(value, OffsetTime.class, options);
                st.setObject(index, offsetTime, Types.TIME_WITH_TIMEZONE);
            }

            @Override
            protected void doBind(CallableStatement st, X value, String name, WrapperOptions options) throws SQLException {
                final OffsetTime offsetTime = javaType.unwrap(value, OffsetTime.class, options);
                st.setObject(name, offsetTime, Types.TIME_WITH_TIMEZONE);
            }
        };
    }
}
