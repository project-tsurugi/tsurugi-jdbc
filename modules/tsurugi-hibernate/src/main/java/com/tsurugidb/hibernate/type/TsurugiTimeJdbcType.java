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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalTime;

import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.ValueExtractor;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.BasicBinder;
import org.hibernate.type.descriptor.jdbc.BasicExtractor;
import org.hibernate.type.descriptor.jdbc.TimeJdbcType;

/**
 * Tsurugi Time JdbcType.
 */
@SuppressWarnings("serial")
public class TsurugiTimeJdbcType extends TimeJdbcType {

    /** instance */
    public static final TsurugiTimeJdbcType INSTANCE = new TsurugiTimeJdbcType();

    /**
     * Creates a new instance.
     */
    public TsurugiTimeJdbcType() {
    }

    @Override
    public <X> ValueBinder<X> getBinder(final JavaType<X> javaType) {
        return new BasicBinder<>(javaType, this) {
            @Override
            protected void doBind(PreparedStatement st, X value, int index, WrapperOptions options) throws SQLException {
                final LocalTime localTime = javaType.unwrap(value, LocalTime.class, options);
                st.setObject(index, localTime, Types.TIME);
            }

            @Override
            protected void doBind(CallableStatement st, X value, String name, WrapperOptions options) throws SQLException {
                final LocalTime localTime = javaType.unwrap(value, LocalTime.class, options);
                st.setObject(name, localTime, Types.TIME);
            }
        };
    }

    @Override
    public <X> ValueExtractor<X> getExtractor(final JavaType<X> javaType) {
        return new BasicExtractor<>(javaType, this) {
            @Override
            protected X doExtract(ResultSet rs, int paramIndex, WrapperOptions options) throws SQLException {
                return javaType.wrap(rs.getObject(paramIndex, LocalTime.class), options);
            }

            @Override
            protected X doExtract(CallableStatement statement, int index, WrapperOptions options) throws SQLException {
                return javaType.wrap(statement.getObject(index, LocalTime.class), options);
            }

            @Override
            protected X doExtract(CallableStatement statement, String name, WrapperOptions options) throws SQLException {
                return javaType.wrap(statement.getObject(name, LocalTime.class), options);
            }
        };
    }
}
