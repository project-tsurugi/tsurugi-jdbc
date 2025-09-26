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
package com.tsurugidb.jdbc.property;

import java.sql.ClientInfoStatus;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nullable;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;

/**
 * Tsurugi JDBC Properties.
 */
@TsurugiJdbcInternal
public class TsurugiJdbcProperties {

    /**
     * Create properties.
     *
     * @param properties source properties
     * @return properties
     */
    public static TsurugiJdbcProperties of(TsurugiJdbcProperty... properties) {
        var map = new LinkedHashMap<String, TsurugiJdbcProperty>(properties.length);
        for (var property : properties) {
            map.put(property.name(), property);
        }
        return new TsurugiJdbcProperties(map);
    }

    private final Map<String, TsurugiJdbcProperty> propertyMap;

    /**
     * Creates a new instance.
     *
     * @param map property map
     */
    public TsurugiJdbcProperties(Map<String, TsurugiJdbcProperty> map) {
        this.propertyMap = map;
    }

    /**
     * Set property value.
     *
     * @param factory factory
     * @param key     key
     * @param value   value
     * @throws SQLException if property value convert error occurs
     */
    public void put(TsurugiJdbcFactory factory, String key, String value) throws SQLException {
        var property = getProperty(key);
        if (property == null) {
            // FIXME throw SQLWarning ?
            return;
        }

        try {
            property.setStringValue(value);
        } catch (Exception e) {
            throw factory.getExceptionHandler().propertyConvertException(key, e);
        }
    }

    /**
     * Set property value from client info.
     *
     * @param key              key
     * @param value            value
     * @param failedProperties failed properties
     */
    public void putForClient(String key, String value, Map<String, ClientInfoStatus> failedProperties) {
        var property = getProperty(key);
        if (property == null) {
            failedProperties.put(key, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            return;
        }

        try {
            property.setStringValue(value);
        } catch (Exception e) {
            failedProperties.put(key, ClientInfoStatus.REASON_VALUE_INVALID);
            throw e;
        }
    }

    /**
     * Set all property values.
     *
     * @param factory factory
     * @param info    properties
     * @throws SQLException if property value convert error occurs
     */
    public void putAll(TsurugiJdbcFactory factory, Properties info) throws SQLException {
        if (info != null) {
            for (var entry : info.entrySet()) {
                put(factory, (String) entry.getKey(), (String) entry.getValue());
            }
        }
    }

    /**
     * Copy property values from other properties.
     *
     * @param fromProperties source properties
     */
    public void copyFrom(TsurugiJdbcProperties fromProperties) {
        for (var entry : propertyMap.entrySet()) {
            String key = entry.getKey();
            var from = fromProperties.getProperty(key);
            if (from != null) {
                var property = entry.getValue();
                property.setFrom(from);
            }
        }
    }

    /**
     * Copy property values from other properties if present.
     *
     * @param fromProperties source properties
     */
    public void copyIfPresentFrom(TsurugiJdbcProperties fromProperties) {
        for (var entry : propertyMap.entrySet()) {
            String key = entry.getKey();
            var from = fromProperties.getProperty(key);
            if (from != null && from.isPresentValue()) {
                var property = entry.getValue();
                property.setFrom(from);
            }
        }
    }

    /**
     * Get property.
     *
     * @param key key
     * @return property or null if not found
     */
    public @Nullable TsurugiJdbcProperty getProperty(String key) {
        return propertyMap.get(key);
    }

    /**
     * Get all properties.
     *
     * @return properties
     */
    public Collection<TsurugiJdbcProperty> getProperties() {
        return propertyMap.values();
    }
}
