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
package com.tsurugidb.jdbc.test.util;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import com.tsurugidb.jdbc.TsurugiConfig;
import com.tsurugidb.jdbc.driver.TsurugiJdbcCredentialSetter;

public class JdbcDbTestCredential {

    public static JdbcDbTestCredential create() {
        var map = new LinkedHashMap<String, String>();

        String user = JdbcDbTestConnector.getUser();
        if (user != null) {
            map.put(TsurugiConfig.USER, user);
            String password = JdbcDbTestConnector.getPassword();
            map.put(TsurugiConfig.PASSWORD, password);
        } else {
            String token = JdbcDbTestConnector.getAuthToken();
            if (token != null) {
                map.put(TsurugiConfig.AUTH_TOKEN, token);
            } else {
                String path = JdbcDbTestConnector.getCredentials();
                if (path != null) {
                    map.put(TsurugiConfig.CREDENTIALS, path);
                } else {
                    map.put(TsurugiConfig.USER, "tsurugi");
                    map.put(TsurugiConfig.PASSWORD, "password");
                }
            }
        }

        return new JdbcDbTestCredential(map);
    }

    private final Map<String, String> map;

    public JdbcDbTestCredential() {
        this(new LinkedHashMap<>());
    }

    public JdbcDbTestCredential(Map<String, String> map) {
        this.map = map;
    }

    public void setUser(String user) {
        map.put(TsurugiConfig.USER, user);
    }

    public void setPassword(String password) {
        map.put(TsurugiConfig.PASSWORD, password);
    }

    public void setAuthToken(String token) {
        map.put(TsurugiConfig.AUTH_TOKEN, token);
    }

    public void setCredentials(String path) {
        map.put(TsurugiConfig.CREDENTIALS, path);
    }

    public String toQueryString() {
        return toQueryString("?");
    }

    public String toQueryString(String prefix) {
        var sb = new StringBuilder();

        String p = prefix;
        for (var entry : map.entrySet()) {
            encoding(sb, p, entry.getKey(), entry.getValue());
            p = "&";
        }

        return sb.toString();
    }

    private void encoding(StringBuilder sb, String prefix, String key, String value) {
        if (prefix != null) {
            sb.append(prefix);
        }
        sb.append(URLEncoder.encode(key, StandardCharsets.UTF_8));
        sb.append("=");
        sb.append(URLEncoder.encode(value, StandardCharsets.UTF_8));
    }

    public Properties toProperties() {
        var properties = new Properties();

        for (var entry : map.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }

        return properties;
    }

    public void setTo(TsurugiJdbcCredentialSetter config) {
        String user = JdbcDbTestConnector.getUser();
        if (user != null) {
            config.setUser(user);
            String password = JdbcDbTestConnector.getPassword();
            config.setPassword(password);
        } else {
            String token = JdbcDbTestConnector.getAuthToken();
            if (token != null) {
                config.setAuthToken(token);
            } else {
                String path = JdbcDbTestConnector.getCredentials();
                if (path != null) {
                    config.setCredentials(path);
                } else {
                    config.setUser("tsurugi");
                    config.setPassword("password");
                }
            }
        }
    }
}
