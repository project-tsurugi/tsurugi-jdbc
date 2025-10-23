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
package com.tsurugidb.hibernate.test.util;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class HibernateTestCredential {

    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String AUTH_TOKEN = "authToken";
    private static final String CREDENTIALS = "credentials";

    public static HibernateTestCredential create() {
        var map = new LinkedHashMap<String, String>();

        String user = HibernateTestConnector.getUser();
        if (user != null) {
            map.put(USER, user);
            String password = HibernateTestConnector.getPassword();
            map.put(PASSWORD, password);
        } else {
            String token = HibernateTestConnector.getAuthToken();
            if (token != null) {
                map.put(AUTH_TOKEN, token);
            } else {
                String path = HibernateTestConnector.getCredentials();
                if (path != null) {
                    map.put(CREDENTIALS, path);
                } else {
                    map.put(USER, "tsurugi");
                    map.put(PASSWORD, "password");
                }
            }
        }

        return new HibernateTestCredential(map);
    }

    private final Map<String, String> map;

    public HibernateTestCredential() {
        this(new LinkedHashMap<>());
    }

    public HibernateTestCredential(Map<String, String> map) {
        this.map = map;
    }

    public void setUser(String user) {
        map.put(USER, user);
    }

    public void setPassword(String password) {
        map.put(PASSWORD, password);
    }

    public void setAuthToken(String token) {
        map.put(AUTH_TOKEN, token);
    }

    public void setCredentials(String path) {
        map.put(CREDENTIALS, path);
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
}
