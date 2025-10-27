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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Properties;

import javax.sql.DataSource;

import com.tsurugidb.jdbc.TsurugiDriver;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.FileCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class HibernateTestConnector {

    private static final String SYSPROP_DBTEST_ENDPOINT = "tsurugi.dbtest.endpoint";
    private static final String SYSPROP_DBTEST_USER = "tsurugi.dbtest.user";
    private static final String SYSPROP_DBTEST_PASSWORD = "tsurugi.dbtest.password";
    private static final String SYSPROP_DBTEST_AUTH_TOKEN = "tsurugi.dbtest.auth-token";
    private static final String SYSPROP_DBTEST_CREDENTIALS = "tsurugi.dbtest.credentials";

    private static String staticEndpoint;
    private static String staticJdbcUrl;
    private static HibernateTestCredential staticCredential;
    private static Credential staticIceaxeCredential;

    public static String getEndPoint() {
        if (staticEndpoint == null) {
            staticEndpoint = System.getProperty(SYSPROP_DBTEST_ENDPOINT, "tcp://localhost:12345");
        }
        return staticEndpoint;
    }

    public static String getJdbcUrlWithCredential() {
        String url = getJdbcUrl();
        String queryString = getJdbcUrlQueryString();
        return url + queryString;
    }

    public static String getJdbcUrl() {
        if (staticJdbcUrl == null) {
            staticJdbcUrl = "jdbc:tsurugi:" + getEndPoint();
        }
        return staticJdbcUrl;
    }

    public static String getJdbcUrlQueryString() {
        return getCredential().toQueryString();
    }

    public static Properties getConnectProperties() {
        return getCredential().toProperties();
    }

    private static HibernateTestCredential getCredential() {
        if (staticCredential == null) {
            staticCredential = HibernateTestCredential.create();
        }
        return staticCredential;
    }

    public static Credential getIceaxeCredential() {
        if (staticIceaxeCredential == null) {
            staticIceaxeCredential = createIceaxeCredential();
        }
        return staticIceaxeCredential;
    }

    private static Credential createIceaxeCredential() {
        String user = getUser();
        if (user != null) {
            String password = getPassword();
            return new UsernamePasswordCredential(user, password);
        }

        String authToken = getAuthToken();
        if (authToken != null) {
            return new RememberMeCredential(authToken);
        }

        String credentials = getCredentials();
        if (credentials != null) {
            try {
                return FileCredential.load(Path.of(credentials));
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }

//      return NullCredential.INSTANCE;
        return new UsernamePasswordCredential("tsurugi", "password");
    }

    public static String getUser() {
        return getSystemProperty(SYSPROP_DBTEST_USER);
    }

    public static String getPassword() {
        return getSystemProperty(SYSPROP_DBTEST_PASSWORD);
    }

    public static String getAuthToken() {
        return getSystemProperty(SYSPROP_DBTEST_AUTH_TOKEN);
    }

    public static String getCredentials() {
        return getSystemProperty(SYSPROP_DBTEST_CREDENTIALS);
    }

    private static String getSystemProperty(String key) {
        String value = System.getProperty(key);
        if (value != null && value.isEmpty()) {
            return null;
        }
        return value;
    }

    public static DataSource createDataSource() {
        var config = new HikariConfig();
        config.setDriverClassName(TsurugiDriver.class.getCanonicalName());
        config.setJdbcUrl(getJdbcUrl());
        config.setUsername(getUser());
        config.setPassword(getPassword());

        // プールサイズを最小限に
        config.setMaximumPoolSize(1); // 最大1接続
        config.setMinimumIdle(0); // アイドル接続を保持しない

        return new HikariDataSource(config);
    }
}
