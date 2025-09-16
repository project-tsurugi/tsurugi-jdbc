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
package com.tsurugidb.jdbc.driver;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import com.tsurugidb.jdbc.TsurugiJdbcProperties;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;

public class TsurugiJdbcUrlParser {

    private static final String URL_PREFIX = "jdbc:tsurugi:";

    public static boolean acceptUrl(TsurugiJdbcFactory factory, String url) throws SQLException {
        if (url == null) {
            throw factory.getExceptionHandler().jdbcUrlNullException();
        }

        return url.startsWith(URL_PREFIX);
    }

    public static TsurugiJdbcProperties parse(TsurugiJdbcFactory factory, String url) throws SQLException {
        if (url == null) {
            throw factory.getExceptionHandler().jdbcUrlNullException();
        }
        if (!url.startsWith(URL_PREFIX)) {
            return null;
        }

        String endpointUrl;
        String queryString;
        {
            int s = URL_PREFIX.length();
            int q = url.indexOf('?', s);
            if (q >= 0) {
                endpointUrl = url.substring(s, q);
                queryString = url.substring(q + 1);
            } else {
                endpointUrl = url.substring(s);
                queryString = null;
            }
        }

        var result = new TsurugiJdbcProperties();

        if (queryString != null) {
            String[] pairs = queryString.split("&");
            for (String pair : pairs) {
                String[] keyValue = pair.split("=", 2);

                String key = decode(keyValue[0]);
                String value = keyValue.length > 1 ? decode(keyValue[1]) : null;

                result.put(factory, key, value);
            }
        }

        result.setEndpoint(endpointUrl);

        return result;
    }

    private static String decode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }
}
