/*
 * Copyright 2025-2026 Project Tsurugi.
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.sql.type.IceaxeObjectFactory;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnectionBuilder;
import com.tsurugidb.jdbc.driver.TsurugiJdbcCredentialSetter;
import com.tsurugidb.jdbc.driver.TsurugiJdbcLobPathMappingEntry;
import com.tsurugidb.jdbc.driver.TsurugiJdbcLobSettingSetter;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.FileCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;

public class JdbcDbTestConnector {

    private static final String SYSPROP_DBTEST_ENDPOINT = "tsurugi.dbtest.endpoint";
    private static final String SYSPROP_DBTEST_USER = "tsurugi.dbtest.user";
    private static final String SYSPROP_DBTEST_PASSWORD = "tsurugi.dbtest.password";
    private static final String SYSPROP_DBTEST_AUTH_TOKEN = "tsurugi.dbtest.auth-token";
    private static final String SYSPROP_DBTEST_CREDENTIALS = "tsurugi.dbtest.credentials";
    private static final String SYSPROP_DBTEST_LOB_SEND_PATH_MAPPING = "tsurugi.dbtest.lob-send-path-mapping";
    private static final String SYSPROP_DBTEST_LOB_RECV_PATH_MAPPING = "tsurugi.dbtest.lob-recv-path-mapping";
    private static final String SYSPROP_DBTEST_BLOB_RELAY_SERVICE_ENDPOINT = "tsurugi.dbtest.blob-relay-service-endpoint";

    private static String staticEndpoint;
    private static String staticJdbcUrl;
    private static JdbcDbTestCredential staticCredential;
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
        var queryParts = new ArrayList<String>();
        String credential = getCredential().toQueryString("");
        if (!credential.isEmpty()) {
            queryParts.add(credential);
        }

        String lobSendPathMapping = getSystemProperty(SYSPROP_DBTEST_LOB_SEND_PATH_MAPPING);
        encoding(queryParts, "lobPathMappingOnSend", lobSendPathMapping);
        String lobRecvPathMapping = getSystemProperty(SYSPROP_DBTEST_LOB_RECV_PATH_MAPPING);
        encoding(queryParts, "lobPathMappingOnReceive", lobRecvPathMapping);
        String blobRelayServiceEndpoint = getSystemProperty(SYSPROP_DBTEST_BLOB_RELAY_SERVICE_ENDPOINT);
        encoding(queryParts, "blobRelayServiceEndpoint", blobRelayServiceEndpoint);

        if (queryParts.isEmpty()) {
            return "";
        }
        return "?" + String.join("&", queryParts);
    }

    private static void encoding(List<String> queryParts, String key, String value) {
        if (value == null) {
            return;
        }

        var sb = new StringBuilder();
        sb.append(URLEncoder.encode(key, StandardCharsets.UTF_8));
        sb.append("=");
        sb.append(URLEncoder.encode(value, StandardCharsets.UTF_8));

        queryParts.add(sb.toString());
    }

    public static Properties getConnectProperties() {
        return getCredential().toProperties();
    }

    public static void setCredentialTo(TsurugiJdbcCredentialSetter config) {
        getCredential().setTo(config);
    }

    public static void setCredentialTo(TsurugiJdbcConnectionBuilder builder) {
        getCredential().setTo(builder);
    }

    private static JdbcDbTestCredential getCredential() {
        if (staticCredential == null) {
            staticCredential = JdbcDbTestCredential.create();
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

    public static boolean enableNullCredential() {
        String endpoint = getEndPoint();
        var connector = TsurugiConnector.of(endpoint, NullCredential.INSTANCE);
        try (var session = connector.createSession()) {
            session.getLowSession();
            return true;
        } catch (TsurugiIOException e) {
            if (e.getDiagnosticCode() == CoreServiceCode.AUTHENTICATION_ERROR) {
                return false;
            }
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void setLobSettingTo(TsurugiJdbcLobSettingSetter config) {
        String lobSendPathMapping = getSystemProperty(SYSPROP_DBTEST_LOB_SEND_PATH_MAPPING);
        if (lobSendPathMapping != null) {
            config.setLobPathMappingOnSend(List.of(lobSendPathMapping));
        }
        String lobRecvPathMapping = getSystemProperty(SYSPROP_DBTEST_LOB_RECV_PATH_MAPPING);
        if (lobRecvPathMapping != null) {
            config.setLobPathMappingOnReceive(List.of(lobRecvPathMapping));
        }
        String blobRelayServiceEndpoint = getSystemProperty(SYSPROP_DBTEST_BLOB_RELAY_SERVICE_ENDPOINT);
        if (blobRelayServiceEndpoint != null) {
            var uri = URI.create(blobRelayServiceEndpoint);
            config.setBlobRelayServiceEndpoint(uri);
        }
    }

    public static void setLobSettingTo(TsurugiJdbcConnectionBuilder builder) {
        String lobSendPathMapping = getSystemProperty(SYSPROP_DBTEST_LOB_SEND_PATH_MAPPING);
        if (lobSendPathMapping != null) {
            var entry = TsurugiJdbcLobPathMappingEntry.parse(lobSendPathMapping);
            builder.lobPathMappingOnSend(entry.clientPath(), entry.serverPath());
        }
        String lobRecvPathMapping = getSystemProperty(SYSPROP_DBTEST_LOB_RECV_PATH_MAPPING);
        if (lobRecvPathMapping != null) {
            var entry = TsurugiJdbcLobPathMappingEntry.parse(lobRecvPathMapping);
            builder.lobPathMappingOnReceive(entry.clientPath(), entry.serverPath());
        }
        String blobRelayServiceEndpoint = getSystemProperty(SYSPROP_DBTEST_BLOB_RELAY_SERVICE_ENDPOINT);
        if (blobRelayServiceEndpoint != null) {
            var uri = URI.create(blobRelayServiceEndpoint);
            builder.blobRelayServiceEndpoint(uri);
        }
    }

    public static void setLobSettingTo(TgSessionOption option) {
        String lobSendPathMapping = getSystemProperty(SYSPROP_DBTEST_LOB_SEND_PATH_MAPPING);
        if (lobSendPathMapping != null) {
            var entry = TsurugiJdbcLobPathMappingEntry.parse(lobSendPathMapping);
            option.addLargeObjectPathMappingOnSend(entry.clientPath(), entry.serverPath());
            IceaxeObjectFactory.getDefaultInstance().setTempDirectory(entry.clientPath());
        }
        String lobRecvPathMapping = getSystemProperty(SYSPROP_DBTEST_LOB_RECV_PATH_MAPPING);
        if (lobRecvPathMapping != null) {
            var entry = TsurugiJdbcLobPathMappingEntry.parse(lobRecvPathMapping);
            option.addLargeObjectPathMappingOnReceive(entry.serverPath(), entry.clientPath());
        }
        String blobRelayServiceEndpoint = getSystemProperty(SYSPROP_DBTEST_BLOB_RELAY_SERVICE_ENDPOINT);
        if (blobRelayServiceEndpoint != null) {
            var uri = URI.create(blobRelayServiceEndpoint);
            option.setBlobRelayServiceEndpoint(uri);
        }
    }
}
