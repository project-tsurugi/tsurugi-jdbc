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
package com.tsurugidb.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.tsurugidb.jdbc.driver.TsurugiJdbcUrlParser;
import com.tsurugidb.jdbc.factory.HasFactory;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.exception.ServerException;

public class TsurugiDriver implements Driver, HasFactory {
    private static final Logger PARENT_LOGGER = Logger.getLogger(TsurugiDriver.class.getPackageName());
    private static final Logger LOG = Logger.getLogger(TsurugiDriver.class.getName());

    public static final String DRIVER_NAME = "Tsurugi JDBC Driver";
    public static final String DRIVER_VERSION = "0.1.0";
    public static final String TSURUGI_VERSION = "1.6.0"; // FIXME tsurugidbのバージョンが取れるようになるまでの暫定用

    public static final int DRIVER_VERSION_MAJOR;
    public static final int DRIVER_VERSION_MINAR;
    static {
        String[] ss = DRIVER_VERSION.split(Pattern.quote("."));
        DRIVER_VERSION_MAJOR = Integer.parseInt(ss[0]);
        DRIVER_VERSION_MINAR = Integer.parseInt(ss[1]);
    }

    static {
        var driver = new TsurugiDriver();
        try {
            DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private TsurugiJdbcFactory factory = new TsurugiJdbcFactory();

    @Override
    public void setFactory(TsurugiJdbcFactory factory) {
        this.factory = factory;
    }

    @Override
    public TsurugiJdbcFactory getFactory() {
        return this.factory;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        var properties = TsurugiJdbcUrlParser.parse(factory, url);
        if (properties == null) {
            return null;
        }
        properties.putAll(factory, info);

        var builder = createLowSessionBuilder(properties);

        int timeout = properties.getConnectTimeout(DriverManager.getLoginTimeout());
        LOG.config(() -> String.format("connectTimeout=%d [seconds]", timeout));

        Session session;
        try {
            session = builder.create(timeout, TimeUnit.SECONDS);
        } catch (IOException | ServerException | InterruptedException | TimeoutException e) {
            throw factory.getExceptionHandler().sqlException("Connect error", e);
        }

        return factory.createConnection(session, properties);
    }

    protected SessionBuilder createLowSessionBuilder(TsurugiJdbcProperties properties) throws SQLException {
        String endpoint = properties.getEndpoint();
        LOG.config(() -> String.format("endpoint=%s", endpoint));
        var builder = SessionBuilder.connect(endpoint);

        Credential credential = properties.getCredential(factory);
        LOG.config(() -> String.format("credential=%s", credential));
        builder.withCredential(credential);

        String applicationName = properties.getApplicationName();
        LOG.config(() -> String.format("applicationName=%s", applicationName));
        if (applicationName != null) {
            builder.withApplicationName(applicationName);
        }

        String label = properties.getSessionLabel();
        LOG.config(() -> String.format("label=%s", label));
        if (label != null) {
            builder.withLabel(label);
        }

        boolean keepAlive = properties.getKeepAlive();
        LOG.config(() -> String.format("keepAlive=%b", keepAlive));
        builder.withKeepAlive(keepAlive);

        return builder;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return TsurugiJdbcUrlParser.acceptUrl(factory, url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        throw new UnsupportedOperationException(); // TODO TsurugiDriver.getPropertyInfo()
    }

    @Override
    public int getMajorVersion() {
        return DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getMinorVersion() {
        return DRIVER_VERSION_MINAR;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return PARENT_LOGGER;
    }
}
