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

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import com.tsurugidb.jdbc.connection.TsurugiJdbcConnection;
import com.tsurugidb.jdbc.driver.TsurugiJdbcUrlParser;
import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.factory.HasFactory;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.property.TsurugiJdbcProperty;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;

/**
 * Tsurugi JDBC Driver.
 */
public class TsurugiDriver implements Driver, HasFactory {
    private static final Logger PARENT_LOGGER = Logger.getLogger(TsurugiDriver.class.getPackageName());
    private static final Logger LOG = Logger.getLogger(TsurugiDriver.class.getName());

    /** Driver name. */
    public static final String DRIVER_NAME = "Tsurugi JDBC Driver";
    /** Driver version. */
    public static final String DRIVER_VERSION = "0.2.0-SNAPSHOT";

    /** Driver major version. */
    public static final int DRIVER_VERSION_MAJOR;
    /** Driver minor version. */
    public static final int DRIVER_VERSION_MINOR;
    static {
        String[] ss = DRIVER_VERSION.split(Pattern.quote("."));
        DRIVER_VERSION_MAJOR = Integer.parseInt(ss[0]);
        DRIVER_VERSION_MINOR = Integer.parseInt(ss[1]);
    }

    private static final TsurugiDriver INSTANCE;
    static {
        try {
            INSTANCE = new TsurugiDriver();
            DriverManager.registerDriver(INSTANCE);
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Get the singleton instance of TsurugiDriver.
     *
     * @return instance
     */
    public static TsurugiDriver getTsurugiDriver() {
        return INSTANCE;
    }

    private TsurugiJdbcFactory factory = TsurugiJdbcFactory.getDefaultFactory();

    @Override
    public void setFactory(TsurugiJdbcFactory factory) {
        this.factory = Objects.requireNonNull(factory, "factory is null");
    }

    @Override
    public TsurugiJdbcFactory getFactory() {
        return this.factory;
    }

    /**
     * Get exception handler.
     *
     * @return exception handler
     */
    protected TsurugiJdbcExceptionHandler getExceptionHandler() {
        return getFactory().getExceptionHandler();
    }

    @Override
    public TsurugiJdbcConnection connect(String url, Properties info) throws SQLException {
        var config = TsurugiJdbcUrlParser.parse(factory, url, info);
        if (config == null) {
            return null;
        }

        return connect(config);
    }

    /**
     * Connect to the database.
     *
     * @param config configuration
     * @return connection
     * @throws SQLException if a database access error occurs
     */
    public TsurugiJdbcConnection connect(@Nonnull TsurugiConfig config) throws SQLException {
        Objects.requireNonNull(config, "config is null");
        var builder = createLowSessionBuilder(config);

        int timeout = config.getConnectTimeout();
        LOG.config(() -> String.format("connectTimeout=%d [seconds]", timeout));

        Session session;
        try {
            session = builder.create(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("Connect error", e);
        }

        return factory.createConnection(session, config);
    }

    /**
     * Create low-level SessionBuilder.
     *
     * @param config configuration
     * @return SessionBuilder
     * @throws SQLException If SessionBuilder creation fails
     */
    protected SessionBuilder createLowSessionBuilder(TsurugiConfig config) throws SQLException {
        String endpoint = config.getEndpoint();
        LOG.config(() -> String.format("endpoint=%s", endpoint));
        if (endpoint == null) {
            throw new IllegalArgumentException("endpoint not specified");
        }
        var builder = SessionBuilder.connect(endpoint);

        Credential credential = config.getCredential(factory);
        LOG.config(() -> String.format("credential=%s", credential));
        builder.withCredential(credential);

        String applicationName = config.getApplicationName();
        LOG.config(() -> String.format("applicationName=%s", applicationName));
        if (applicationName != null) {
            builder.withApplicationName(applicationName);
        }

        String label = config.getSessionLabel();
        LOG.config(() -> String.format("label=%s", label));
        if (label != null) {
            builder.withLabel(label);
        }

        boolean keepAlive = config.getKeepAlive();
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
        var config = TsurugiJdbcUrlParser.parse(factory, url, info);
        if (config == null) {
            return new DriverPropertyInfo[0];
        }

        var properties = config.getInternalProperties().getProperties();
        var result = new DriverPropertyInfo[properties.size()];

        int i = 0;
        for (var property : properties) {
            result[i++] = toDriverPropertyInfo(property);
        }

        return result;
    }

    /**
     * Convert to DriverPropertyInfo.
     *
     * @param property property
     * @return DriverPropertyInfo
     */
    protected DriverPropertyInfo toDriverPropertyInfo(TsurugiJdbcProperty property) {
        var info = new DriverPropertyInfo(property.name(), property.getStringValue());
        info.description = property.description();
        info.choices = property.getChoice();

        return info;
    }

    @Override
    public int getMajorVersion() {
        return DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getMinorVersion() {
        return DRIVER_VERSION_MINOR;
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
