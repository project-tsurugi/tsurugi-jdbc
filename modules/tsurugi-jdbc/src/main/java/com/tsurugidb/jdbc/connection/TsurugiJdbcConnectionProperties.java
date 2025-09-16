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
package com.tsurugidb.jdbc.connection;

import static com.tsurugidb.jdbc.TsurugiJdbcProperties.AUTO_COMMIT;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.BEGIN_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.COMMIT_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.DEFAULT_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.EXECUTE_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.QUERY_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.ROLLBACK_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.SHUTDOWN_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.SHUTDOWN_TYPE;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.TRANSACTION_LABEL;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.TRANSACTION_TYPE;

import java.sql.ClientInfoStatus;
import java.sql.SQLException;
import java.util.Map;

import com.tsurugidb.jdbc.TsurugiJdbcProperties;
import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.property.TsurugiJdbcInternalProperties;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyBoolean;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyEnum;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyInt;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyString;
import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;

public class TsurugiJdbcConnectionProperties {

    public static TsurugiJdbcConnectionProperties of(TsurugiJdbcProperties from) {
        var properties = new TsurugiJdbcConnectionProperties();
        properties.properties.copyFrom(from.getInternalProperties());
        return properties;
    }

    private TransactionOption.Builder transactionOptionBuilder = null;

    private final TsurugiJdbcPropertyEnum<TsurugiJdbcTransactionType> transactionType = new TsurugiJdbcPropertyEnum<>(TsurugiJdbcTransactionType.class, TRANSACTION_TYPE)
            .withEventHandler(this::resetTransactionOption);
    private final TsurugiJdbcPropertyString transactionLabel = new TsurugiJdbcPropertyString(TRANSACTION_LABEL).withEventHandler(this::resetTransactionOption);
    private final TsurugiJdbcPropertyBoolean autoCommit = new TsurugiJdbcPropertyBoolean(AUTO_COMMIT);
    private final TsurugiJdbcPropertyInt beginTimeout = new TsurugiJdbcPropertyInt(BEGIN_TIMEOUT);
    private final TsurugiJdbcPropertyInt commitTimeout = new TsurugiJdbcPropertyInt(COMMIT_TIMEOUT);
    private final TsurugiJdbcPropertyInt rollbackTimeout = new TsurugiJdbcPropertyInt(ROLLBACK_TIMEOUT);

    private final TsurugiJdbcPropertyInt executeTimeout = new TsurugiJdbcPropertyInt(EXECUTE_TIMEOUT);
    private final TsurugiJdbcPropertyInt queryTimeout = new TsurugiJdbcPropertyInt(QUERY_TIMEOUT);

    private final TsurugiJdbcPropertyEnum<TsurugiJdbcShutdownType> shutdownType = new TsurugiJdbcPropertyEnum<>(TsurugiJdbcShutdownType.class, SHUTDOWN_TYPE);
    private final TsurugiJdbcPropertyInt shutdownTimeout = new TsurugiJdbcPropertyInt(SHUTDOWN_TIMEOUT);

    private final TsurugiJdbcPropertyInt defaultTimeout = new TsurugiJdbcPropertyInt(DEFAULT_TIMEOUT);

    private final TsurugiJdbcInternalProperties properties = TsurugiJdbcInternalProperties.of( //
            transactionType, transactionLabel, autoCommit, //
            beginTimeout, commitTimeout, rollbackTimeout, //
            executeTimeout, //
            queryTimeout, //
            shutdownType, shutdownTimeout, //
            defaultTimeout);

    public void put(String key, String value, Map<String, ClientInfoStatus> failedProperties) throws SQLException {
        properties.putForClient(key, value, failedProperties);
    }

    @TsurugiJdbcInternal
    public TsurugiJdbcInternalProperties getInternalProperties() {
        return this.properties;
    }

    // Transaction

    public void setTransactionType(TsurugiJdbcTransactionType type) {
        transactionType.setValue(type);
    }

    public TsurugiJdbcTransactionType getTransactionType() {
        return transactionType.value();
    }

    public void setTranactionLabel(String label) {
        transactionLabel.setStringValue(label);
    }

    private <T> void resetTransactionOption(T ignore) {
        this.transactionOptionBuilder = null;
    }

    public TransactionOption getTransactionOption() {
        if (this.transactionOptionBuilder == null) {
            var builder = TransactionOption.newBuilder();
            transactionType.ifPresent(t -> builder.setType(t.getLowTransactionType()));
            transactionLabel.ifPresent(builder::setLabel);

            this.transactionOptionBuilder = builder;
        }
        return transactionOptionBuilder.build();
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit.setValue(autoCommit);
    }

    public boolean getAutoCommit() {
        return autoCommit.value(true);
    }

    public int getBeginTimeout() {
        return beginTimeout.value().orElse(getDefaultTimeout());
    }

    public int getCommitTimeout() {
        return commitTimeout.value().orElse(getDefaultTimeout());
    }

    public int getRollbackTimeout() {
        return rollbackTimeout.value().orElse(getDefaultTimeout());
    }

    // Session

    public TsurugiJdbcShutdownType getShutdownType() {
        var type = shutdownType.value();
        if (type != null) {
            return type;
        }
        return TsurugiJdbcShutdownType.GRACEFUL;
    }

    public int getShutdownTimeout() {
        return shutdownTimeout.value().orElse(getDefaultTimeout());
    }

    // Common

    public int getDefaultTimeout() {
        return defaultTimeout.value().orElse(0);
    }
}
