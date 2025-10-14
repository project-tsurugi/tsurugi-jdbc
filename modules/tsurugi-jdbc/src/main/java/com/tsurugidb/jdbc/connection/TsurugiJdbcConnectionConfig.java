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

import static com.tsurugidb.jdbc.TsurugiConfig.AUTO_COMMIT;
import static com.tsurugidb.jdbc.TsurugiConfig.AUTO_DISPOSE;
import static com.tsurugidb.jdbc.TsurugiConfig.BEGIN_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiConfig.COMMIT_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiConfig.COMMIT_TYPE;
import static com.tsurugidb.jdbc.TsurugiConfig.DEFAULT_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiConfig.EXCLUSIVE_READ_AREA;
import static com.tsurugidb.jdbc.TsurugiConfig.EXECUTE_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiConfig.INCLUDE_DDL;
import static com.tsurugidb.jdbc.TsurugiConfig.INCLUSIVE_READ_AREA;
import static com.tsurugidb.jdbc.TsurugiConfig.QUERY_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiConfig.ROLLBACK_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiConfig.SCAN_PARALLEL;
import static com.tsurugidb.jdbc.TsurugiConfig.SHUTDOWN_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiConfig.SHUTDOWN_TYPE;
import static com.tsurugidb.jdbc.TsurugiConfig.TRANSACTION_LABEL;
import static com.tsurugidb.jdbc.TsurugiConfig.TRANSACTION_TYPE;
import static com.tsurugidb.jdbc.TsurugiConfig.WRITE_PRESERVE;

import java.sql.ClientInfoStatus;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import javax.annotation.Nullable;

import com.tsurugidb.jdbc.TsurugiConfig;
import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.property.TsurugiJdbcProperties;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyBoolean;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyEnum;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyInt;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyString;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyStringList;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcCommitType;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcTransactionType;
import com.tsurugidb.sql.proto.SqlRequest.CommitOption;
import com.tsurugidb.sql.proto.SqlRequest.ReadArea;
import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.WritePreserve;

/**
 * Tsurugi JDBC Connection Configuration.
 */
public class TsurugiJdbcConnectionConfig {

    /**
     * Create connection configuration.
     *
     * @param from Tsurugi JDBC configuration
     * @return connection configuration
     */
    public static TsurugiJdbcConnectionConfig of(TsurugiConfig from) {
        var config = new TsurugiJdbcConnectionConfig(from.getEndpoint());
        config.properties.copyFrom(from.getInternalProperties());
        return config;
    }

    private final String endpoint;
    private TransactionOption transactionOption = null;
    private CommitOption commitOption = null;

    private final TsurugiJdbcPropertyEnum<TsurugiJdbcTransactionType> transactionType = new TsurugiJdbcPropertyEnum<>(TsurugiJdbcTransactionType.class, TRANSACTION_TYPE)
            .changeEvent(this::clearTransactionOption);
    private final TsurugiJdbcPropertyString transactionLabel = new TsurugiJdbcPropertyString(TRANSACTION_LABEL).changeEvent(this::clearTransactionOption);
    private final TsurugiJdbcPropertyBoolean includeDdl = new TsurugiJdbcPropertyBoolean(INCLUDE_DDL).changeEvent(this::clearTransactionOption);
    private final TsurugiJdbcPropertyStringList writePreserve = new TsurugiJdbcPropertyStringList(WRITE_PRESERVE).changeEvent(this::clearTransactionOption);
    private final TsurugiJdbcPropertyStringList inclusiveReadArea = new TsurugiJdbcPropertyStringList(INCLUSIVE_READ_AREA).changeEvent(this::clearTransactionOption);
    private final TsurugiJdbcPropertyStringList exclusiveReadArea = new TsurugiJdbcPropertyStringList(EXCLUSIVE_READ_AREA).changeEvent(this::clearTransactionOption);
    private final TsurugiJdbcPropertyInt scanParallel = new TsurugiJdbcPropertyInt(SCAN_PARALLEL).changeEvent(this::clearTransactionOption);
    private final TsurugiJdbcPropertyBoolean autoCommit = new TsurugiJdbcPropertyBoolean(AUTO_COMMIT);
    private final TsurugiJdbcPropertyEnum<TsurugiJdbcCommitType> commitType = new TsurugiJdbcPropertyEnum<>(TsurugiJdbcCommitType.class, COMMIT_TYPE).changeEvent(this::clearCommitOption);
    private final TsurugiJdbcPropertyBoolean autoDispose = new TsurugiJdbcPropertyBoolean(AUTO_DISPOSE).changeEvent(this::clearCommitOption);
    private final TsurugiJdbcPropertyInt beginTimeout = new TsurugiJdbcPropertyInt(BEGIN_TIMEOUT);
    private final TsurugiJdbcPropertyInt commitTimeout = new TsurugiJdbcPropertyInt(COMMIT_TIMEOUT);
    private final TsurugiJdbcPropertyInt rollbackTimeout = new TsurugiJdbcPropertyInt(ROLLBACK_TIMEOUT);

    private final TsurugiJdbcPropertyInt executeTimeout = new TsurugiJdbcPropertyInt(EXECUTE_TIMEOUT);
    private final TsurugiJdbcPropertyInt queryTimeout = new TsurugiJdbcPropertyInt(QUERY_TIMEOUT);

    private final TsurugiJdbcPropertyEnum<TsurugiJdbcShutdownType> shutdownType = new TsurugiJdbcPropertyEnum<>(TsurugiJdbcShutdownType.class, SHUTDOWN_TYPE);
    private final TsurugiJdbcPropertyInt shutdownTimeout = new TsurugiJdbcPropertyInt(SHUTDOWN_TIMEOUT);

    private final TsurugiJdbcPropertyInt defaultTimeout = new TsurugiJdbcPropertyInt(DEFAULT_TIMEOUT);

    private final TsurugiJdbcProperties properties = TsurugiJdbcProperties.of(//
            transactionType, transactionLabel, includeDdl, writePreserve, inclusiveReadArea, exclusiveReadArea, scanParallel, //
            autoCommit, commitType, autoDispose, //
            beginTimeout, commitTimeout, rollbackTimeout, //
            executeTimeout, //
            queryTimeout, //
            shutdownType, shutdownTimeout, //
            defaultTimeout);

    /**
     * Creates a new instance.
     *
     * @param endpoint endpoint
     */
    public TsurugiJdbcConnectionConfig(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Get endpoint.
     *
     * @return endpoint
     */
    public String getEndpoint() {
        return this.endpoint;
    }

    /**
     * Set client info property.
     *
     * @param key              key
     * @param value            value
     * @param failedProperties failed properties
     */
    public void put(String key, String value, Map<String, ClientInfoStatus> failedProperties) {
        properties.putForClient(key, value, failedProperties);
    }

    /**
     * Get internal properties.
     *
     * @return properties
     */
    @TsurugiJdbcInternal
    public TsurugiJdbcProperties getInternalProperties() {
        return this.properties;
    }

    // Transaction

    /**
     * Set transaction type.
     *
     * @param type transaction type
     */
    public void setTransactionType(TsurugiJdbcTransactionType type) {
        transactionType.setValue(type);
    }

    /**
     * Get transaction type.
     *
     * @return transaction type
     */
    public TsurugiJdbcTransactionType getTransactionType() {
        return transactionType.value();
    }

    /**
     * Get default transaction type.
     *
     * @return transaction type
     */
    public TsurugiJdbcTransactionType getDefaultTransactionType() {
        var type = transactionType.getDefaultValue();
        if (type != null) {
            return type;
        }
        return TsurugiJdbcTransactionType.OCC;
    }

    /**
     * Set transaction label.
     *
     * @param label transaction label
     */
    public void setTransactionLabel(String label) {
        transactionLabel.setValue(label);
    }

    /**
     * Get transaction label.
     *
     * @return transaction label
     */
    public String getTransactionLabel() {
        return transactionLabel.value();
    }

    /**
     * Set LTX include DDL.
     *
     * @param include include DDL
     */
    public void setIncludeDdl(boolean include) {
        includeDdl.setValue(include);
    }

    /**
     * Get LTX include DDL.
     *
     * @return include DDL
     */
    public boolean getIncludeDdl() {
        return includeDdl.value();
    }

    /**
     * Set LTX write preserve.
     *
     * @param tableNames table names
     */
    public void setWritePreserve(List<String> tableNames) {
        writePreserve.setValue(tableNames);
    }

    /**
     * Get LTX write preserve.
     *
     * @return table names
     */
    public @Nullable List<String> getWritePreserve() {
        return writePreserve.value();
    }

    /**
     * Set LTX inclusive read area.
     *
     * @param tableNames table names
     */
    public void setInclusiveReadArea(List<String> tableNames) {
        inclusiveReadArea.setValue(tableNames);
    }

    /**
     * Get LTX inclusive read area.
     *
     * @return table names
     */
    public @Nullable List<String> getInclusiveReadArea() {
        return inclusiveReadArea.value();
    }

    /**
     * Set LTX exclusive read area.
     *
     * @param tableNames table names
     */
    public void setExclusiveReadArea(List<String> tableNames) {
        exclusiveReadArea.setValue(tableNames);
    }

    /**
     * Get LTX exclusive read area.
     *
     * @return table names
     */
    public @Nullable List<String> getExclusiveReadArea() {
        return exclusiveReadArea.value();
    }

    /**
     * Set RTX scan parallel.
     *
     * @param parallel scan parallel
     */
    public void setScanParallel(int parallel) {
        scanParallel.setValue(parallel);
    }

    /**
     * Get RTX scan parallel.
     *
     * @return scan parallel
     */
    public OptionalInt getScanParallel() {
        return scanParallel.value();
    }

    private <T> void clearTransactionOption(T value) {
        this.transactionOption = null;
    }

    /**
     * Get transaction option.
     *
     * @return transaction option
     */
    public TransactionOption getLowTransactionOption() {
        if (this.transactionOption == null) {
            var builder = TransactionOption.newBuilder();

            transactionType.ifPresent(t -> builder.setType(t.getLowTransactionType()));
            transactionLabel.ifPresent(builder::setLabel);
            if (includeDdl.value()) {
                builder.setModifiesDefinitions(true);
            }
            writePreserve.ifPresent(list -> {
                for (String tableName : list) {
                    var wp = WritePreserve.newBuilder().setTableName(tableName).build();
                    builder.addWritePreserves(wp);
                }
            });
            inclusiveReadArea.ifPresent(list -> {
                for (String tableName : list) {
                    var area = ReadArea.newBuilder().setTableName(tableName).build();
                    builder.addInclusiveReadAreas(area);
                }
            });
            exclusiveReadArea.ifPresent(list -> {
                for (String tableName : list) {
                    var area = ReadArea.newBuilder().setTableName(tableName).build();
                    builder.addExclusiveReadAreas(area);
                }
            });
            scanParallel.ifPresent(builder::setScanParallel);

            this.transactionOption = builder.build();
        }
        return this.transactionOption;
    }

    // commit

    /**
     * Set auto commit.
     *
     * @param autoCommit auto commit
     */
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit.setValue(autoCommit);
    }

    /**
     * Get auto commit.
     *
     * @return auto commit
     */
    public boolean getAutoCommit() {
        return autoCommit.value();
    }

    /**
     * Set commit type.
     *
     * @param type commit type
     */
    public void setCommitType(TsurugiJdbcCommitType type) {
        commitType.setValue(type);
    }

    /**
     * Get commit type.
     *
     * @return commit type
     */
    public TsurugiJdbcCommitType getCommitType() {
        return commitType.value();
    }

    /**
     * Set automatically dispose upon commit.
     *
     * @param autoDispose automatically dispose
     */
    public void setAutoDispose(boolean autoDispose) {
        this.autoDispose.setValue(autoDispose);
    }

    /**
     * Get automatically dispose upon commit.
     *
     * @return automatically dispose
     */
    public boolean getAutoDispose() {
        return autoDispose.value();
    }

    private <T> void clearCommitOption(T value) {
        this.commitOption = null;
    }

    /**
     * Get commit option.
     *
     * @return commit option
     */
    public CommitOption getLowCommitOption() {
        if (this.commitOption == null) {
            var builder = CommitOption.newBuilder();

            commitType.ifPresent(t -> builder.setNotificationType(t.getLowCommitStatus()));
            builder.setAutoDispose(autoDispose.value());

            this.commitOption = builder.build();
        }
        return this.commitOption;
    }

    // transaction timeout

    /**
     * Get transaction begin timeout.
     *
     * @return begin timeout [seconds]
     */
    public int getBeginTimeout() {
        return beginTimeout.value().orElse(getDefaultTimeout());
    }

    /**
     * Get transaction commit timeout.
     *
     * @return commit timeout [seconds]
     */
    public int getCommitTimeout() {
        return commitTimeout.value().orElse(getDefaultTimeout());
    }

    /**
     * Get transaction rollback timeout.
     *
     * @return rollback timeout [seconds]
     */
    public int getRollbackTimeout() {
        return rollbackTimeout.value().orElse(getDefaultTimeout());
    }

    // Session

    /**
     * Get session shutdown type.
     *
     * @return shutdown type
     */
    public TsurugiJdbcShutdownType getShutdownType() {
        return shutdownType.value();
    }

    /**
     * Get session shutdown timeout.
     *
     * @return shutdown timeout [seconds]
     */
    public int getShutdownTimeout() {
        return shutdownTimeout.value().orElse(getDefaultTimeout());
    }

    // Common

    /**
     * Set default timeout.
     *
     * @param timeout default timeout [seconds]
     */
    public void setDefaultTimeout(int timeout) {
        defaultTimeout.setValue(timeout);
    }

    /**
     * Get default timeout.
     *
     * @return default timeout [seconds]
     */
    public int getDefaultTimeout() {
        return defaultTimeout.value().orElse(0);
    }
}
