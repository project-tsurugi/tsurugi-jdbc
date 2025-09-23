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
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.EXCLUSIVE_READ_AREA;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.EXECUTE_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.INCLUDE_DDL;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.INCLUSIVE_READ_AREA;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.QUERY_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.ROLLBACK_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.SCAN_PARALLEL;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.SHUTDOWN_TIMEOUT;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.SHUTDOWN_TYPE;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.TRANSACTION_LABEL;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.TRANSACTION_TYPE;
import static com.tsurugidb.jdbc.TsurugiJdbcProperties.WRITE_PRESERVE;

import java.sql.ClientInfoStatus;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.tsurugidb.jdbc.TsurugiJdbcProperties;
import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.property.TsurugiJdbcInternalProperties;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyBoolean;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyEnum;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyInt;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyString;
import com.tsurugidb.sql.proto.SqlRequest.ReadArea;
import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.WritePreserve;

public class TsurugiJdbcConnectionProperties {

    public static TsurugiJdbcConnectionProperties of(TsurugiJdbcProperties from) {
        var properties = new TsurugiJdbcConnectionProperties(from.getEndpoint());
        properties.properties.copyFrom(from.getInternalProperties());
        return properties;
    }

    private final String endpoint;
    private TransactionOption.Builder transactionOptionBuilder = null;

    private final TsurugiJdbcPropertyEnum<TsurugiJdbcTransactionType> transactionType = new TsurugiJdbcPropertyEnum<>(TsurugiJdbcTransactionType.class, TRANSACTION_TYPE)
            .changeEvent(this::resetTransactionOption);
    private final TsurugiJdbcPropertyString transactionLabel = new TsurugiJdbcPropertyString(TRANSACTION_LABEL).changeEvent(this::resetTransactionOption);
    private final TsurugiJdbcPropertyBoolean includeDdl = new TsurugiJdbcPropertyBoolean(INCLUDE_DDL).changeEvent(this::resetTransactionOption);
    private final TsurugiJdbcPropertyString writePreserve = new TsurugiJdbcPropertyString(WRITE_PRESERVE).changeEvent(this::resetTransactionOption);
    private final TsurugiJdbcPropertyString inclusiveReadArea = new TsurugiJdbcPropertyString(INCLUSIVE_READ_AREA).changeEvent(this::resetTransactionOption);
    private final TsurugiJdbcPropertyString exclusiveReadArea = new TsurugiJdbcPropertyString(EXCLUSIVE_READ_AREA).changeEvent(this::resetTransactionOption);
    private final TsurugiJdbcPropertyInt scanParallel = new TsurugiJdbcPropertyInt(SCAN_PARALLEL).changeEvent(this::resetTransactionOption);
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
            transactionType, transactionLabel, includeDdl, writePreserve, inclusiveReadArea, exclusiveReadArea, scanParallel, autoCommit, //
            beginTimeout, commitTimeout, rollbackTimeout, //
            executeTimeout, //
            queryTimeout, //
            shutdownType, shutdownTimeout, //
            defaultTimeout);

    public TsurugiJdbcConnectionProperties(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getEndpoint() {
        return this.endpoint;
    }

    public void put(String key, String value, Map<String, ClientInfoStatus> failedProperties) {
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
            if (includeDdl.value()) {
                builder.setModifiesDefinitions(true);
            }
            toStringList(writePreserve).ifPresent(list -> {
                for (String tableName : list) {
                    var wp = WritePreserve.newBuilder().setTableName(tableName).build();
                    builder.addWritePreserves(wp);
                }
            });
            toStringList(inclusiveReadArea).ifPresent(list -> {
                for (String tableName : list) {
                    var area = ReadArea.newBuilder().setTableName(tableName).build();
                    builder.addInclusiveReadAreas(area);
                }
            });
            toStringList(exclusiveReadArea).ifPresent(list -> {
                for (String tableName : list) {
                    var area = ReadArea.newBuilder().setTableName(tableName).build();
                    builder.addExclusiveReadAreas(area);
                }
            });
            scanParallel.ifPresent(builder::setScanParallel);

            this.transactionOptionBuilder = builder;
        }
        return transactionOptionBuilder.build();
    }

    protected Optional<List<String>> toStringList(TsurugiJdbcPropertyString property) {
        String value = property.value();
        if (value == null) {
            return Optional.empty();
        }

        String[] ss = value.split(",");
        List<String> list = Arrays.stream(ss).map(String::trim).collect(Collectors.toList());
        return Optional.of(list);
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit.setValue(autoCommit);
    }

    public boolean getAutoCommit() {
        return autoCommit.value();
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
