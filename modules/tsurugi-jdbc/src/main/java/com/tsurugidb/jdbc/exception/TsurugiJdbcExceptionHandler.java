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
package com.tsurugidb.jdbc.exception;

import java.io.IOException;
import java.sql.ClientInfoStatus;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.exception.CcException;

public class TsurugiJdbcExceptionHandler {

    public SQLException sqlException(String baseMessage, Exception e) {

        if (e instanceof IOException) {
            return sqlException(baseMessage, (IOException) e);
        }
        if (e instanceof ServerException) {
            return sqlException(baseMessage, (ServerException) e);
        }
        if (e instanceof TimeoutException) {
            return sqlException(baseMessage, (TimeoutException) e);
        }
        if (e instanceof InterruptedException) {
            return sqlException(baseMessage, (InterruptedException) e);
        }

        String message = message(baseMessage, e);
        return new SQLException(message, e);
    }

    protected String message(String baseMessage, Exception e) {
        String causeMessage = e.getMessage();
        if (causeMessage != null && !causeMessage.isEmpty()) {
            baseMessage += " (" + causeMessage + ")";
        }
        return baseMessage;
    }

    public SQLException sqlException(String baseMessage, IOException e) {
        String message = message(baseMessage, e);
        return new SQLException(message, e);
    }

    public SQLException sqlException(String baseMessage, InterruptedException e) {
        String message = message(baseMessage, e);
        return new SQLException(message, e);
    }

    public SQLException sqlException(String baseMessage, TimeoutException e) {
        String message = message(baseMessage, e);
        return new SQLTimeoutException(message, e);
    }

    public SQLException sqlException(String baseMessage, ServerException e) {
        var diagnosticCode = e.getDiagnosticCode();

        int prefix = 0;
        int category = serverErrorCategory(diagnosticCode);
        int codeNumber = diagnosticCode.getCodeNumber();
        int vendorCode = vendorCode(prefix, category, codeNumber);

        return sqlException(baseMessage, vendorCode, e);
    }

    protected int serverErrorCategory(DiagnosticCode code) {
        String structuredCode = code.getStructuredCode();
        int n = structuredCode.indexOf('-');
        String category = structuredCode.substring(0, n);

        switch (category) {
        case "SCD":
            return 1;
        case "SQL":
            return 2;
        default: // unknown
            return 99;
        }
    }

    protected int vendorCode(int prefix, int category, int codeNumber) {
        return (prefix * 100 + category) * 100_000 + codeNumber;
    }

    protected SQLException sqlException(String baseMessage, int vendorCode, ServerException e) {
        String message = String.format("%s. %s: %s", baseMessage, e.getDiagnosticCode(), e.getMessage());

        if (e instanceof CcException) {
            var state = SqlState.S40001_SERIALIZATION_FAILURE;
            return new SQLTransactionRollbackException(message, state.code(), vendorCode, e);
        }

        // TODO convert ServerException to SQLException

        return new SQLException(message, null, vendorCode, e);
    }

    // Common

    public SQLException unwrapException(Class<?> iface) {
        String message = MessageFormat.format("Not a wrapper for {0}", iface.getName());
        return new SQLException(message, SqlState.HY000_CLI_SPECIFIC_CONDITION.code());
    }

    public SQLException propertyConvertException(String key, Exception e) {
        String message = message(MessageFormat.format("Invalid value. key={0}", key), e);
        var state = SqlState.HY024_INVALID_ATTRIBUTE_VALUE;
        return new SQLException(message, state.code(), e);
    }

    // Driver

    public SQLException jdbcUrlNullException() {
        String message = "url is null";
        return new SQLNonTransientConnectionException(message, SqlState.S08001_UNABLE_TO_CONNECTION.code());
    }

    public SQLException credentialFileLoadException(IOException e) {
        String message = message("Credential file load error", e);
        return new SQLInvalidAuthorizationSpecException(message, SqlState.S28000_INVALID_AUTHORIZATION_SPECIFICATION.code(), e);
    }

    // Connection

    public SQLClientInfoException clientInfoException(Exception e, Map<String, ClientInfoStatus> failedProperties) {
        String message = message("setClientInfo error", e);
        return new SQLClientInfoException(message, failedProperties, e);
    }

    // Transaction

    public SQLException transactionNotFoundException() {
        String message = "Transaction not found";
        return new SQLException(message, SqlState.S25000_INVALID_TRANSACTION_STATE.code());
    }

    public SQLException transactionFoundException() {
        String message = "Transaction exists";
        return new SQLException(message, SqlState.S25001_ACTIVE_TRANSACTION.code());
    }
}
