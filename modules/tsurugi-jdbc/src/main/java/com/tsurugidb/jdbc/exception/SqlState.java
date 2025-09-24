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

public enum SqlState {
    /** 08001: SQL-client unable to establish SQL-connection */
    S08001_UNABLE_TO_CONNECTION("08001", "SQL-client unable to establish SQL-connection"),

    /** 22002: null value, no indicator parameter */
    S22002_NULL_VALUE_NO_INDICATOR_PARAMETER("22002", "null value, no indicator parameter"),
    /** 22005: error in assignment */
    S22005_ERRPR_IN_ASSIGNMENT("22005", "error in assignment"),
    /** 2200G: most specific type mismatch */
    S2200G_TYPE_MISMATCH("2200G", "most specific type mismatch"),

    /** 23000: integrity constraint violation */
    S23000_INTEGRITY_CONSTRAINT_VIOLATION("23000", "integrity constraint violation"),

    /** 25000: invalid transaction state */
    S25000_INVALID_TRANSACTION_STATE("25000", "invalid transaction state"),
    /** 25001: active SQL-transaction */
    S25001_ACTIVE_TRANSACTION("25001", "active SQL-transaction"),

    /** 28000: invalid authorization specification */
    S28000_INVALID_AUTHORIZATION_SPECIFICATION("28000", "invalid authorization specification"),

    /** 40001: serialization failure */
    S40001_SERIALIZATION_FAILURE("40001", "serialization failure"),

    /** 42000: syntax error or access rule violation */
    S42000_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION("42000", "syntax error or access rule violation"),
    /** 42703: undefined column name */
    S42703_UNDEFINED_COLUMN_NAME("42703", "undefined column name"),

    /** HY000: CLI-specific condition */
    HY000_CLI_SPECIFIC_CONDITION("HY000", "CLI-specific condition"),
    /** HY024: invalid attribute value */
    HY024_INVALID_ATTRIBUTE_VALUE("HY024", "invalid attribute value"),

    ;

    private final String code;
    private final String message;

    SqlState(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public String code() {
        return this.code;
    }

    public String message() {
        return this.message;
    }
}
