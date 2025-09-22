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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.tsurugidb.jdbc.TsurugiDriver;
import com.tsurugidb.jdbc.annotation.TsurugiJdbcNotSupported;
import com.tsurugidb.jdbc.driver.TsurugiJdbcUrlParser;
import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.factory.GetFactory;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.resultset.FixedResultSet;
import com.tsurugidb.jdbc.resultset.FixedResultSetColumn;
import com.tsurugidb.jdbc.util.TableNameMatcher;
import com.tsurugidb.jdbc.util.TsurugiJdbcSqlTypeUtil;
import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.tsubakuro.sql.exception.TargetNotFoundException;

public class TsurugiJdbcDatabaseMetaData implements DatabaseMetaData, GetFactory {

    private final TsurugiJdbcConnection ownerConnection;

    public TsurugiJdbcDatabaseMetaData(TsurugiJdbcConnection ownerConnection) {
        this.ownerConnection = ownerConnection;
    }

    @Override
    public TsurugiJdbcFactory getFactory() {
        return ownerConnection.getFactory();
    }

    protected TsurugiJdbcExceptionHandler getExceptionHandler() {
        return getFactory().getExceptionHandler();
    }

    protected TsurugiJdbcConnectionProperties getProperties() {
        return ownerConnection.getProperties();
    }

    protected TsurugiJdbcSqlTypeUtil getSqlTypeUtil() {
        return getFactory().getSqlTypeUtil();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException e) {
            throw getExceptionHandler().unwrapException(iface);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        String endpoint = ownerConnection.getProperties().getEndpoint();
        return TsurugiJdbcUrlParser.getJdbcUrl(endpoint);
    }

    @Override
    public String getUserName() throws SQLException {
        var lowSession = ownerConnection.getLowSession();
        int timeout = getProperties().getDefaultTimeout();
        try {
            Optional<String> userName = lowSession.getUserName().await(timeout, TimeUnit.SECONDS);
            return userName.orElse(null);
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("getUserName error", e);
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "Tsurugi";
    }

    @Override
    @TsurugiJdbcNotSupported
    public String getDatabaseProductVersion() throws SQLException {
        // FIXME DBのバージョンを取得する
        return TsurugiDriver.TSURUGI_VERSION;
    }

    @Override
    public String getDriverName() throws SQLException {
        return TsurugiDriver.DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return TsurugiDriver.DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return TsurugiDriver.DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getDriverMinorVersion() {
        return TsurugiDriver.DRIVER_VERSION_MINAR;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return ""; // FIXME getSQLKeywords()
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return ""; // FIXME getNumericFunctions()
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return ""; // FIXME getStringFunctions()
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return ""; // FIXME getSystemFunctions()
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return ""; // FIXME getTimeDateFunctions()
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "database";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return true;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 60;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_SERIALIZABLE;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return level == Connection.TRANSACTION_SERIALIZABLE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return true;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getProcedures not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getProcedureColumns not supported");
    }

    private static final List<FixedResultSetColumn> TABLES_COLUMN_LIST = List.of( //
            FixedResultSetColumn.ofStringNullable("TABLE_CAT"), //
            FixedResultSetColumn.ofStringNullable("TABLE_SCHEM"), //
            FixedResultSetColumn.ofString("TABLE_NAME"), //
            FixedResultSetColumn.ofString("TABLE_TYPE"), //
            FixedResultSetColumn.ofStringNullable("REMARKS"), //
            FixedResultSetColumn.ofStringNullable("TYPE_CAT"), //
            FixedResultSetColumn.ofStringNullable("TYPE_SCHEM"), //
            FixedResultSetColumn.ofStringNullable("TYPE_NAME"), //
            FixedResultSetColumn.ofStringNullable("SELF_REFERENCING_COL_NAME"), //
            FixedResultSetColumn.ofStringNullable("REF_GENERATION") //
    );

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        try {
            var matcher = TableNameMatcher.of(catalog, schemaPattern, tableNamePattern, types);

            var lowSqlClient = ownerConnection.getLowSqlClient();

            int timeout = ownerConnection.getProperties().getDefaultTimeout();
            var lowTableList = lowSqlClient.listTables().await(timeout, TimeUnit.SECONDS);
            List<String> tableNames = lowTableList.getTableNames();

            var valuesList = new ArrayList<Object[]>(tableNames.size());
            for (String tableName : tableNames) {
                if (!matcher.matches(tableName, "TABLE")) {
                    continue;
                }
                Object[] values = { //
                        null, // TABLE_CAT
                        null, // TABLE_SCHEM
                        tableName, // TABLE_NAME
                        "TABLE", // TABLE_TYPE
                        null, // REMARKS
                        null, // TYPE_CAT
                        null, // TYPE_SCHEM
                        null, // TYPE_NAME
                        null, // SELF_REFERENCING_COL_NAME
                        null, // REF_GENERATION
                };
                valuesList.add(values);
            }

            return new FixedResultSet(this, TABLES_COLUMN_LIST, valuesList);
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("getTables error", e);
        }
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getSchemas() throws SQLException {
        throw new SQLFeatureNotSupportedException("getSchemas not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getCatalogs() throws SQLException {
        throw new SQLFeatureNotSupportedException("getCatalogs not supported");
    }

    private static final List<FixedResultSetColumn> TABLE_TYPES_COLUMN_LIST = List.of( //
            FixedResultSetColumn.ofString("TABLE_TYPE") //
    );

    @Override
    public ResultSet getTableTypes() throws SQLException {
        Object[] values = { "TABLE" };
        var valuesList = List.<Object[]>of(values);
        return new FixedResultSet(this, TABLE_TYPES_COLUMN_LIST, valuesList);
    }

    private static final List<FixedResultSetColumn> COLUMNS_COLUMN_LIST = List.of( //
            FixedResultSetColumn.ofStringNullable("TABLE_CAT"), //
            FixedResultSetColumn.ofStringNullable("TABLE_SCHEM"), //
            FixedResultSetColumn.ofString("TABLE_NAME"), //
            FixedResultSetColumn.ofString("COLUMN_NAME"), //
            FixedResultSetColumn.ofInt("DATA_TYPE"), //
            FixedResultSetColumn.ofString("TYPE_NAME"), //
            FixedResultSetColumn.ofInt("COLUMN_SIZE"), //
            FixedResultSetColumn.ofString("BUFFER_LENGTH"), //
            FixedResultSetColumn.ofIntNullable("DECIMAL_DIGITS"), //
            FixedResultSetColumn.ofInt("NUM_PREC_RADIX"), //
            FixedResultSetColumn.ofInt("NULLABLE"), //
            FixedResultSetColumn.ofStringNullable("REMARKS"), //
            FixedResultSetColumn.ofStringNullable("COLUMN_DEF"), //
            FixedResultSetColumn.ofInt("SQL_DATA_TYPE"), //
            FixedResultSetColumn.ofInt("SQL_DATETIME_SUB"), //
            FixedResultSetColumn.ofInt("CHAR_OCTET_LENGTH"), //
            FixedResultSetColumn.ofInt("ORDINAL_POSITION"), //
            FixedResultSetColumn.ofString("IS_NULLABLE"), //
            FixedResultSetColumn.ofIntNullable("SCOPE_CATALOG"), //
            FixedResultSetColumn.ofIntNullable("SCOPE_SCHEMA"), //
            FixedResultSetColumn.ofIntNullable("SCOPE_TABLE"), //
            FixedResultSetColumn.ofShortNullable("SOURCE_DATA_TYPE"), //
            FixedResultSetColumn.ofString("IS_AUTOINCREMENT"), //
            FixedResultSetColumn.ofString("IS_GENERATEDCOLUMN") //
    );

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        var valuesList = new ArrayList<Object[]>();
        try {
            var matcher = TableNameMatcher.of(catalog, schemaPattern, tableNamePattern, null).columnNamePattern(columnNamePattern);
            var util = getSqlTypeUtil();

            var lowSqlClient = ownerConnection.getLowSqlClient();

            int timeout = ownerConnection.getProperties().getDefaultTimeout();
            var lowTableList = lowSqlClient.listTables().await(timeout, TimeUnit.SECONDS);
            List<String> tableNames = lowTableList.getTableNames();

            for (String tableName : tableNames) {
                if (!matcher.matches(tableName, "TABLE")) {
                    continue;
                }

                var lowMetadata = lowSqlClient.getTableMetadata(tableName).await(timeout, TimeUnit.SECONDS);
                var lowColumnList = lowMetadata.getColumns();
                int position = 1;
                for (var lowColumn : lowColumnList) {
                    String columnName = lowColumn.getName();
                    JDBCType jdbcType = util.toJdbcType(lowColumn);
                    String typeName = util.toSqlTypeName(lowColumn);
                    var nullableOpt = util.findNullable(lowColumn);
                    int nullable;
                    String isNullable;
                    if (nullableOpt.isPresent()) {
                        if (nullableOpt.get()) {
                            nullable = columnNullable;
                            isNullable = "YES";
                        } else {
                            nullable = columnNoNulls;
                            isNullable = "NO";
                        }
                    } else {
                        nullable = columnNullableUnknown;
                        isNullable = "";
                    }
                    int length = util.getLength(lowColumn);

                    if (matcher.matchesColumnName(columnName)) {
                        Object[] values = { //
                                lowMetadata.getDatabaseName().orElse(null), // TABLE_CAT
                                lowMetadata.getSchemaName().orElse(null), // TABLE_SCHEM
                                tableName, // TABLE_NAME
                                columnName, // COLUMN_NAME
                                jdbcType.getVendorTypeNumber(), // DATA_TYPE
                                typeName, // TYPE_NAME
                                length, // COLUMN_SIZE
                                null, // BUFFER_LENGTH
                                null, // DECIMAL_DIGITS
                                10, // NUM_PREC_RADIX
                                nullable, // NULLABLE
                                lowColumn.getDescription(), // REMARKS
                                null, // COLUMN_DEF
                                null, // SQL_DATA_TYPE
                                null, // SQL_DATETIME_SUB
                                length, // CHAR_OCTET_LENGTH
                                position, // ORDINAL_POSITION
                                isNullable, // IS_NULLABLE
                                null, // SCOPE_CATALOG
                                null, // SCOPE_SCHEMA
                                null, // SCOPE_TABLE
                                null, // SOURCE_DATA_TYPE
                                "", // IS_AUTOINCREMENT
                                "", // IS_GENERATEDCOLUMN
                        };
                        valuesList.add(values);
                    }
                    position++;
                }
            }
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("getColumns error", e);
        }
        return new FixedResultSet(this, COLUMNS_COLUMN_LIST, valuesList);
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getColumnPrivileges not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTablePrivileges not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBestRowIdentifier not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("getVersionColumns not supported");
    }

    private static final List<FixedResultSetColumn> PRIMARY_KEYS_COLUMN_LIST = List.of( //
            FixedResultSetColumn.ofStringNullable("TABLE_CAT"), //
            FixedResultSetColumn.ofStringNullable("TABLE_SCHEM"), //
            FixedResultSetColumn.ofString("TABLE_NAME"), //
            FixedResultSetColumn.ofString("COLUMN_NAME"), //
            FixedResultSetColumn.ofShort("KEY_SEQ"), //
            FixedResultSetColumn.ofStringNullable("PK_NAME") //
    );

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        // FIXME getPrimaryKeys(): use catalog, schema
        try {
            var lowSqlClient = ownerConnection.getLowSqlClient();

            TableMetadata lowTableMetadata;
            try {
                int timeout = ownerConnection.getProperties().getDefaultTimeout();
                lowTableMetadata = lowSqlClient.getTableMetadata(table).await(timeout, TimeUnit.SECONDS);
            } catch (TargetNotFoundException e) {
                return new FixedResultSet(this, PRIMARY_KEYS_COLUMN_LIST, List.of());
            }
            List<String> primaryKeys = lowTableMetadata.getPrimaryKeys();

            var valuesList = new ArrayList<Object[]>(primaryKeys.size());
            short keySeq = 1;
            for (String keyName : primaryKeys) {
                Object[] values = { //
                        lowTableMetadata.getDatabaseName().orElse(null), // TABLE_CAT
                        lowTableMetadata.getSchemaName().orElse(null), // TABLE_SCHEM
                        lowTableMetadata.getTableName(), // TABLE_NAME
                        keyName, // COLUMN_NAME
                        keySeq++, // KEY_SEQ
                        null, // PK_NAME
                };
                valuesList.add(values);
            }

            return new FixedResultSet(this, PRIMARY_KEYS_COLUMN_LIST, valuesList);
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("getTables error", e);
        }
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("getImportedKeys not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("getExportedKeys not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        throw new SQLFeatureNotSupportedException("getCrossReference not supported");
    }

    private static final List<FixedResultSetColumn> TYPE_INFO_COLUMN_LIST = List.of( //
            FixedResultSetColumn.ofString("TYPE_NAME"), //
            FixedResultSetColumn.ofInt("DATA_TYPE"), //
            FixedResultSetColumn.ofInt("PRECISION"), //
            FixedResultSetColumn.ofStringNullable("LITERAL_PREFIX"), //
            FixedResultSetColumn.ofStringNullable("LITERAL_SUFFIX"), //
            FixedResultSetColumn.ofShort("NULLABLE"), //
            FixedResultSetColumn.ofBoolean("CASE_SENSITIVE"), //
            FixedResultSetColumn.ofShort("SEARCHABLE"), //
            FixedResultSetColumn.ofBoolean("UNSIGNED_ATTRIBUTE"), //
            FixedResultSetColumn.ofBoolean("FIXED_PREC_SCALE"), //
            FixedResultSetColumn.ofBoolean("AUTO_INCREMENT"), //
            FixedResultSetColumn.ofStringNullable("LOCAL_TYPE_NAME"), //
            FixedResultSetColumn.ofShort("MINIMUM_SCALE"), //
            FixedResultSetColumn.ofShort("MAXIMUM_SCALE"), //
            FixedResultSetColumn.ofInt("SQL_DATA_TYPE"), //
            FixedResultSetColumn.ofInt("SQL_DATETIME_SUB"), //
            FixedResultSetColumn.ofInt("NUM_PREC_RADIX") //
    );

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return new FixedResultSet(this, TYPE_INFO_COLUMN_LIST, TsurugiJdbcSqlTypeUtil.TYPE_INFO_VALUES_LIST);
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        throw new SQLFeatureNotSupportedException("getIndexInfo not supported");
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return supportsResultSetType(type) && (concurrency == ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        throw new SQLFeatureNotSupportedException("getUDTs not supported");
    }

    @Override
    public TsurugiJdbcConnection getConnection() throws SQLException {
        return this.ownerConnection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSuperTypes not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSuperTables not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getAttributes not supported");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        String[] ss = TsurugiDriver.TSURUGI_VERSION.split(Pattern.quote("."));
        return Integer.parseInt(ss[0]);
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        String[] ss = TsurugiDriver.TSURUGI_VERSION.split(Pattern.quote("."));
        return Integer.parseInt(ss[1]);
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 3;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }

    @Override
    @TsurugiJdbcNotSupported
    public boolean locatorsUpdateCopy() throws SQLException {
        throw new SQLFeatureNotSupportedException("locatorsUpdateCopy not supported");
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSchemas not supported");
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return true;
    }

    private static final List<FixedResultSetColumn> PROPERTIES_COLUMN_LIST = List.of( //
            FixedResultSetColumn.ofString("NAME"), //
            FixedResultSetColumn.ofInt("MAX_LEN"), //
            FixedResultSetColumn.ofStringNullable("DEFAULT_VALUE"), //
            FixedResultSetColumn.ofStringNullable("DESCRIPTION") //
    );

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        var properties = ownerConnection.getProperties().getInternalProperties().getProperties();
        var valuesList = new ArrayList<Object[]>(properties.size());
        for (var property : properties) {
            Object[] values = { //
                    property.name(), // NAME
                    0, // MAX_LEN
                    property.getStringDefaultValue(), // DEFAULT_VALUE
                    property.description(), // DESCRIPTION
            };
            valuesList.add(values);
        }
        return new FixedResultSet(this, PROPERTIES_COLUMN_LIST, valuesList);
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getFunctions not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("getFunctions not supported");
    }

    private static final List<FixedResultSetColumn> PSEUDO_COLUMNS_COLUMN_LIST = List.of( //
            FixedResultSetColumn.ofStringNullable("TABLE_CAT"), //
            FixedResultSetColumn.ofStringNullable("TABLE_SCHEM"), //
            FixedResultSetColumn.ofString("TABLE_NAME"), //
            FixedResultSetColumn.ofString("COLUMN_NAME"), //
            FixedResultSetColumn.ofInt("DATA_TYPE"), //
            FixedResultSetColumn.ofInt("COLUMN_SIZE"), //
            FixedResultSetColumn.ofIntNullable("DECIMAL_DIGITS"), //
            FixedResultSetColumn.ofInt("NUM_PREC_RADIX"), //
            FixedResultSetColumn.ofString("COLUMN_USAGE"), //
            FixedResultSetColumn.ofStringNullable("REMARKS"), //
            FixedResultSetColumn.ofInt("CHAR_OCTET_LENGTH"), //
            FixedResultSetColumn.ofString("IS_NULLABLE") //
    );

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        List<Object[]> valuesList = List.of();
        return new FixedResultSet(this, PSEUDO_COLUMNS_COLUMN_LIST, valuesList);
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }
}
