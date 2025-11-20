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
package com.tsurugidb.hibernate;

import static org.hibernate.internal.util.JdbcExceptionHelper.extractSqlState;
import static org.hibernate.type.SqlTypes.BINARY;
import static org.hibernate.type.SqlTypes.CHAR;
import static org.hibernate.type.SqlTypes.CLOB;
import static org.hibernate.type.SqlTypes.DATE;
import static org.hibernate.type.SqlTypes.DOUBLE;
import static org.hibernate.type.SqlTypes.FLOAT;
import static org.hibernate.type.SqlTypes.LONG32NVARCHAR;
import static org.hibernate.type.SqlTypes.LONG32VARBINARY;
import static org.hibernate.type.SqlTypes.LONG32VARCHAR;
import static org.hibernate.type.SqlTypes.NCHAR;
import static org.hibernate.type.SqlTypes.NCLOB;
import static org.hibernate.type.SqlTypes.NVARCHAR;
import static org.hibernate.type.SqlTypes.TIME;
import static org.hibernate.type.SqlTypes.TIMESTAMP;
import static org.hibernate.type.SqlTypes.TIMESTAMP_UTC;
import static org.hibernate.type.SqlTypes.TIMESTAMP_WITH_TIMEZONE;
import static org.hibernate.type.SqlTypes.TIME_UTC;
import static org.hibernate.type.SqlTypes.TIME_WITH_TIMEZONE;
import static org.hibernate.type.SqlTypes.VARBINARY;
import static org.hibernate.type.SqlTypes.VARCHAR;
import static org.hibernate.type.descriptor.DateTimeUtils.appendAsDate;
import static org.hibernate.type.descriptor.DateTimeUtils.appendAsLocalTime;
import static org.hibernate.type.descriptor.DateTimeUtils.appendAsTime;
import static org.hibernate.type.descriptor.DateTimeUtils.appendAsTimestampWithMicros;
import static org.hibernate.type.descriptor.DateTimeUtils.appendAsTimestampWithMillis;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.hibernate.PessimisticLockException;
import org.hibernate.boot.model.FunctionContributions;
import org.hibernate.boot.model.TypeContributions;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.NationalizationSupport;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.dialect.Replacer;
import org.hibernate.dialect.SelectItemReferenceStrategy;
import org.hibernate.dialect.TimeZoneSupport;
import org.hibernate.dialect.aggregate.AggregateSupport;
import org.hibernate.dialect.aggregate.PostgreSQLAggregateSupport;
import org.hibernate.dialect.function.CommonFunctionFactory;
import org.hibernate.dialect.identity.IdentityColumnSupport;
import org.hibernate.dialect.identity.PostgreSQLIdentityColumnSupport;
import org.hibernate.dialect.lock.internal.PostgreSQLLockingSupport;
import org.hibernate.dialect.lock.spi.LockingSupport;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.temptable.TemporaryTableStrategy;
import org.hibernate.engine.jdbc.env.spi.IdentifierCaseStrategy;
import org.hibernate.engine.jdbc.env.spi.IdentifierHelper;
import org.hibernate.engine.jdbc.env.spi.IdentifierHelperBuilder;
import org.hibernate.engine.jdbc.env.spi.NameQualifierSupport;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.exception.spi.SQLExceptionConversionDelegate;
import org.hibernate.mapping.Table;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.sql.ast.spi.StandardSqlAstTranslatorFactory;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.exec.spi.JdbcOperation;
import org.hibernate.tool.schema.spi.Exporter;
import org.hibernate.type.descriptor.jdbc.BlobJdbcType;
import org.hibernate.type.descriptor.jdbc.ClobJdbcType;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.spi.JdbcTypeRegistry;
import org.hibernate.type.spi.TypeConfiguration;

import com.tsurugidb.hibernate.type.TsurugiDateJdbcType;
import com.tsurugidb.hibernate.type.TsurugiTimeJdbcType;
import com.tsurugidb.hibernate.type.TsurugiTimeUtcJdbcType;
import com.tsurugidb.hibernate.type.TsurugiTimestampJdbcType;
import com.tsurugidb.hibernate.type.TsurugiTimestampUtcJdbcType;

import jakarta.persistence.GenerationType;
import jakarta.persistence.TemporalType;
import jakarta.persistence.Timeout;

/**
 * Tsurugi Dialect.
 */
@SuppressWarnings("deprecation")
public class TsurugiDialect extends Dialect {

    private static final DatabaseVersion MINIMUM_VERSION;
    static {
        var minimumTsurugiVersion = "1.8.0";
        String[] ss = minimumTsurugiVersion.split(Pattern.quote("."));
        int major = Integer.parseInt(ss[0]);
        int minor = Integer.parseInt(ss[1]);
        int micro = Integer.parseInt(ss[2]);
        MINIMUM_VERSION = DatabaseVersion.make(major, minor, micro);
    }

    @Override
    protected String columnType(int sqlTypeCode) {
        return switch (sqlTypeCode) {
        case FLOAT -> "float";
        case DOUBLE -> "double";
        case NCHAR -> super.columnType(CHAR);
        case LONG32VARCHAR, LONG32NVARCHAR, NVARCHAR -> super.columnType(VARCHAR);
        case DATE -> "date";
        case TIME -> "time";
        case TIMESTAMP -> "timestamp";
        case TIME_WITH_TIMEZONE, TIME_UTC -> "time with time zone";
        case TIMESTAMP_WITH_TIMEZONE, TIMESTAMP_UTC -> "timestamp with time zone";
        case NCLOB -> super.columnType(CLOB);
        case LONG32VARBINARY -> super.columnType(VARBINARY);
        default -> super.columnType(sqlTypeCode);
        };
    }

    @Override
    protected String castType(int sqlTypeCode) {
        return switch (sqlTypeCode) {
        case CHAR, NCHAR, VARCHAR, NVARCHAR, LONG32VARCHAR, LONG32NVARCHAR -> "varchar";
        case BINARY, VARBINARY, LONG32VARBINARY -> "varbinary";
        default -> super.castType(sqlTypeCode);
        };
    }

    @Override
    protected DatabaseVersion getMinimumSupportedVersion() {
        return MINIMUM_VERSION;
    }

    @Override
    protected Integer resolveSqlTypeCode(String columnTypeName, TypeConfiguration typeConfiguration) {
        return switch (columnTypeName) {
        case "character" -> Types.CHAR;
        case "char varying", "character varying" -> Types.VARCHAR;
        case "binary varying" -> Types.VARBINARY;
        case "binary large object" -> Types.BLOB;
        case "char large object", "character large object" -> Types.CLOB;
        default -> super.resolveSqlTypeCode(columnTypeName, typeConfiguration);
        };
    }

    @Override
    public JdbcType resolveSqlTypeDescriptor(String columnTypeName, int jdbcTypeCode, int precision, int scale, JdbcTypeRegistry jdbcTypeRegistry) {
        return jdbcTypeRegistry.getDescriptor(jdbcTypeCode);
    }

    @Override
    public void initializeFunctionRegistry(FunctionContributions functionContributions) {
        super.initializeFunctionRegistry(functionContributions);

        final var functionFactory = new CommonFunctionFactory(functionContributions);

        functionFactory.ceiling_ceil();
        functionFactory.concat_pipeOperator(); // `concat` function of the Criteria API uses `||`
        functionFactory.length_characterLength();
        functionFactory.localtimeLocaltimestamp();
        functionFactory.lowerUpper();
        functionFactory.mod_operator();
        functionFactory.octetLength();
        functionFactory.position();
        functionFactory.round();
        functionFactory.substr();
        functionFactory.substringFromFor();
    }

    @Override
    public String currentTime() {
        return "localtime";
    }

    @Override
    public String currentTimestamp() {
        return "localtimestamp";
    }

    @Override
    public String currentTimestampWithTimeZone() {
        return "current_timestamp";
    }

    @Override
    public int getDefaultStatementBatchSize() {
        return 15;
    }

    @Override
    public boolean getDefaultNonContextualLobCreation() {
        return true;
    }

    @Override
    public void contributeTypes(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
        super.contributeTypes(typeContributions, serviceRegistry);
        contributeTsurugiTypes(typeContributions, serviceRegistry);
    }

    /**
     * contribute Tsurugi types.
     *
     * @param typeContributions Callback to contribute the types
     * @param serviceRegistry   The service registry
     */
    protected void contributeTsurugiTypes(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
        JdbcTypeRegistry jdbcTypeRegistry = typeContributions.getTypeConfiguration().getJdbcTypeRegistry();

        jdbcTypeRegistry.addDescriptor(TsurugiDateJdbcType.INSTANCE);
        jdbcTypeRegistry.addDescriptor(TsurugiTimeJdbcType.INSTANCE);
        jdbcTypeRegistry.addDescriptor(TsurugiTimestampJdbcType.INSTANCE);
        jdbcTypeRegistry.addDescriptor(TsurugiTimeUtcJdbcType.INSTANCE);
        jdbcTypeRegistry.addDescriptor(TsurugiTimestampUtcJdbcType.INSTANCE);

        jdbcTypeRegistry.addDescriptor(Types.BLOB, BlobJdbcType.BLOB_BINDING);
        jdbcTypeRegistry.addDescriptor(Types.CLOB, ClobJdbcType.CLOB_BINDING);
    }

    @Override
    public GenerationType getNativeValueGenerationStrategy() {
        return GenerationType.IDENTITY;
    }

    @Override
    public IdentityColumnSupport getIdentityColumnSupport() {
        // TODO getIdentityColumnSupport
        return PostgreSQLIdentityColumnSupport.INSTANCE;
    }

    @Override
    public LimitHandler getLimitHandler() {
        return TsurugiLimitHandler.INSTANCE;
    }

    @Override
    public LockingSupport getLockingSupport() {
        // TODO LockingSupport
        return PostgreSQLLockingSupport.LOCKING_SUPPORT;
    }

    @Override
    public String getForUpdateString() {
        return "";
    }

    @Override
    public String getWriteLockString(Timeout timeout) {
        return "";
    }

    @Override
    public String getWriteLockString(int timeout) {
        return "";
    }

    @Override
    public boolean supportsIfExistsBeforeTableName() {
        return true;
    }

    @Override
    public boolean qualifyIndexName() {
        return false;
    }

    @Override
    public boolean supportsCurrentTimestampSelection() {
        return true;
    }

    @Override
    public boolean isCurrentTimestampSelectStringCallable() {
        return false;
    }

    @Override
    public SQLExceptionConversionDelegate buildSQLExceptionConversionDelegate() {
        return (sqlException, message, sql) -> {
            String sqlState = extractSqlState(sqlException);
            if (sqlState == null) {
                return null;
            }
            switch (sqlState) {
            case "40001": // serialization failure
                return new PessimisticLockException(message, sqlException, sql);
            default:
                return null;
            }
        };
    }

    @Override
    public boolean supportsIsTrue() {
        return true;
    }

    @Override
    public void appendBooleanValueString(SqlAppender appender, boolean bool) {
        appender.appendSql(bool);
    }

    @Override
    public IdentifierHelper buildIdentifierHelper(IdentifierHelperBuilder builder, DatabaseMetaData metadata) throws SQLException {
        builder.setUnquotedCaseStrategy(IdentifierCaseStrategy.MIXED);
        builder.setQuotedCaseStrategy(IdentifierCaseStrategy.MIXED);
        builder.applyReservedWords(getKeywords());
        builder.setNameQualifierSupport(getNameQualifierSupport());
        return builder.build();
    }

    @Override
    public Exporter<Table> getTableExporter() {
        // TODO getTableExporter
        return super.getTableExporter();
    }

    @Override
    public TemporaryTableStrategy getLocalTemporaryTableStrategy() {
        return null;
    }

    @Override
    public String quoteCollation(String collation) {
        return '\"' + collation + '\"';
    }

    @Override
    public boolean useConnectionToCreateLob() {
        return true;
    }

    @Override
    public boolean supportsOrdinalSelectItemReference() {
        return false;
    }

    @Override
    public boolean supportsLobValueChangePropagation() {
        return false;
    }

    @Override
    public boolean supportsUnboundedLobLocatorMaterialization() {
        return false;
    }

    @Override
    public NameQualifierSupport getNameQualifierSupport() {
        return null;
    }

    @Override
    public NationalizationSupport getNationalizationSupport() {
        return NationalizationSupport.IMPLICIT;
    }

    @Override
    public AggregateSupport getAggregateSupport() {
        // TODO getAggregateSupport
        return PostgreSQLAggregateSupport.valueOf(this);
    }

    @Override
    public boolean supportsValuesList() {
        return true;
    }

    @Override
    public boolean supportsJdbcConnectionLobCreation(DatabaseMetaData databaseMetaData) {
        // TODO supportsJdbcConnectionLobCreation
        return false;
    }

    @Override
    public boolean supportsMaterializedLobAccess() {
        return false;
    }

    @Override
    public SqlAstTranslatorFactory getSqlAstTranslatorFactory() {
        return new StandardSqlAstTranslatorFactory() {
            @Override
            protected <T extends JdbcOperation> SqlAstTranslator<T> buildTranslator(SessionFactoryImplementor sessionFactory, Statement statement) {
                return new TsurugiSqlAstTranslator<>(sessionFactory, statement);
            }
        };
    }

    @Override
    public SelectItemReferenceStrategy getGroupBySelectItemReferenceStrategy() {
        return SelectItemReferenceStrategy.EXPRESSION;
    }

    @Override
    public int getMaxVarcharLength() {
        return 2097132;
    }

    @Override
    public int getMaxVarbinaryLength() {
        return 2097132;
    }

    @Override
    public void appendDatetimeFormat(SqlAppender appender, String format) {
        appender.appendSql(datetimeFormat(format).result());
    }

    /**
     * datetime format.
     *
     * @param format format
     * @return replacer
     */
    protected Replacer datetimeFormat(String format) {
        return OracleDialect.datetimeFormat(format, true, false) //
                .replace("SSSSSS", "US").replace("SSSSS", "US").replace("SSSS", "US").replace("SSS", "MS").replace("SS", "MS").replace("S", "MS")
                // use ISO day in week, as per DateTimeFormatter
                .replace("ee", "ID").replace("e", "fmID")
                // TZR is TZ in Postgres
                .replace("zzz", "TZ").replace("zz", "TZ").replace("z", "TZ").replace("xxx", "OF").replace("xx", "OF").replace("x", "OF");
    }

    @Override
    public void appendDateTimeLiteral(SqlAppender appender, TemporalAccessor temporalAccessor, TemporalType precision, TimeZone jdbcTimeZone) {
        switch (precision) {
        case DATE:
            appender.appendSql("date'");
            appendAsDate(appender, temporalAccessor);
            appender.appendSql('\'');
            break;
        case TIME:
            if (temporalAccessor.isSupported(ChronoField.OFFSET_SECONDS)) {
                appender.appendSql("time with time zone'");
                appendAsTime(appender, temporalAccessor, true, jdbcTimeZone);
            } else {
                appender.appendSql("time'");
                appendAsLocalTime(appender, temporalAccessor);
            }
            appender.appendSql('\'');
            break;
        case TIMESTAMP:
            if (temporalAccessor.isSupported(ChronoField.OFFSET_SECONDS)) {
                appender.appendSql("timestamp with time zone'");
                appendAsTimestampWithMicros(appender, temporalAccessor, true, jdbcTimeZone);
                appender.appendSql('\'');
            } else {
                appender.appendSql("timestamp'");
                appendAsTimestampWithMicros(appender, temporalAccessor, false, jdbcTimeZone);
                appender.appendSql('\'');
            }
            break;
        default:
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void appendDateTimeLiteral(SqlAppender appender, Date date, TemporalType precision, TimeZone jdbcTimeZone) {
        switch (precision) {
        case DATE:
            appender.appendSql("date'");
            appendAsDate(appender, date);
            appender.appendSql('\'');
            break;
        case TIME:
            appender.appendSql("time with time zone'");
            appendAsTime(appender, date, jdbcTimeZone);
            appender.appendSql('\'');
            break;
        case TIMESTAMP:
            appender.appendSql("timestamp with time zone'");
            appendAsTimestampWithMicros(appender, date, jdbcTimeZone);
            appender.appendSql('\'');
            break;
        default:
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void appendDateTimeLiteral(SqlAppender appender, Calendar calendar, TemporalType precision, TimeZone jdbcTimeZone) {
        switch (precision) {
        case DATE:
            appender.appendSql("date'");
            appendAsDate(appender, calendar);
            appender.appendSql('\'');
            break;
        case TIME:
            appender.appendSql("time with time zone'");
            appendAsTime(appender, calendar, jdbcTimeZone);
            appender.appendSql('\'');
            break;
        case TIMESTAMP:
            appender.appendSql("timestamp with time zone'");
            appendAsTimestampWithMillis(appender, calendar, jdbcTimeZone);
            appender.appendSql('\'');
            break;
        default:
            throw new IllegalArgumentException();
        }
    }

    @Override
    public boolean supportsTemporalLiteralOffset() {
        return true;
    }

    @Override
    public TimeZoneSupport getTimeZoneSupport() {
        return TimeZoneSupport.NORMALIZE;
    }

    @Override
    public boolean supportsBindingNullSqlTypeForSetNull() {
        // TODO supportsBindingNullSqlTypeForSetNull
        return true;
    }
}
