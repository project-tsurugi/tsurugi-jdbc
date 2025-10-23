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

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.sql.ast.Clause;
import org.hibernate.sql.ast.spi.AbstractSqlAstTranslator;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.ast.tree.from.TableReference;
import org.hibernate.sql.ast.tree.select.QueryPart;
import org.hibernate.sql.exec.spi.JdbcOperation;

/**
 * Tsurugi SqlAstTranslator.
 *
 * @param <T> operation
 */
public class TsurugiSqlAstTranslator<T extends JdbcOperation> extends AbstractSqlAstTranslator<T> {

    /**
     * Creates a new instance.
     *
     * @param sessionFactory session factory
     * @param statement      statement
     */
    public TsurugiSqlAstTranslator(SessionFactoryImplementor sessionFactory, Statement statement) {
        super(sessionFactory, statement);
    }

    /**
     * Renders the table reference identification variable (alias).
     *
     * Tsurugi does not support table aliases in UPDATE and DELETE statements, so we override this method to skip rendering the alias for these clauses.
     *
     * @param tableReference the table reference
     */
    @Override
    protected void renderTableReferenceIdentificationVariable(TableReference tableReference) {
        Clause currentClause = getClauseStack().getCurrent();

        // Tsurugi does not support table aliases in UPDATE and DELETE clauses
        if (currentClause == Clause.UPDATE || currentClause == Clause.DELETE) {
            // Do not render the alias for UPDATE and DELETE statements
            return;
        }

        // For other clauses (SELECT, INSERT, etc.), use the default behavior
        super.renderTableReferenceIdentificationVariable(tableReference);
    }

    /**
     * Renders the LIMIT/OFFSET clause for Tsurugi Database.
     *
     * Tsurugi does not support OFFSET clause, so only LIMIT is rendered. Tsurugi also does not support placeholders in LIMIT clause, so literal values are used.
     */
    @Override
    public void visitOffsetFetchClause(QueryPart queryPart) {
        if (!isRowNumberingCurrentQueryPart()) {
            // Tsurugi does not support OFFSET, so only LIMIT is rendered
            if (queryPart.isRoot() && hasLimit()) {
                prepareLimitOffsetParameters();
                renderTsurugiLimitClause(getLimitParameter());
            } else if (queryPart.getFetchClauseExpression() != null) {
                renderTsurugiLimitClause(queryPart.getFetchClauseExpression());
            }
        }
    }

    /**
     * Renders the LIMIT clause with literal value (not placeholder).
     *
     * @param fetchExpression expression
     */
    private void renderTsurugiLimitClause(org.hibernate.sql.ast.tree.expression.Expression fetchExpression) {
        appendSql(" limit ");
        // Tsurugi does not support placeholders, so output the value as a literal
        getClauseStack().push(Clause.FETCH);
        try {
            renderExpressionAsLiteral(fetchExpression, getJdbcParameterBindings());
        } finally {
            getClauseStack().pop();
        }
    }
}
