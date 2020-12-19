/*
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
package io.prestosql.sql.planner.iterative.rule;

import io.prestosql.metadata.Metadata;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.LogicalBinaryExpression;

import static io.prestosql.sql.ExpressionUtils.combinePredicates;
import static io.prestosql.sql.ExpressionUtils.extractPredicates;

/**
 * Flattens and removes duplicate conjuncts or disjuncts. E.g.,
 * <p>
 * a = 1 AND a = 1
 * <p>
 * rewrites to:
 * <p>
 * a = 1
 */
public class RemoveDuplicateConditions
        extends ExpressionRewriteRuleSet
{
    public RemoveDuplicateConditions(Metadata metadata)
    {
        super(createRewrite(metadata));
    }

    private static ExpressionRewriter createRewrite(Metadata metadata)
    {
        return (expression, context) -> ExpressionTreeRewriter.rewriteWith(new Visitor(metadata), expression);
    }

    private static class Visitor
            extends io.prestosql.sql.tree.ExpressionRewriter<Void>
    {
        private final Metadata metadata;

        public Visitor(Metadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        public Expression rewriteLogicalBinaryExpression(LogicalBinaryExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return combinePredicates(metadata, node.getOperator(), extractPredicates(node));
        }
    }
}
