/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace org.apache.hadoop.hive.ql.io.orc.query
{
    using System;
    using System.Collections.Generic;
    using org.apache.hadoop.hive.ql.io.orc.external;

    public class ConvertAstToSearchArg
    {
        private static Log LOG = LogFactory.getLog(typeof(ConvertAstToSearchArg));
        private SearchArgument.Builder builder =
            SearchArgumentFactory.newBuilder();

        /**
         * Builds the expression and leaf list from the original predicate.
         * @param expression the expression to translate.
         */
        ConvertAstToSearchArg(ExprNodeGenericFuncDesc expression)
        {
            parse(expression);
        }

        /**
         * Build the search argument from the expression.
         * @return the search argument
         */
        public SearchArgument buildSearchArgument()
        {
            return builder.build();
        }

        /**
         * Get the type of the given expression node.
         * @param expr the expression to get the type of
         * @return int, string, or float or null if we don't know the type
         */
        private static PredicateLeaf.Type? getType(ExprNodeDesc expr)
        {
            TypeInfo type = expr.getTypeInfo();
            if (type.getCategory() == ObjectInspectorCategory.PRIMITIVE)
            {
                switch (((PrimitiveTypeInfo)type).getPrimitiveCategory())
                {
                    case PrimitiveCategory.BYTE:
                    case PrimitiveCategory.SHORT:
                    case PrimitiveCategory.INT:
                    case PrimitiveCategory.LONG:
                        return PredicateLeaf.Type.LONG;
                    case PrimitiveCategory.CHAR:
                    case PrimitiveCategory.VARCHAR:
                    case PrimitiveCategory.STRING:
                        return PredicateLeaf.Type.STRING;
                    case PrimitiveCategory.FLOAT:
                    case PrimitiveCategory.DOUBLE:
                        return PredicateLeaf.Type.FLOAT;
                    case PrimitiveCategory.DATE:
                        return PredicateLeaf.Type.DATE;
                    case PrimitiveCategory.TIMESTAMP:
                        return PredicateLeaf.Type.TIMESTAMP;
                    case PrimitiveCategory.DECIMAL:
                        return PredicateLeaf.Type.DECIMAL;
                    case PrimitiveCategory.BOOLEAN:
                        return PredicateLeaf.Type.BOOLEAN;
                }
            }
            return null;
        }

        /**
         * Get the column name referenced in the expression. It must be at the top
         * level of this expression and there must be exactly one column.
         * @param expr the expression to look in
         * @param variable the slot the variable is expected in
         * @return the column name or null if there isn't exactly one column
         */
        private static string getColumnName(ExprNodeGenericFuncDesc expr,
                                            int variable)
        {
            List<ExprNodeDesc> children = expr.getChildren();
            if (variable < 0 || variable >= children.Count)
            {
                return null;
            }
            ExprNodeDesc child = children[variable];
            if (child is ExprNodeColumnDesc)
            {
                return ((ExprNodeColumnDesc)child).getColumn();
            }
            return null;
        }

        private static object boxLiteral(ExprNodeConstantDesc constantDesc,
                                         PredicateLeaf.Type type)
        {
            object lit = constantDesc.getValue();
            if (lit == null)
            {
                return null;
            }
            switch (type)
            {
                case PredicateLeaf.Type.LONG:
                    return ((Number)lit).longValue();
                case PredicateLeaf.Type.STRING:
                    if (lit is HiveChar)
                    {
                        return ((HiveChar)lit).getPaddedValue();
                    }
                    else if (lit is String)
                    {
                        return lit;
                    }
                    else
                    {
                        return lit.toString();
                    }
                case PredicateLeaf.Type.FLOAT:
                    if (lit is Float)
                    {
                        // converting a float directly to a double causes annoying conversion
                        // problems
                        return Double.parseDouble(lit.toString());
                    }
                    else
                    {
                        return ((Number)lit).doubleValue();
                    }
                case PredicateLeaf.Type.TIMESTAMP:
                    return Timestamp.valueOf(lit.toString());
                case PredicateLeaf.Type.DATE:
                    return Date.valueOf(lit.toString());
                case PredicateLeaf.Type.DECIMAL:
                    LOG.warn("boxing " + lit);
                    return new HiveDecimalWritable(lit.toString());
                case PredicateLeaf.Type.BOOLEAN:
                    return lit;
                default:
                    throw new ArgumentException("Unknown literal " + type);
            }
        }

        /**
         * Find the child that is the literal.
         * @param expr the parent node to check
         * @param type the type of the expression
         * @return the literal boxed if found or null
         */
        private static object findLiteral(ExprNodeGenericFuncDesc expr,
                                          PredicateLeaf.Type type)
        {
            List<ExprNodeDesc> children = expr.getChildren();
            if (children.Count != 2)
            {
                return null;
            }
            object result = null;
            foreach (ExprNodeDesc child in children)
            {
                if (child is ExprNodeConstantDesc)
                {
                    if (result != null)
                    {
                        return null;
                    }
                    result = boxLiteral((ExprNodeConstantDesc)child, type);
                }
            }
            return result;
        }

        /**
         * Return the boxed literal at the given position
         * @param expr the parent node
         * @param type the type of the expression
         * @param position the child position to check
         * @return the boxed literal if found otherwise null
         */
        private static object getLiteral(ExprNodeGenericFuncDesc expr,
                                         PredicateLeaf.Type type,
                                         int position)
        {
            List<ExprNodeDesc> children = expr.getChildren();
            object child = children.get(position);
            if (child is ExprNodeConstantDesc)
            {
                return boxLiteral((ExprNodeConstantDesc)child, type);
            }
            return null;
        }

        private static object[] getLiteralList(ExprNodeGenericFuncDesc expr,
                                               PredicateLeaf.Type type,
                                               int start)
        {
            List<ExprNodeDesc> children = expr.getChildren();
            object[] result = new object[children.Count - start];

            // ignore the first child, since it is the variable
            int posn = 0;
            foreach (ExprNodeDesc child in children.subList(start, children.Count))
            {
                if (child is ExprNodeConstantDesc)
                {
                    result[posn++] = boxLiteral((ExprNodeConstantDesc)child, type);
                }
                else
                {
                    // if we get some non-literals, we need to punt
                    return null;
                }
            }
            return result;
        }

        private void createLeaf(PredicateLeaf.Operator @operator,
                                ExprNodeGenericFuncDesc expression,
                                int variable)
        {
            string columnName = getColumnName(expression, variable);
            if (columnName == null)
            {
                builder.literal(TruthValue.YES_NO_NULL);
                return;
            }
            PredicateLeaf.Type type = getType(expression.getChildren().get(variable));
            if (type == null)
            {
                builder.literal(TruthValue.YES_NO_NULL);
                return;
            }

            // if the variable was on the right, we need to swap things around
            bool needSwap = false;
            if (variable != 0)
            {
                if (@operator == PredicateLeaf.Operator.LESS_THAN)
                {
                    needSwap = true;
                    @operator = PredicateLeaf.Operator.LESS_THAN_EQUALS;
                }
                else if (@operator == PredicateLeaf.Operator.LESS_THAN_EQUALS)
                {
                    needSwap = true;
                    @operator = PredicateLeaf.Operator.LESS_THAN;
                }
            }
            if (needSwap)
            {
                builder.startNot();
            }

            switch (@operator)
            {
                case PredicateLeaf.Operator.IS_NULL:
                    builder.isNull(columnName, type);
                    break;
                case PredicateLeaf.Operator.EQUALS:
                    builder.equals(columnName, type, findLiteral(expression, type));
                    break;
                case PredicateLeaf.Operator.NULL_SAFE_EQUALS:
                    builder.nullSafeEquals(columnName, type, findLiteral(expression, type));
                    break;
                case PredicateLeaf.Operator.LESS_THAN:
                    builder.lessThan(columnName, type, findLiteral(expression, type));
                    break;
                case PredicateLeaf.Operator.LESS_THAN_EQUALS:
                    builder.lessThanEquals(columnName, type, findLiteral(expression, type));
                    break;
                case PredicateLeaf.Operator.IN:
                    builder.@in(columnName, type,
                        getLiteralList(expression, type, variable + 1));
                    break;
                case PredicateLeaf.Operator.BETWEEN:
                    builder.between(columnName, type,
                        getLiteral(expression, type, variable + 1),
                        getLiteral(expression, type, variable + 2));
                    break;
            }

            if (needSwap)
            {
                builder.end();
            }
        }

        /**
         * Find the variable in the expression.
         * @param expr the expression to look in
         * @return the index of the variable or -1 if there is not exactly one
         *   variable.
         */
        private int findVariable(ExprNodeDesc expr)
        {
            int result = -1;
            List<ExprNodeDesc> children = expr.getChildren();
            for (int i = 0; i < children.Count; ++i)
            {
                ExprNodeDesc child = children[i];
                if (child is ExprNodeColumnDesc)
                {
                    // if we already found a variable, this isn't a sarg
                    if (result != -1)
                    {
                        return -1;
                    }
                    else
                    {
                        result = i;
                    }
                }
            }
            return result;
        }

        /**
         * Create a leaf expression when we aren't sure where the variable is
         * located.
         * @param operator the operator type that was found
         * @param expression the expression to check
         */
        private void createLeaf(PredicateLeaf.Operator @operator,
                                ExprNodeGenericFuncDesc expression)
        {
            createLeaf(@operator, expression, findVariable(expression));
        }

        private void addChildren(ExprNodeGenericFuncDesc node)
        {
            foreach (ExprNodeDesc child in node.getChildren())
            {
                parse(child);
            }
        }

        /**
         * Do the recursive parse of the Hive ExprNodeDesc into our ExpressionTree.
         * @param expression the Hive ExprNodeDesc
         */
        private void parse(ExprNodeDesc expression)
        {
            // Most of the stuff we can handle are generic function descriptions, so
            // handle the special cases.
            if (expression.GetType() != typeof(ExprNodeGenericFuncDesc))
            {

                // if it is a reference to a boolean column, covert it to a truth test.
                if (expression is ExprNodeColumnDesc)
                {
                    ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc)expression;
                    if (columnDesc.getTypeString().Equals("boolean"))
                    {
                        builder.equals(columnDesc.getColumn(), PredicateLeaf.Type.BOOLEAN,
                            true);
                        return;
                    }
                }

                // otherwise, we don't know what to do so make it a maybe
                builder.literal(TruthValue.YES_NO_NULL);
                return;
            }

            // get the kind of expression
            ExprNodeGenericFuncDesc expr = (ExprNodeGenericFuncDesc)expression;
            Type op = expr.getGenericUDF().GetType();

            // handle the logical operators
            if (op == typeof(GenericUDFOPOr))
            {
                builder.startOr();
                addChildren(expr);
                builder.end();
            }
            else if (op == typeof(GenericUDFOPAnd))
            {
                builder.startAnd();
                addChildren(expr);
                builder.end();
            }
            else if (op == typeof(GenericUDFOPNot))
            {
                builder.startNot();
                addChildren(expr);
                builder.end();
            }
            else if (op == typeof(GenericUDFOPEqual))
            {
                createLeaf(PredicateLeaf.Operator.EQUALS, expr);
            }
            else if (op == typeof(GenericUDFOPNotEqual))
            {
                builder.startNot();
                createLeaf(PredicateLeaf.Operator.EQUALS, expr);
                builder.end();
            }
            else if (op == typeof(GenericUDFOPEqualNS))
            {
                createLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS, expr);
            }
            else if (op == typeof(GenericUDFOPGreaterThan))
            {
                builder.startNot();
                createLeaf(PredicateLeaf.Operator.LESS_THAN_EQUALS, expr);
                builder.end();
            }
            else if (op == typeof(GenericUDFOPEqualOrGreaterThan))
            {
                builder.startNot();
                createLeaf(PredicateLeaf.Operator.LESS_THAN, expr);
                builder.end();
            }
            else if (op == typeof(GenericUDFOPLessThan))
            {
                createLeaf(PredicateLeaf.Operator.LESS_THAN, expr);
            }
            else if (op == typeof(GenericUDFOPEqualOrLessThan))
            {
                createLeaf(PredicateLeaf.Operator.LESS_THAN_EQUALS, expr);
            }
            else if (op == typeof(GenericUDFIn))
            {
                createLeaf(PredicateLeaf.Operator.IN, expr, 0);
            }
            else if (op == typeof(GenericUDFBetween))
            {
                createLeaf(PredicateLeaf.Operator.BETWEEN, expr, 1);
            }
            else if (op == typeof(GenericUDFOPNull))
            {
                createLeaf(PredicateLeaf.Operator.IS_NULL, expr, 0);
            }
            else if (op == typeof(GenericUDFOPNotNull))
            {
                builder.startNot();
                createLeaf(PredicateLeaf.Operator.IS_NULL, expr, 0);
                builder.end();

                // otherwise, we didn't understand it, so mark it maybe
            }
            else
            {
                builder.literal(TruthValue.YES_NO_NULL);
            }
        }


        public static string SARG_PUSHDOWN = "sarg.pushdown";

        public static SearchArgument create(ExprNodeGenericFuncDesc expression)
        {
            return new ConvertAstToSearchArg(expression).buildSearchArgument();
        }

        public static SearchArgument createFromConf(Configuration conf)
        {
            string sargString;
            if ((sargString = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR)) != null)
            {
                return create(Utilities.deserializeExpression(sargString));
            }
            else if ((sargString = conf.get(SARG_PUSHDOWN)) != null)
            {
                return create(sargString);
            }
            return null;
        }

        public static bool canCreateFromConf(Configuration conf)
        {
            return conf.get(TableScanDesc.FILTER_EXPR_CONF_STR) != null || conf.get(SARG_PUSHDOWN) != null;
        }

    }
}
