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

    /**
     * Primary interface for <a href="http://en.wikipedia.org/wiki/Sargable">
     *   SearchArgument</a>, which are the subset of predicates
     * that can be pushed down to the RecordReader. Each SearchArgument consists
     * of a series of SearchClauses that must each be true for the row to be
     * accepted by the filter.
     *
     * This requires that the filter be normalized into conjunctive normal form
     * (<a href="http://en.wikipedia.org/wiki/Conjunctive_normal_form">CNF</a>).
     */

    /**
     * The potential result sets of logical operations.
     */
    public enum TruthValue
    {
        YES, NO, NULL, YES_NULL, NO_NULL, YES_NO, YES_NO_NULL
    }

    public static class TruthValueOperations
    {

        /**
         * Compute logical or between the two values.
         * @param right the other argument or null
         * @return the result
         */
        public static TruthValue or(this TruthValue left, TruthValue right)
        {
            if (/* right == null || */ right == left)
            {
                return left;
            }
            if (right == TruthValue.YES || left == TruthValue.YES)
            {
                return TruthValue.YES;
            }
            if (right == TruthValue.YES_NULL || left == TruthValue.YES_NULL)
            {
                return TruthValue.YES_NULL;
            }
            if (right == TruthValue.NO)
            {
                return left;
            }
            if (left == TruthValue.NO)
            {
                return right;
            }
            if (left == TruthValue.NULL)
            {
                if (right == TruthValue.NO_NULL)
                {
                    return TruthValue.NULL;
                }
                else
                {
                    return TruthValue.YES_NULL;
                }
            }
            if (right == TruthValue.NULL)
            {
                if (left == TruthValue.NO_NULL)
                {
                    return TruthValue.NULL;
                }
                else
                {
                    return TruthValue.YES_NULL;
                }
            }
            return TruthValue.YES_NO_NULL;
        }

        /**
         * Compute logical AND between the two values.
         * @param right the other argument or null
         * @return the result
         */
        public static TruthValue and(this TruthValue left, TruthValue right)
        {
            if (/* right == null || */ right == left)
            {
                return left;
            }
            if (right == TruthValue.NO || left == TruthValue.NO)
            {
                return TruthValue.NO;
            }
            if (right == TruthValue.NO_NULL || left == TruthValue.NO_NULL)
            {
                return TruthValue.NO_NULL;
            }
            if (right == TruthValue.YES)
            {
                return left;
            }
            if (left == TruthValue.YES)
            {
                return right;
            }
            if (left == TruthValue.NULL)
            {
                if (right == TruthValue.YES_NULL)
                {
                    return TruthValue.NULL;
                }
                else
                {
                    return TruthValue.NO_NULL;
                }
            }
            if (right == TruthValue.NULL)
            {
                if (left == TruthValue.YES_NULL)
                {
                    return TruthValue.NULL;
                }
                else
                {
                    return TruthValue.NO_NULL;
                }
            }
            return TruthValue.YES_NO_NULL;
        }

        public static TruthValue not(this TruthValue left)
        {
            switch (left)
            {
                case TruthValue.NO:
                    return TruthValue.YES;
                case TruthValue.YES:
                    return TruthValue.NO;
                case TruthValue.NULL:
                case TruthValue.YES_NO:
                case TruthValue.YES_NO_NULL:
                    return left;
                case TruthValue.NO_NULL:
                    return TruthValue.YES_NULL;
                case TruthValue.YES_NULL:
                    return TruthValue.NO_NULL;
                default:
                    throw new ArgumentException("Unknown value: " + left);
            }
        }

        /**
         * Does the RecordReader need to include this set of records?
         * @return true unless none of the rows qualify
         */
        public static bool isNeeded(this TruthValue value)
        {
            switch (value)
            {
                case TruthValue.NO:
                case TruthValue.NULL:
                case TruthValue.NO_NULL:
                    return false;
                default:
                    return true;
            }
        }
    }

    public abstract class SearchArgument
    {
        /**
         * Get the leaf predicates that are required to evaluate the predicate. The
         * list will have the duplicates removed.
         * @return the list of leaf predicates
         */
        public abstract List<PredicateLeaf> getLeaves();

        /**
         * Get the expression tree. This should only needed for file formats that
         * need to translate the expression to an internal form.
         */
        public abstract ExpressionTree getExpression();

        /**
         * Evaluate the entire predicate based on the values for the leaf predicates.
         * @param leaves the value of each leaf predicate
         * @return the value of hte entire predicate
         */
        public abstract TruthValue evaluate(TruthValue[] leaves);

        /**
         * A builder object for contexts outside of Hive where it isn't easy to
         * get a ExprNodeDesc. The user must call startOr, startAnd, or startNot
         * before adding any leaves.
         */
        public interface Builder
        {

            /**
             * Start building an or operation and push it on the stack.
             * @return this
             */
            Builder startOr();

            /**
             * Start building an and operation and push it on the stack.
             * @return this
             */
            Builder startAnd();

            /**
             * Start building a not operation and push it on the stack.
             * @return this
             */
            Builder startNot();

            /**
             * Finish the current operation and pop it off of the stack. Each start
             * call must have a matching end.
             * @return this
             */
            Builder end();

            /**
             * Add a less than leaf to the current item on the stack.
             * @param column the name of the column
             * @param type the type of the expression
             * @param literal the literal
             * @return this
             */
            Builder lessThan(string column, PredicateLeaf.Type type, object literal);

            /**
             * Add a less than equals leaf to the current item on the stack.
             * @param column the name of the column
             * @param type the type of the expression
             * @param literal the literal
             * @return this
             */
            Builder lessThanEquals(string column, PredicateLeaf.Type type, object literal);

            /**
             * Add an equals leaf to the current item on the stack.
             * @param column the name of the column
             * @param type the type of the expression
             * @param literal the literal
             * @return this
             */
            Builder equals(string column, PredicateLeaf.Type type, object literal);

            /**
             * Add a null safe equals leaf to the current item on the stack.
             * @param column the name of the column
             * @param type the type of the expression
             * @param literal the literal
             * @return this
             */
            Builder nullSafeEquals(string column, PredicateLeaf.Type type, object literal);

            /**
             * Add an in leaf to the current item on the stack.
             * @param column the name of the column
             * @param type the type of the expression
             * @param literal the literal
             * @return this
             */
            Builder @in(string column, PredicateLeaf.Type type, params object[] literal);

            /**
             * Add an is null leaf to the current item on the stack.
             * @param column the name of the column
             * @param type the type of the expression
             * @return this
             */
            Builder isNull(string column, PredicateLeaf.Type type);

            /**
             * Add a between leaf to the current item on the stack.
             * @param column the name of the column
             * @param type the type of the expression
             * @param lower the literal
             * @param upper the literal
             * @return this
             */
            Builder between(string column, PredicateLeaf.Type type, object lower, object upper);

            /**
             * Add a truth value to the expression.
             * @param truth
             * @return this
             */
            Builder literal(TruthValue truth);

            /**
             * Build and return the SearchArgument that has been defined. All of the
             * starts must have been ended before this call.
             * @return the new SearchArgument
             */
            SearchArgument build();
        }
    }
}
