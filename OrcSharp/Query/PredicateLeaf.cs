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
     * The primitive predicates that form a SearchArgument.
     */
    public abstract class PredicateLeaf
    {
        /**
         * The possible operators for predicates. To get the opposites, construct
         * an expression with a not operator.
         */
        public enum Operator
        {
            EQUALS,
            NULL_SAFE_EQUALS,
            LESS_THAN,
            LESS_THAN_EQUALS,
            IN,
            BETWEEN,
            IS_NULL
        }

        /**
         * The possible types for sargs.
         */
        public enum Type
        {
            LONG,      // all of the integer types
            FLOAT,     // float and double
            STRING,    // string, char, varchar
            DATE,
            DECIMAL,
            TIMESTAMP,
            BOOLEAN
        }


        /**
         * Get the operator for the leaf.
         */
        public abstract Operator getOperator();

        /**
         * Get the type of the column and literal by the file format.
         */
        public abstract Type getType();

        /**
         * Get the simple column name.
         * @return the column name
         */
        public abstract string getColumnName();

        /**
         * Get the literal half of the predicate leaf. Adapt the original type for what orc needs
         *
         * @return an Integer, Long, Double, or String
         */
        public abstract object getLiteral();

        /**
         * For operators with multiple literals (IN and BETWEEN), get the literals.
         *
         * @return the list of literals (Integer, Longs, Doubles, or Strings)
         *
         */
        public abstract List<object> getLiteralList();
    }

    static class TypeMethods
    {
        public static Type getValueClass(this PredicateLeaf.Type type)
        {
            switch (type)
            {
                case PredicateLeaf.Type.BOOLEAN:
                    return typeof(bool);
                case PredicateLeaf.Type.DATE:
                    return typeof(DateTime);
                case PredicateLeaf.Type.DECIMAL:
                    return typeof(HiveDecimal);
                case PredicateLeaf.Type.FLOAT:
                    return typeof(double);
                case PredicateLeaf.Type.LONG:
                    return typeof(long);
                case PredicateLeaf.Type.STRING:
                    return typeof(string);
                case PredicateLeaf.Type.TIMESTAMP:
                    return typeof(DateTime);
                default:
                    throw new InvalidOperationException();
            }
        }
    }
}
