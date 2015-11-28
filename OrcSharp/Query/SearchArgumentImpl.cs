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
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using org.apache.hadoop.hive.ql.io.orc;

    /**
     * The implementation of SearchArguments.
     */
    sealed class SearchArgumentImpl : SearchArgument
    {
        internal sealed class PredicateLeafImpl : PredicateLeaf
        {
            private PredicateLeaf.Operator @operator;
            private Type type;
            private string columnName;
            private object literal;
            private List<object> literalList;

            public PredicateLeafImpl(
                PredicateLeaf.Operator @operator,
                Type type,
                String columnName,
                Object literal,
                List<Object> literalList)
            {
                this.@operator = @operator;
                this.type = type;
                this.columnName = columnName;
                this.literal = literal;
                if (literal != null)
                {
                    if (literal.GetType() != type.getValueClass())
                    {
                        throw new ArgumentException("Wrong value class " +
                            literal.GetType().Name + " for " + type + "." + @operator +
                            " leaf");
                    }
                }
                this.literalList = literalList;
                if (literalList != null)
                {
                    System.Type valueCls = type.getValueClass();
                    foreach (object lit in literalList)
                    {
                        if (lit != null && lit.GetType() != valueCls)
                        {
                            throw new ArgumentException("Wrong value class item " +
                                lit.GetType().Name + " for " + type + "." + @operator +
                                " leaf");
                        }
                    }
                }
            }

            public override PredicateLeaf.Operator getOperator()
            {
                return @operator;
            }

            public override Type getType()
            {
                return type;
            }

            public override string getColumnName()
            {
                return columnName;
            }

            public override object getLiteral()
            {
#if FALSE
                // To get around a kryo 2.22 bug while deserialize a Timestamp into Date
                // (https://github.com/EsotericSoftware/kryo/issues/88)
                // When we see a Date, convert back into Timestamp
                if (literal is java.util.Date)
                {
                    return new Timestamp(((java.util.Date)literal).getTime());
                }
#endif
                return literal;
            }

            public override List<object> getLiteralList()
            {
                return literalList;
            }

            public override string ToString()
            {
                StringBuilder buffer = new StringBuilder();
                buffer.Append('(');
                buffer.Append(@operator);
                buffer.Append(' ');
                buffer.Append(columnName);
                if (literal != null)
                {
                    buffer.Append(' ');
                    buffer.Append(literal);
                }
                else if (literalList != null)
                {
                    foreach (Object lit in literalList)
                    {
                        buffer.Append(' ');
                        buffer.Append(lit == null ? "null" : lit.ToString());
                    }
                }
                buffer.Append(')');
                return buffer.ToString();
            }

            private static bool isEqual(Object left, Object right)
            {

                return left == right ||
                    (left != null && right != null && left.Equals(right));
            }

            public override bool Equals(object other)
            {
                if (other == null || other.GetType() != GetType())
                {
                    return false;
                }
                else if (other == this)
                {
                    return true;
                }
                else
                {
                    PredicateLeafImpl o = (PredicateLeafImpl)other;
                    return @operator == o.@operator &&
                        type == o.type &&
                        columnName.Equals(o.columnName) &&
                        isEqual(literal, o.literal) &&
                        isEqual(literalList, o.literalList);
                }
            }

            public override int GetHashCode()
            {
                return @operator.GetHashCode() +
                     type.GetHashCode() * 17 +
                     columnName.GetHashCode() * 3 * 17 +
                     (literal == null ? 0 : literal.GetHashCode()) * 101 * 3 * 17 +
                     (literalList == null ? 0 : literalList.GetHashCode()) *
                         103 * 101 * 3 * 17;
            }

            public static void setColumnName(PredicateLeaf leaf, String newName)
            {
                Debug.Assert(leaf is PredicateLeafImpl);
                ((PredicateLeafImpl)leaf).columnName = newName;
            }
        }


        private List<PredicateLeaf> leaves;
        private ExpressionTree expression;

        SearchArgumentImpl(ExpressionTree expression, List<PredicateLeaf> leaves)
        {
            this.expression = expression;
            this.leaves = leaves;
        }

        // Used by kyro
        SearchArgumentImpl()
        {
            leaves = null;
            expression = null;
        }

        public override List<PredicateLeaf> getLeaves()
        {
            return leaves;
        }

        public override TruthValue evaluate(TruthValue[] leaves)
        {
            return expression == null ? TruthValue.YES : expression.evaluate(leaves);
        }

        public override ExpressionTree getExpression()
        {
            return expression;
        }

        public override string ToString()
        {
            StringBuilder buffer = new StringBuilder();
            for (int i = 0; i < leaves.Count; ++i)
            {
                buffer.Append("leaf-");
                buffer.Append(i);
                buffer.Append(" = ");
                buffer.Append(leaves[i].ToString());
                buffer.Append(", ");
            }
            buffer.Append("expr = ");
            buffer.Append(expression);
            return buffer.ToString();
        }

        internal class BuilderImpl : SearchArgument.Builder
        {

            // max threshold for CNF conversion. having >8 elements in andList will be
            // converted to maybe
            private const int CNF_COMBINATIONS_THRESHOLD = 256;

            private Stack<ExpressionTree> currentTree =
                new Stack<ExpressionTree>();
            private Dictionary<PredicateLeaf, int> leaves =
                new Dictionary<PredicateLeaf, int>();
            private ExpressionTree root =
                new ExpressionTree(ExpressionTree.Operator.AND);

            internal BuilderImpl()
            {
                currentTree.Push(root);
            }

            public SearchArgument.Builder startOr()
            {
                ExpressionTree node = new ExpressionTree(ExpressionTree.Operator.OR);
                currentTree.Peek().getChildren().Add(node);
                currentTree.Push(node);
                return this;
            }

            public SearchArgument.Builder startAnd()
            {
                ExpressionTree node = new ExpressionTree(ExpressionTree.Operator.AND);
                currentTree.Peek().getChildren().Add(node);
                currentTree.Push(node);
                return this;
            }

            public SearchArgument.Builder startNot()
            {
                ExpressionTree node = new ExpressionTree(ExpressionTree.Operator.NOT);
                currentTree.Peek().getChildren().Add(node);
                currentTree.Push(node);
                return this;
            }

            public SearchArgument.Builder end()
            {
                ExpressionTree current = currentTree.Pop();
                if (current.getChildren().Count == 0)
                {
                    throw new ArgumentException("Can't create expression " + root +
                        " with no children.");
                }
                if (current.getOperator() == ExpressionTree.Operator.NOT &&
                    current.getChildren().Count != 1)
                {
                    throw new ArgumentException("Can't create not expression " +
                        current + " with more than 1 child.");
                }
                return this;
            }

            private int addLeaf(PredicateLeaf leaf)
            {
                int result;
                if (!leaves.TryGetValue(leaf, out result))
                {
                    int id = leaves.Count;
                    leaves[leaf] = id;
                    return id;
                }
                else
                {
                    return result;
                }
            }

            public SearchArgument.Builder lessThan(String column, PredicateLeaf.Type type,
                                    Object literal)
            {
                ExpressionTree parent = currentTree.Peek();
                if (column == null || literal == null)
                {
                    parent.getChildren().Add(new ExpressionTree(TruthValue.YES_NO_NULL));
                }
                else
                {
                    PredicateLeaf leaf =
                        new PredicateLeafImpl(PredicateLeaf.Operator.LESS_THAN,
                            type, column, literal, null);
                    parent.getChildren().Add(new ExpressionTree(addLeaf(leaf)));
                }
                return this;
            }

            public SearchArgument.Builder lessThanEquals(String column, PredicateLeaf.Type type,
                                          Object literal)
            {
                ExpressionTree parent = currentTree.Peek();
                if (column == null || literal == null)
                {
                    parent.getChildren().Add(new ExpressionTree(TruthValue.YES_NO_NULL));
                }
                else
                {
                    PredicateLeaf leaf =
                        new PredicateLeafImpl(PredicateLeaf.Operator.LESS_THAN_EQUALS,
                            type, column, literal, null);
                    parent.getChildren().Add(new ExpressionTree(addLeaf(leaf)));
                }
                return this;
            }

            public SearchArgument.Builder equals(String column, PredicateLeaf.Type type,
                                  Object literal)
            {
                ExpressionTree parent = currentTree.Peek();
                if (column == null || literal == null)
                {
                    parent.getChildren().Add(new ExpressionTree(TruthValue.YES_NO_NULL));
                }
                else
                {
                    PredicateLeaf leaf =
                        new PredicateLeafImpl(PredicateLeaf.Operator.EQUALS,
                            type, column, literal, null);
                    parent.getChildren().Add(new ExpressionTree(addLeaf(leaf)));
                }
                return this;
            }

            public SearchArgument.Builder nullSafeEquals(String column, PredicateLeaf.Type type,
                                          Object literal)
            {
                ExpressionTree parent = currentTree.Peek();
                if (column == null || literal == null)
                {
                    parent.getChildren().Add(new ExpressionTree(TruthValue.YES_NO_NULL));
                }
                else
                {
                    PredicateLeaf leaf =
                        new PredicateLeafImpl(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                            type, column, literal, null);
                    parent.getChildren().Add(new ExpressionTree(addLeaf(leaf)));
                }
                return this;
            }

            public SearchArgument.Builder @in(String column, PredicateLeaf.Type type,
                              params object[] literal)
            {
                ExpressionTree parent = currentTree.Peek();
                if (column == null || literal == null)
                {
                    parent.getChildren().Add(new ExpressionTree(TruthValue.YES_NO_NULL));
                }
                else
                {
                    if (literal.Length == 0)
                    {
                        throw new ArgumentException("Can't create in expression with "
                            + "no arguments");
                    }
                    List<Object> argList = new List<Object>();
                    argList.AddRange(literal.ToList());

                    PredicateLeaf leaf =
                        new PredicateLeafImpl(PredicateLeaf.Operator.IN,
                            type, column, null, argList);
                    parent.getChildren().Add(new ExpressionTree(addLeaf(leaf)));
                }
                return this;
            }

            public SearchArgument.Builder isNull(String column, PredicateLeaf.Type type)
            {
                ExpressionTree parent = currentTree.Peek();
                if (column == null)
                {
                    parent.getChildren().Add(new ExpressionTree(TruthValue.YES_NO_NULL));
                }
                else
                {
                    PredicateLeaf leaf =
                        new PredicateLeafImpl(PredicateLeaf.Operator.IS_NULL,
                            type, column, null, null);
                    parent.getChildren().Add(new ExpressionTree(addLeaf(leaf)));
                }
                return this;
            }

            public SearchArgument.Builder between(String column, PredicateLeaf.Type type, Object lower,
                                   Object upper)
            {
                ExpressionTree parent = currentTree.Peek();
                if (column == null || lower == null || upper == null)
                {
                    parent.getChildren().Add(new ExpressionTree(TruthValue.YES_NO_NULL));
                }
                else
                {
                    List<Object> argList = new List<Object>();
                    argList.Add(lower);
                    argList.Add(upper);
                    PredicateLeaf leaf =
                        new PredicateLeafImpl(PredicateLeaf.Operator.BETWEEN,
                            type, column, null, argList);
                    parent.getChildren().Add(new ExpressionTree(addLeaf(leaf)));
                }
                return this;
            }

            public SearchArgument.Builder literal(TruthValue truth)
            {
                ExpressionTree parent = currentTree.Peek();
                parent.getChildren().Add(new ExpressionTree(truth));
                return this;
            }

            /**
             * Recursively explore the tree to find the leaves that are still reachable
             * after optimizations.
             * @param tree the node to check next
             * @param next the next available leaf id
             * @param leafReorder
             * @return the next available leaf id
             */
            static int compactLeaves(ExpressionTree tree, int next, int[] leafReorder)
            {
                if (tree.getOperator() == ExpressionTree.Operator.LEAF)
                {
                    int oldLeaf = tree.getLeaf();
                    if (leafReorder[oldLeaf] == -1)
                    {
                        leafReorder[oldLeaf] = next++;
                    }
                }
                else if (tree.getChildren() != null)
                {
                    foreach (ExpressionTree child in tree.getChildren())
                    {
                        next = compactLeaves(child, next, leafReorder);
                    }
                }
                return next;
            }

            /**
             * Rewrite expression tree to update the leaves.
             * @param root the root of the tree to fix
             * @param leafReorder a map from old leaf ids to new leaf ids
             * @return the fixed root
             */
            static ExpressionTree rewriteLeaves(ExpressionTree root,
                                                int[] leafReorder)
            {
                if (root.getOperator() == ExpressionTree.Operator.LEAF)
                {
                    return new ExpressionTree(leafReorder[root.getLeaf()]);
                }
                else if (root.getChildren() != null)
                {
                    List<ExpressionTree> children = root.getChildren();
                    for (int i = 0; i < children.Count; ++i)
                    {
                        children[i] = rewriteLeaves(children[i], leafReorder);
                    }
                }
                return root;
            }

            public SearchArgument build()
            {
                if (currentTree.Count != 1)
                {
                    throw new ArgumentException("Failed to end " +
                        currentTree.Count + " operations.");
                }
                ExpressionTree optimized = pushDownNot(root);
                optimized = foldMaybe(optimized);
                optimized = flatten(optimized);
                optimized = convertToCNF(optimized);
                optimized = flatten(optimized);
                int[] leafReorder = new int[leaves.Count];
                Arrays.fill(leafReorder, -1);
                int newLeafCount = compactLeaves(optimized, 0, leafReorder);
                optimized = rewriteLeaves(optimized, leafReorder);
                List<PredicateLeaf> leafList = new List<PredicateLeaf>(newLeafCount);
                // expand list to correct size
                for (int i = 0; i < newLeafCount; ++i)
                {
                    leafList.Add(null);
                }
                // build the new list
                foreach (KeyValuePair<PredicateLeaf, int> elem in leaves)
                {
                    int newLoc = leafReorder[elem.Value];
                    if (newLoc != -1)
                    {
                        leafList[newLoc] = elem.Key;
                    }
                }
                return new SearchArgumentImpl(optimized, leafList);
            }

            /**
             * Push the negations all the way to just before the leaves. Also remove
             * double negatives.
             * @param root the expression to normalize
             * @return the normalized expression, which may share some or all of the
             * nodes of the original expression.
             */
            internal static ExpressionTree pushDownNot(ExpressionTree root)
            {
                if (root.getOperator() == ExpressionTree.Operator.NOT)
                {
                    ExpressionTree child = root.getChildren()[0];
                    switch (child.getOperator())
                    {
                        case ExpressionTree.Operator.NOT:
                            return pushDownNot(child.getChildren()[0]);
                        case ExpressionTree.Operator.CONSTANT:
                            return new ExpressionTree(child.getConstant().Value.not());
                        case ExpressionTree.Operator.AND:
                            root = new ExpressionTree(ExpressionTree.Operator.OR);
                            foreach (ExpressionTree kid in child.getChildren())
                            {
                                root.getChildren().Add(pushDownNot(new
                                    ExpressionTree(ExpressionTree.Operator.NOT, kid)));
                            }
                            break;
                        case ExpressionTree.Operator.OR:
                            root = new ExpressionTree(ExpressionTree.Operator.AND);
                            foreach (ExpressionTree kid in child.getChildren())
                            {
                                root.getChildren().Add(pushDownNot(new ExpressionTree
                                    (ExpressionTree.Operator.NOT, kid)));
                            }
                            break;
                        // for leaf, we don't do anything
                        default:
                            break;
                    }
                }
                else if (root.getChildren() != null)
                {
                    // iterate through children and push down not for each one
                    for (int i = 0; i < root.getChildren().Count; ++i)
                    {
                        root.getChildren()[i] = pushDownNot(root.getChildren()[i]);
                    }
                }
                return root;
            }

            /**
             * Remove MAYBE values from the expression. If they are in an AND operator,
             * they are dropped. If they are in an OR operator, they kill their parent.
             * This assumes that pushDownNot has already been called.
             * @param expr The expression to clean up
             * @return The cleaned up expression
             */
            internal static ExpressionTree foldMaybe(ExpressionTree expr)
            {
                if (expr.getChildren() != null)
                {
                    for (int i = 0; i < expr.getChildren().Count; ++i)
                    {
                        ExpressionTree child = foldMaybe(expr.getChildren()[i]);
                        if (child.getConstant() == TruthValue.YES_NO_NULL)
                        {
                            switch (expr.getOperator())
                            {
                                case ExpressionTree.Operator.AND:
                                    expr.getChildren().RemoveAt(i);
                                    i -= 1;
                                    break;
                                case ExpressionTree.Operator.OR:
                                    // a maybe will kill the or condition
                                    return child;
                                default:
                                    throw new InvalidOperationException("Got a maybe as child of " +
                                        expr);
                            }
                        }
                        else
                        {
                            expr.getChildren()[i] = child;
                        }
                    }
                    if (expr.getChildren().Count == 0)
                    {
                        return new ExpressionTree(TruthValue.YES_NO_NULL);
                    }
                }
                return expr;
            }

            /**
             * Converts multi-level ands and ors into single level ones.
             * @param root the expression to flatten
             * @return the flattened expression, which will always be root with
             *   potentially modified children.
             */
            internal static ExpressionTree flatten(ExpressionTree root)
            {
                if (root.getChildren() != null)
                {
                    // iterate through the index, so that if we add more children,
                    // they don't get re-visited
                    for (int i = 0; i < root.getChildren().Count; ++i)
                    {
                        ExpressionTree child = flatten(root.getChildren()[i]);
                        // do we need to flatten?
                        if (child.getOperator() == root.getOperator() &&
                            child.getOperator() != ExpressionTree.Operator.NOT)
                        {
                            bool first = true;
                            foreach (ExpressionTree grandkid in child.getChildren())
                            {
                                // for the first grandkid replace the original parent
                                if (first)
                                {
                                    first = false;
                                    root.getChildren()[i] = grandkid;
                                }
                                else
                                {
                                    root.getChildren().Insert(++i, grandkid);
                                }
                            }
                        }
                        else
                        {
                            root.getChildren()[i] = child;
                        }
                    }
                    // if we have a singleton AND or OR, just return the child
                    if ((root.getOperator() == ExpressionTree.Operator.OR ||
                        root.getOperator() == ExpressionTree.Operator.AND) &&
                        root.getChildren().Count == 1)
                    {
                        return root.getChildren()[0];
                    }
                }
                return root;
            }

            /**
             * Generate all combinations of items on the andList. For each item on the
             * andList, it generates all combinations of one child from each and
             * expression. Thus, (and a b) (and c d) will be expanded to: (or a c)
             * (or a d) (or b c) (or b d). If there are items on the nonAndList, they
             * are added to each or expression.
             * @param result a list to put the results onto
             * @param andList a list of and expressions
             * @param nonAndList a list of non-and expressions
             */
            private static void generateAllCombinations(List<ExpressionTree> result,
                                                        List<ExpressionTree> andList,
                                                        List<ExpressionTree> nonAndList
            )
            {
                List<ExpressionTree> kids = andList[0].getChildren();
                if (result.Count == 0)
                {
                    foreach (ExpressionTree kid in kids)
                    {
                        ExpressionTree or = new ExpressionTree(ExpressionTree.Operator.OR);
                        result.Add(or);
                        foreach (ExpressionTree node in nonAndList)
                        {
                            or.getChildren().Add(new ExpressionTree(node));
                        }
                        or.getChildren().Add(kid);
                    }
                }
                else
                {
                    List<ExpressionTree> work = new List<ExpressionTree>(result);
                    result.Clear();
                    foreach (ExpressionTree kid in kids)
                    {
                        foreach (ExpressionTree or in work)
                        {
                            ExpressionTree copy = new ExpressionTree(or);
                            copy.getChildren().Add(kid);
                            result.Add(copy);
                        }
                    }
                }
                if (andList.Count > 1)
                {
                    generateAllCombinations(result, andList.subList(1, andList.Count),
                        nonAndList);
                }
            }

            /**
             * Convert an expression so that the top level operator is AND with OR
             * operators under it. This routine assumes that all of the NOT operators
             * have been pushed to the leaves via pushdDownNot.
             * @param root the expression
             * @return the normalized expression
             */
            internal static ExpressionTree convertToCNF(ExpressionTree root)
            {
                if (root.getChildren() != null)
                {
                    // convert all of the children to CNF
                    int size = root.getChildren().Count;
                    for (int i = 0; i < size; ++i)
                    {
                        root.getChildren()[i] = convertToCNF(root.getChildren()[i]);
                    }
                    if (root.getOperator() == ExpressionTree.Operator.OR)
                    {
                        // a list of leaves that weren't under AND expressions
                        List<ExpressionTree> nonAndList = new List<ExpressionTree>();
                        // a list of AND expressions that we need to distribute
                        List<ExpressionTree> andList = new List<ExpressionTree>();
                        foreach (ExpressionTree child in root.getChildren())
                        {
                            if (child.getOperator() == ExpressionTree.Operator.AND)
                            {
                                andList.Add(child);
                            }
                            else if (child.getOperator() == ExpressionTree.Operator.OR)
                            {
                                // pull apart the kids of the OR expression
                                foreach (ExpressionTree grandkid in child.getChildren())
                                {
                                    nonAndList.Add(grandkid);
                                }
                            }
                            else
                            {
                                nonAndList.Add(child);
                            }
                        }
                        if (andList.Count != 0)
                        {
                            if (checkCombinationsThreshold(andList))
                            {
                                root = new ExpressionTree(ExpressionTree.Operator.AND);
                                generateAllCombinations(root.getChildren(), andList, nonAndList);
                            }
                            else
                            {
                                root = new ExpressionTree(TruthValue.YES_NO_NULL);
                            }
                        }
                    }
                }
                return root;
            }

            private static bool checkCombinationsThreshold(List<ExpressionTree> andList)
            {
                int numComb = 1;
                foreach (ExpressionTree tree in andList)
                {
                    numComb *= tree.getChildren().Count;
                    if (numComb > CNF_COMBINATIONS_THRESHOLD)
                    {
                        return false;
                    }
                }
                return true;
            }

        }
    }
}
