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
    using System.Linq;
    using System.Text;

    /**
     * The inner representation of the SearchArgument. Most users should not
     * need this interface, it is only for file formats that need to translate
     * the SearchArgument into an internal form.
     */
    public class ExpressionTree
    {
        public enum Operator { OR, AND, NOT, LEAF, CONSTANT }
        private Operator @operator;
        private List<ExpressionTree> children;
        private int leaf;
        private TruthValue? constant;

        public ExpressionTree(Operator op, params ExpressionTree[] kids)
        {
            @operator = op;
            children = kids.ToList();
            leaf = -1;
            this.constant = null;
        }

        public ExpressionTree(int leaf)
        {
            @operator = Operator.LEAF;
            children = null;
            this.leaf = leaf;
            this.constant = null;
        }

        public ExpressionTree(TruthValue constant)
        {
            @operator = Operator.CONSTANT;
            children = null;
            this.leaf = -1;
            this.constant = constant;
        }

        public ExpressionTree(ExpressionTree other)
        {
            this.@operator = other.@operator;
            if (other.children == null)
            {
                this.children = null;
            }
            else
            {
                this.children = new List<ExpressionTree>();
                foreach (ExpressionTree child in other.children)
                {
                    children.Add(new ExpressionTree(child));
                }
            }
            this.leaf = other.leaf;
            this.constant = other.constant;
        }

        public TruthValue evaluate(TruthValue[] leaves)
        {
            TruthValue result;
            switch (@operator)
            {
                case Operator.OR:
                    result = TruthValue.NO;
                    foreach (ExpressionTree child in children)
                    {
                        result = child.evaluate(leaves).or(result);
                    }
                    return result;
                case Operator.AND:
                    result = TruthValue.YES;
                    foreach (ExpressionTree child in children)
                    {
                        result = child.evaluate(leaves).and(result);
                    }
                    return result;
                case Operator.NOT:
                    return children[0].evaluate(leaves).not();
                case Operator.LEAF:
                    return leaves[leaf];
                case Operator.CONSTANT:
                    return constant.Value;
                default:
                    throw new InvalidOperationException("Unknown operator: " + @operator);
            }
        }

        public override string ToString()
        {
            StringBuilder buffer = new StringBuilder();
            switch (@operator)
            {
                case Operator.OR:
                    buffer.Append("(or");
                    foreach (ExpressionTree child in children)
                    {
                        buffer.Append(' ');
                        buffer.Append(child.ToString());
                    }
                    buffer.Append(')');
                    break;
                case Operator.AND:
                    buffer.Append("(and");
                    foreach (ExpressionTree child in children)
                    {
                        buffer.Append(' ');
                        buffer.Append(child.ToString());
                    }
                    buffer.Append(')');
                    break;
                case Operator.NOT:
                    buffer.Append("(not ");
                    buffer.Append(children[0]);
                    buffer.Append(')');
                    break;
                case Operator.LEAF:
                    buffer.Append("leaf-");
                    buffer.Append(leaf);
                    break;
                case Operator.CONSTANT:
                    buffer.Append(constant);
                    break;
            }
            return buffer.ToString();
        }

        public Operator getOperator()
        {
            return @operator;
        }

        public List<ExpressionTree> getChildren()
        {
            return children;
        }

        public TruthValue? getConstant()
        {
            return constant;
        }

        public int getLeaf()
        {
            return leaf;
        }
    }
}
