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

namespace OrcSharp
{
    using System;
    using System.IO;
    using System.Text;
    using Xunit;

    /**
     * Test the red-black tree with string keys.
     */
    public class TestStringRedBlackTree
    {

        /**
         * Checks the red-black tree rules to make sure that we have correctly built
         * a valid tree.
         *
         * Properties:
         *   1. Red nodes must have black children
         *   2. Each node must have the same black height on both sides.
         *
         * @param node The id of the root of the subtree to check for the red-black
         *        tree properties.
         * @return The black-height of the subtree.
         */
        private int checkSubtree(StringRedBlackTree tree, int node, ref int count)
        {
            if (node == StringRedBlackTree.NULL)
            {
                return 1;
            }
            count++;
            bool is_red = tree.isRed(node);
            int left = tree.getLeft(node);
            int right = tree.getRight(node);
            if (is_red)
            {
                if (tree.isRed(left))
                {
                    printTree(tree, "", tree.Root);
                    throw new InvalidOperationException("Left node of " + node + " is " + left +
                      " and both are red.");
                }
                if (tree.isRed(right))
                {
                    printTree(tree, "", tree.Root);
                    throw new InvalidOperationException("Right node of " + node + " is " +
                      right + " and both are red.");
                }
            }
            int left_depth = checkSubtree(tree, left, ref count);
            int right_depth = checkSubtree(tree, right, ref count);
            if (left_depth != right_depth)
            {
                printTree(tree, "", tree.Root);
                throw new InvalidOperationException("Lopsided tree at node " + node +
                  " with depths " + left_depth + " and " + right_depth);
            }
            if (is_red)
            {
                return left_depth;
            }
            else
            {
                return left_depth + 1;
            }
        }

        /**
         * Checks the validity of the entire tree. Also ensures that the number of
         * nodes visited is the same as the size of the set.
         */
        void checkTree(StringRedBlackTree tree)
        {
            int count = 0;
            if (tree.isRed(tree.Root))
            {
                printTree(tree, "", tree.Root);
                throw new InvalidOperationException("root is red");
            }
            checkSubtree(tree, tree.Root, ref count);
            if (count != tree.Size)
            {
                printTree(tree, "", tree.Root);
                throw new InvalidOperationException("Broken tree! visited= " + count +
                  " size=" + tree.Size);
            }
        }

        void printTree(StringRedBlackTree tree, string indent, int node)
        {
            if (node == StringRedBlackTree.NULL)
            {
                System.Console.Error.WriteLine(indent + "NULL");
            }
            else
            {
                System.Console.Error.WriteLine(indent + "Node " + node + " color " +
                  (tree.isRed(node) ? "red" : "black"));
                printTree(tree, indent + "  ", tree.getLeft(node));
                printTree(tree, indent + "  ", tree.getRight(node));
            }
        }

        private class MyVisitor : StringRedBlackTree.Visitor
        {
            private String[] words;
            private int[] order;
            private MemoryStream buffer = new MemoryStream();
            int current = 0;

            public MyVisitor(string[] args, int[] order)
            {
                words = args;
                this.order = order;
            }

            public void visit(StringRedBlackTree.VisitorContext context)
            {
                string word = context.getText().ToString();
                Assert.Equal(words[current], word);
                Assert.Equal(order[current], context.getOriginalPosition());
                buffer.Position = 0;
                buffer.SetLength(0);
                context.writeBytes(buffer);
                Assert.Equal(word, Encoding.UTF8.GetString(buffer.ToArray()));
                current += 1;
            }
        }

        void checkContents(StringRedBlackTree tree, int[] order, params string[] values)
        {
            tree.visit(new MyVisitor(values, order));
        }

        StringRedBlackTree buildTree(params string[] values)
        {
            StringRedBlackTree result = new StringRedBlackTree(1000);
            foreach (string word in values)
            {
                result.add(word);
                checkTree(result);
            }
            return result;
        }

        [Fact]
        public void test1()
        {
            StringRedBlackTree tree = new StringRedBlackTree(5);
            Assert.Equal(0, tree.getSizeInBytes());
            checkTree(tree);
            Assert.Equal(0, tree.add("owen"));
            checkTree(tree);
            Assert.Equal(1, tree.add("ashutosh"));
            checkTree(tree);
            Assert.Equal(0, tree.add("owen"));
            checkTree(tree);
            Assert.Equal(2, tree.add("alan"));
            checkTree(tree);
            Assert.Equal(2, tree.add("alan"));
            checkTree(tree);
            Assert.Equal(1, tree.add("ashutosh"));
            checkTree(tree);
            Assert.Equal(3, tree.add("greg"));
            checkTree(tree);
            Assert.Equal(4, tree.add("eric"));
            checkTree(tree);
            Assert.Equal(5, tree.add("arun"));
            checkTree(tree);
            Assert.Equal(6, tree.Size);
            checkTree(tree);
            Assert.Equal(6, tree.add("eric14"));
            checkTree(tree);
            Assert.Equal(7, tree.add("o"));
            checkTree(tree);
            Assert.Equal(8, tree.add("ziggy"));
            checkTree(tree);
            Assert.Equal(9, tree.add("z"));
            checkTree(tree);
            checkContents(tree, new int[] { 2, 5, 1, 4, 6, 3, 7, 0, 9, 8 },
              "alan", "arun", "ashutosh", "eric", "eric14", "greg",
              "o", "owen", "z", "ziggy");
            Assert.Equal(32888, tree.getSizeInBytes());
            // check that adding greg again bumps the count
            Assert.Equal(3, tree.add("greg"));
            Assert.Equal(41, tree.getCharacterSize());
            // add some more strings to test the different branches of the
            // rebalancing
            Assert.Equal(10, tree.add("zak"));
            checkTree(tree);
            Assert.Equal(11, tree.add("eric1"));
            checkTree(tree);
            Assert.Equal(12, tree.add("ash"));
            checkTree(tree);
            Assert.Equal(13, tree.add("harry"));
            checkTree(tree);
            Assert.Equal(14, tree.add("john"));
            checkTree(tree);
            tree.clear();
            checkTree(tree);
            Assert.Equal(0, tree.getSizeInBytes());
            Assert.Equal(0, tree.getCharacterSize());
        }

        [Fact]
        public void test2()
        {
            StringRedBlackTree tree =
              buildTree("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l",
                "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");
            Assert.Equal(26, tree.Size);
            checkContents(tree, new int[]{0,1,2, 3,4,5, 6,7,8, 9,10,11, 12,13,14,
      15,16,17, 18,19,20, 21,22,23, 24,25},
              "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o",
              "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");
        }

        [Fact]
        public void test3()
        {
            StringRedBlackTree tree =
              buildTree("z", "y", "x", "w", "v", "u", "t", "s", "r", "q", "p", "o", "n",
                "m", "l", "k", "j", "i", "h", "g", "f", "e", "d", "c", "b", "a");
            Assert.Equal(26, tree.Size);
            checkContents(tree, new int[]{25,24,23, 22,21,20, 19,18,17, 16,15,14,
      13,12,11, 10,9,8, 7,6,5, 4,3,2, 1,0},
              "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o",
              "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");
        }
    }
}
