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

namespace org.apache.hadoop.hive.ql.io.orc
{
    using System.IO;
    using System.Text;


    /**
     * A red-black tree that stores strings. The strings are stored as UTF-8 bytes
     * and an offset for each entry.
     */
    public class StringRedBlackTree : RedBlackTree<byte[]>
    {
        private DynamicByteArray byteArray = new DynamicByteArray();
        private DynamicIntArray keyOffsets;

        public StringRedBlackTree(int initialCapacity)
            : base(initialCapacity)
        {
            keyOffsets = new DynamicIntArray(initialCapacity);
        }

        public int add(string value)
        {
            return add(Encoding.UTF8.GetBytes(value));
        }

        public int add(byte[] bytes)
        {
            // if the newKey is actually new, add it to our byteArray and store the offset & length
            if (addBase(bytes))
            {
                keyOffsets.add(byteArray.add(bytes, 0, bytes.Length));
            }
            return lastAdd;
        }

        protected override int compareValue(int position, byte[] value)
        {
            int start = keyOffsets.get(position);
            int end;
            if (position + 1 == keyOffsets.size())
            {
                end = byteArray.size();
            }
            else
            {
                end = keyOffsets.get(position + 1);
            }
            return byteArray.compare(value, 0, value.Length, start, end - start);
        }

        /**
         * The information about each node.
         */
        public interface VisitorContext
        {
            /**
             * Get the position where the key was originally added.
             * @return the number returned by add.
             */
            int getOriginalPosition();

            /**
             * Write the bytes for the string to the given output stream.
             * @param out the stream to write to.
             * @
             */
            void writeBytes(Stream @out);

            /**
             * Get the original string.
             * @return the string
             */
            string getText();

            /**
             * Get the number of bytes.
             * @return the string's length in bytes
             */
            int getLength();
        }

        /**
         * The interface for visitors.
         */
        public interface Visitor
        {
            /**
             * Called once for each node of the tree in sort order.
             * @param context the information about each node
             * @
             */
            void visit(VisitorContext context);
        }

        private class VisitorContextImpl : VisitorContext
        {
            private StringRedBlackTree tree;
            private int originalPosition;
            private int start;
            private int end;

            public VisitorContextImpl(StringRedBlackTree tree)
            {
                this.tree = tree;
            }

            public int getOriginalPosition()
            {
                return originalPosition;
            }

            public string getText()
            {
                string text;
                tree.byteArray.setText(out text, start, end - start);
                return text;
            }

            public void writeBytes(Stream @out)
            {
                tree.byteArray.write(@out, start, end - start);
            }

            public int getLength()
            {
                return end - start;
            }

            public void setPosition(int position)
            {
                originalPosition = position;
                start = tree.keyOffsets.get(originalPosition);
                if (position + 1 == tree.keyOffsets.size())
                {
                    end = tree.byteArray.size();
                }
                else
                {
                    end = tree.keyOffsets.get(originalPosition + 1);
                }
            }
        }

        private void recurse(int node, Visitor visitor, VisitorContextImpl context
                            )
        {
            if (node != NULL)
            {
                recurse(getLeft(node), visitor, context);
                context.setPosition(node);
                visitor.visit(context);
                recurse(getRight(node), visitor, context);
            }
        }

        /**
         * Visit all of the nodes in the tree in sorted order.
         * @param visitor the action to be applied to each node
         * @
         */
        public void visit(Visitor visitor)
        {
            recurse(root, visitor, new VisitorContextImpl(this));
        }

        /**
         * Reset the table to empty.
         */
        public override void clear()
        {
            base.clear();
            byteArray.clear();
            keyOffsets.clear();
        }

        public string getText(int originalPosition)
        {
            int offset = keyOffsets.get(originalPosition);
            int length;
            if (originalPosition + 1 == keyOffsets.size())
            {
                length = byteArray.size() - offset;
            }
            else
            {
                length = keyOffsets.get(originalPosition + 1) - offset;
            }
            string result;
            byteArray.setText(out result, offset, length);
            return result;
        }

        /**
         * Get the size of the character data in the table.
         * @return the bytes used by the table
         */
        public int getCharacterSize()
        {
            return byteArray.size();
        }

        /**
         * Calculate the approximate size in memory.
         * @return the number of bytes used in storing the tree.
         */
        public override long getSizeInBytes()
        {
            return byteArray.getSizeInBytes() + keyOffsets.getSizeInBytes() +
              base.getSizeInBytes();
        }
    }
}
