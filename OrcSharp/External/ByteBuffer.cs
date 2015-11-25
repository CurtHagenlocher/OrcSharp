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

namespace org.apache.hadoop.hive.ql.io.orc.external
{
    using System;

    public class ByteBuffer
    {
        public static ByteBuffer allocate(int capacity)
        {
            throw new NotImplementedException();
        }

        public static ByteBuffer allocateDirect(int capacity)
        {
            throw new NotImplementedException();
        }

        public void clear()
        {
            throw new NotImplementedException();
        }

        public ByteBuffer duplicate()
        {
            throw new NotImplementedException();
        }

        public int get()
        {
            throw new NotImplementedException();
        }

        public int get(byte[] buffer, int offset, int length)
        {
            throw new NotImplementedException();
        }

        public bool isDirect()
        {
            throw new NotImplementedException();
        }

        public int limit()
        {
            throw new NotImplementedException();
        }

        public ByteBuffer limit(int newLimit)
        {
            throw new NotImplementedException();
        }

        public int position()
        {
            throw new NotImplementedException();
        }

        public ByteBuffer position(int newPosition)
        {
            throw new NotImplementedException();
        }

        public ByteBuffer put(ByteBuffer src)
        {
            throw new NotImplementedException();
        }

        public int remaining()
        {
            throw new NotImplementedException();
        }

        public ByteBuffer slice()
        {
            throw new NotImplementedException();
        }

        internal static ByteBuffer wrap(byte[] buffer)
        {
            throw new NotImplementedException();
        }

        internal byte[] array()
        {
            throw new NotImplementedException();
        }

        internal int arrayOffset()
        {
            throw new NotImplementedException();
        }

        internal void put(int position, byte p)
        {
            throw new NotImplementedException();
        }

        internal void put(byte p)
        {
            throw new NotImplementedException();
        }

        internal void put(byte[] bytes, int offset, int remaining)
        {
            throw new NotImplementedException();
        }

        internal int capacity()
        {
            throw new NotImplementedException();
        }

        internal void flip()
        {
            throw new NotImplementedException();
        }

        internal byte[] contents()
        {
            throw new NotImplementedException();
        }

        internal int get(int lastByteAbsPos)
        {
            throw new NotImplementedException();
        }

        internal void mark()
        {
            throw new NotImplementedException();
        }

        internal void put(byte[] buffer)
        {
            throw new NotImplementedException();
        }

        internal bool hasArray()
        {
            throw new NotImplementedException();
        }
    }
}
