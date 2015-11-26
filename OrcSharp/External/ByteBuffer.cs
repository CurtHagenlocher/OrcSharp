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
    using System.IO;

    public class ByteBuffer
    {
        private byte[] buffer;
        int _offset;
        int _limit;
        int _position;
        int _mark;

        private ByteBuffer(int capacity)
        {
            buffer = new byte[capacity];
            _limit = capacity;
        }

        private ByteBuffer(byte[] sharedBuffer)
        {
            buffer = sharedBuffer;
        }

        public static ByteBuffer allocate(int capacity)
        {
            return new ByteBuffer(capacity);
        }

        public static ByteBuffer allocateDirect(int capacity)
        {
            return new ByteBuffer(capacity);
        }

        public void clear()
        {
            _position = _offset;
            _limit = buffer.Length;
            _mark = 0;
        }

        public ByteBuffer duplicate()
        {
            ByteBuffer result = new ByteBuffer(buffer);
            result._offset = _offset;
            result._position = _position;
            result._limit = _limit;
            result._mark = _mark;
            return result;
        }

        public byte get()
        {
            if (_position >= _limit)
            {
                throw new InternalBufferOverflowException();
            }
            return buffer[_position++];
        }

        public ByteBuffer get(byte[] buffer, int offset, int length)
        {
            if (length > remaining())
            {
                throw new ArgumentException();
            }
            Array.Copy(this.buffer, _position, buffer, offset, length);
            _position += length;
            return this;
        }

        public bool isDirect()
        {
            return false;
        }

        public int limit()
        {
            return _limit - _offset;
        }

        public ByteBuffer limit(int newLimit)
        {
            _limit = newLimit + _offset;
            return this;
        }

        public int position()
        {
            return _position - _offset;
        }

        public ByteBuffer position(int newPosition)
        {
            _position = newPosition + _offset;
            return this;
        }

        public ByteBuffer put(ByteBuffer src)
        {
            throw new NotImplementedException();
        }

        public int remaining()
        {
            return _limit - _position;
        }

        public ByteBuffer slice()
        {
            ByteBuffer result = new ByteBuffer(buffer);
            result._offset = _position;
            result._position = _position;
            result._limit = _limit;
            result._mark = _mark;
            return result;
        }

        internal static ByteBuffer wrap(byte[] buffer)
        {
            ByteBuffer result = new ByteBuffer(buffer);
            result._offset = 0;
            result._position = 0;
            result._limit = buffer.Length;
            return result;
        }

        internal byte[] array()
        {
            return this.buffer;
        }

        internal int arrayOffset()
        {
            return this._offset;
        }

        internal void put(int position, byte p)
        {
            throw new NotImplementedException();
        }

        internal void put(byte p)
        {
            throw new NotImplementedException();
        }

        internal void put(byte[] bytes, int offset, int length)
        {
            if (length > remaining())
            {
                throw new InternalBufferOverflowException();
            }
            Array.Copy(bytes, offset, buffer, _position, length);
            _position += length;
        }

        internal int capacity()
        {
            throw new NotImplementedException();
        }

        internal void flip()
        {
            _limit = _position;
            _position = _offset;
            _mark = 0;
        }

        internal byte[] contents()
        {
            throw new NotImplementedException();
        }

        internal int get(int lastByteAbsPos)
        {
            return this.buffer[_offset + lastByteAbsPos];
        }

        internal void mark()
        {
            _mark = _position;
        }

        internal void put(byte[] buffer)
        {
            throw new NotImplementedException();
        }

        internal bool hasArray()
        {
            return true;
        }

        internal int readRemaining(Stream file)
        {
            throw new NotImplementedException();
        }
    }
}
