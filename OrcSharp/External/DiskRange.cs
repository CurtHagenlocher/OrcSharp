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

namespace OrcSharp.External
{
    using System;
    using System.Diagnostics;

    /**
     * The sections of a file.
     */
    public class DiskRange
    {
        /** The first address. */
        protected long offset;
        /** The address afterwards. */
        protected long end;

        public DiskRange(long offset, long end)
        {
            this.offset = offset;
            this.end = end;
            if (end < offset)
            {
                throw new ArgumentException("invalid range " + this);
            }
        }

        public override bool Equals(object other)
        {
            DiskRange range = other as DiskRange;
            if (range == null || other.GetType() != GetType())
            {
                return false;
            }
            return equalRange((DiskRange)other);
        }

        public bool equalRange(DiskRange other)
        {
            return other.offset == offset && other.end == end;
        }

        public override int GetHashCode()
        {
            return (int)(offset ^ (offset >> 32)) * 31 + (int)(end ^ (end >> 32));
        }

        public override string ToString()
        {
            return "range start: " + offset + " end: " + end;
        }

        public long getOffset()
        {
            return offset;
        }

        public long getEnd()
        {
            return end;
        }

        public int getLength()
        {
            long len = this.end - this.offset;
            Debug.Assert(len <= Int32.MaxValue);
            return (int)len;
        }

        // For subclasses
        public virtual bool hasData()
        {
            return false;
        }

        public virtual DiskRange sliceAndShift(long offset, long end, long shiftBy)
        {
            // Rather, unexpected usage exception.
            throw new NotSupportedException();
        }

        public virtual ByteBuffer getData()
        {
            throw new NotSupportedException();
        }

        protected bool merge(long otherOffset, long otherEnd)
        {
            if (!overlap(offset, end, otherOffset, otherEnd)) return false;
            offset = Math.Min(offset, otherOffset);
            end = Math.Max(end, otherEnd);
            return true;
        }

        private static bool overlap(long leftA, long rightA, long leftB, long rightB)
        {
            if (leftA <= leftB)
            {
                return rightA >= leftB;
            }
            return rightB >= leftA;
        }
    }
}
