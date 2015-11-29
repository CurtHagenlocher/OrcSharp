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

    class JavaDataModel
    {
        // TODO: Replace with more-appropriate CLR values

        public const int Four = 4;
        public const int Eight = 8;

        // Test suites assume 64-bit operation
        static readonly int objectSize = 32; // 4 * IntPtr.Size;
        static readonly int arraySize = 40; // 5 * IntPtr.Size;

        internal static long lengthForStringOfLength(int strLen)
        {
            return objectSize + Four * 3 + arraySize + strLen;
        }

        internal static long lengthOfTimestamp()
        {
            // object overhead + 4 bytes for int (nanos) + 4 bytes of padding
            return objectSize + Eight;
        }

        internal static long lengthOfDate()
        {
            // object overhead + 8 bytes for long (fastTime) + 16 bytes for cdate
            return objectSize + 3 * Eight;
        }

        internal static long lengthOfDecimal()
        {
            // object overhead + 8 bytes for intCompact + 4 bytes for precision
            // + 4 bytes for scale + size of BigInteger
            return objectSize + 2 * Eight + lengthOfBigInteger();
        }

        private static int lengthOfBigInteger()
        {
            // object overhead + 4 bytes for bitCount + 4 bytes for bitLength
            // + 4 bytes for firstNonzeroByteNum + 4 bytes for firstNonzeroIntNum +
            // + 4 bytes for lowestSetBit + 5 bytes for size of magnitude (since max precision
            // is only 38 for HiveDecimal) + 7 bytes of padding (since java memory allocations
            // are 8 byte aligned)
            return objectSize + 4 * Eight;
        }
    }
}
