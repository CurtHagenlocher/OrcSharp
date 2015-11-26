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
    using System;

    static class TestHelpers
    {
        public static long nextLong(this Random rng, long n)
        {
            byte[] tmp = new byte[8];
            long bits, val;
            do
            {
                rng.NextBytes(tmp);
                bits = (long)((ulong)(BitConverter.ToInt64(tmp, 0) << 1) >> 1);
                val = bits % n;
            } while (bits - val + (n - 1) < 0L);
            return val;
        }

        public static long NextLong(this Random random)
        {
            return random.Next() << 32 | random.Next();
        }

        public static float NextFloat(this Random random)
        {
            byte[] buffer = new byte[4];
            random.NextBytes(buffer);
            return BitConverter.ToSingle(buffer, 0);
        }
    }
}
