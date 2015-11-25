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
    using System.Numerics;

    public class HiveDecimal
    {
        public const int MAX_PRECISION = 38;
        public const int MAX_SCALE = 38;
        public const int SYSTEM_DEFAULT_PRECISION = 38;
        public const int SYSTEM_DEFAULT_SCALE = 28;
        public const int USER_DEFAULT_PRECISION = 10;
        public const int USER_DEFAULT_SCALE = 0;

        public static readonly HiveDecimal Zero = new HiveDecimal();
        public static readonly HiveDecimal One = HiveDecimal.create(BigInteger.One, 0);

        BigInteger mantissa;
        int scale;

        public static HiveDecimal create(BigInteger mantissa, int scale)
        {
            throw new NotImplementedException();
        }

        public static HiveDecimal Parse(string text)
        {
            throw new NotImplementedException();
        }

        public static HiveDecimal operator +(HiveDecimal left, HiveDecimal right)
        {
            return Zero;
        }

        public int CompareTo(HiveDecimal value)
        {
            return 0;
        }

        public override string ToString()
        {
            return base.ToString();
        }

        public static HiveDecimal enforcePrecisionScale(HiveDecimal hiveDec, short precision, short scale)
        {
            throw new NotImplementedException();
        }
    }
}
