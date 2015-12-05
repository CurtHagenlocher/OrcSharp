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
namespace OrcSharpTests
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Numerics;
    using System.Text;
    using OrcSharp;
    using Xunit;

    public class TestSerializationUtils
    {
        private MemoryStream fromBuffer(MemoryStream buffer)
        {
            return new MemoryStream(buffer.ToArray());
        }

        [Fact]
        public void testDoubles()
        {
            int tolerance = 15;
            MemoryStream buffer = new MemoryStream();
            SerializationUtils utils = new SerializationUtils();
            utils.writeDouble(buffer, 1343822337.759);
            Assert.Equal(1343822337.759, utils.readDouble(fromBuffer(buffer)), tolerance);
            buffer = new MemoryStream();
            utils.writeDouble(buffer, 0.8);
            double got = utils.readDouble(fromBuffer(buffer));
            Assert.Equal(0.8, got, tolerance);
        }

        [Fact]
        public void testBigIntegers()
        {
            MemoryStream buffer = new MemoryStream();
            SerializationUtils.writeBigInteger(buffer, new BigInteger(0));
            Assert.Equal(new byte[] { 0 }, buffer.ToArray());
            Assert.Equal(0L,
                (long)SerializationUtils.readBigInteger(fromBuffer(buffer)));
            buffer.reset();
            SerializationUtils.writeBigInteger(buffer, new BigInteger(1));
            Assert.Equal(new byte[] { 2 }, buffer.ToArray());
            Assert.Equal(1L,
                (long)SerializationUtils.readBigInteger(fromBuffer(buffer)));
            buffer.reset();
            SerializationUtils.writeBigInteger(buffer, new BigInteger(-1));
            Assert.Equal(new byte[] { 1 }, buffer.ToArray());
            Assert.Equal(-1L,
                (long)SerializationUtils.readBigInteger(fromBuffer(buffer)));
            buffer.reset();
            SerializationUtils.writeBigInteger(buffer, new BigInteger(50));
            Assert.Equal(new byte[] { 100 }, buffer.ToArray());
            Assert.Equal(50L,
                (long)SerializationUtils.readBigInteger(fromBuffer(buffer)));
            buffer.reset();
            SerializationUtils.writeBigInteger(buffer, new BigInteger(-50));
            Assert.Equal(new byte[] { 99 }, buffer.ToArray());
            Assert.Equal(-50L,
                (long)SerializationUtils.readBigInteger(fromBuffer(buffer)));
            for (int i = -8192; i < 8192; ++i)
            {
                buffer.reset();
                SerializationUtils.writeBigInteger(buffer, new BigInteger(i));
                Assert.Equal(i >= -64 && i < 64 ? 1 : 2, buffer.Length);
                Assert.Equal(i, (int)SerializationUtils.readBigInteger(fromBuffer(buffer)));
            }
            buffer.reset();
            SerializationUtils.writeBigInteger(buffer,
                BigInteger.Parse("123456789abcdef0", NumberStyles.HexNumber));
            Assert.Equal(BigInteger.Parse("123456789abcdef0", NumberStyles.HexNumber),
                SerializationUtils.readBigInteger(fromBuffer(buffer)));
            buffer.reset();
            SerializationUtils.writeBigInteger(buffer,
                BigInteger.Parse("-1234567898765432"));
            Assert.Equal(BigInteger.Parse("-1234567898765432"),
                SerializationUtils.readBigInteger(fromBuffer(buffer)));
            StringBuilder buf = new StringBuilder();
            for (int i = 0; i < 256; ++i)
            {
                buf.AppendFormat(CultureInfo.InvariantCulture, "{0:X2}", i);
            }
            buffer.reset();
            SerializationUtils.writeBigInteger(buffer,
                BigInteger.Parse(buf.ToString(), NumberStyles.HexNumber));
            Assert.Equal(BigInteger.Parse(buf.ToString(), NumberStyles.HexNumber),
                SerializationUtils.readBigInteger(fromBuffer(buffer)));
            buffer.reset();
            SerializationUtils.writeBigInteger(buffer,
                BigInteger.Parse("ff000000000000000000000000000000000000000000ff", NumberStyles.HexNumber));
            Assert.Equal(
                BigInteger.Parse("ff000000000000000000000000000000000000000000ff", NumberStyles.HexNumber),
                SerializationUtils.readBigInteger(fromBuffer(buffer)));
        }

        [Fact]
        public void testSubtractionOverflow()
        {
            // cross check results with Guava results below
            SerializationUtils utils = new SerializationUtils();
            Assert.Equal(false, utils.isSafeSubtract(22222222222L, Int64.MinValue));
            Assert.Equal(false, utils.isSafeSubtract(-22222222222L, Int64.MaxValue));
            Assert.Equal(false, utils.isSafeSubtract(Int64.MinValue, Int64.MaxValue));
            Assert.Equal(true, utils.isSafeSubtract(-1553103058346370095L, 6553103058346370095L));
            Assert.Equal(true, utils.isSafeSubtract(0, Int64.MaxValue));
            Assert.Equal(true, utils.isSafeSubtract(Int64.MinValue, 0));
        }
    }

    static class SerializationTestHelpers
    {
        public static void reset(this MemoryStream stream)
        {
            stream.Position = 0;
            stream.SetLength(0);
        }
    }
}
