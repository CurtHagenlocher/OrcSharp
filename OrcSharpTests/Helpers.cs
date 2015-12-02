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
    using System.Collections.Generic;
    using System.IO;
    using System.Numerics;
    using System.Reflection;
    using System.Text;
    using System.Threading;
    using Xunit;

    static class TestHelpers
    {
        public static readonly string ResourcesDirectory = GetResourcesDirectory();

        public static readonly string[] words = new string[]
            {
                "It", "was", "the", "best", "of", "times,",
                "it", "was", "the", "worst", "of", "times,", "it", "was", "the", "age",
                "of", "wisdom,", "it", "was", "the", "age", "of", "foolishness,", "it",
                "was", "the", "epoch", "of", "belief,", "it", "was", "the", "epoch",
                "of", "incredulity,", "it", "was", "the", "season", "of", "Light,",
                "it", "was", "the", "season", "of", "Darkness,", "it", "was", "the",
                "spring", "of", "hope,", "it", "was", "the", "winter", "of", "despair,",
                "we", "had", "everything", "before", "us,", "we", "had", "nothing",
                "before", "us,", "we", "were", "all", "going", "direct", "to",
                "Heaven,", "we", "were", "all", "going", "direct", "the", "other",
                "way"
            };

        public static void CompareFilesByLine(string expected, string actual)
        {
            using (StreamReader eStream = File.OpenText(Path.Combine(TestHelpers.ResourcesDirectory, expected)))
            using (StreamReader aStream = File.OpenText(actual))
            {
                string expectedLine = eStream.ReadLine().Trim();
                while (expectedLine != null)
                {
                    string actualLine = aStream.ReadLine().Trim();
                    System.Console.WriteLine("actual:   " + actualLine);
                    System.Console.WriteLine("expected: " + expectedLine);
                    Assert.Equal(expectedLine, actualLine);
                    expectedLine = eStream.ReadLine();
                    expectedLine = expectedLine == null ? null : expectedLine.Trim();
                }
                Assert.Null(eStream.ReadLine());
                Assert.Null(aStream.ReadLine());
            }
        }

        public static long NextLong(this Random rng, long n)
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

        public static BigInteger NextBigInteger(this Random rand, int bits)
        {
            byte[] tmp = new byte[(int)Math.Ceiling(bits / 8.0)];
            rand.NextBytes(tmp);
            BigInteger result = new BigInteger(tmp);
            return result < 0 ? -result : result;
        }

        public static byte[] getBytes(this string s)
        {
            return Encoding.UTF8.GetBytes(s);
        }

        static string GetResourcesDirectory()
        {
            string codebase = Assembly.GetExecutingAssembly().EscapedCodeBase;
            string location = (new Uri(codebase)).LocalPath;
            return Path.Combine(Path.GetDirectoryName(location), "resources");
        }

        public static IDisposable SetTimeZoneInfo(string timeZone)
        {
            return OverrideTimeZone.SetTimeZoneInfo(TimeZoneInfo.FindSystemTimeZoneById(timeZone));
        }

        class OverrideTimeZone : IDisposable
        {
            static Dictionary<int, TimeZoneInfo> timeZones = new Dictionary<int, TimeZoneInfo>();

            private readonly int threadId;

            OverrideTimeZone(int threadId)
            {
                this.threadId = threadId;
            }

            public static IDisposable SetTimeZoneInfo(TimeZoneInfo timeZone)
            {
                lock (timeZones)
                {
                    TreeReaderFactory.CreateTimeZone = GetTimeZoneInfo;
                    timeZones[Thread.CurrentThread.ManagedThreadId] = timeZone;
                    Thread.MemoryBarrier();
                }
                return new OverrideTimeZone(Thread.CurrentThread.ManagedThreadId);
            }

            static TimeZoneInfo GetTimeZoneInfo()
            {
                lock (timeZones)
                {
                    TimeZoneInfo result;
                    if (!timeZones.TryGetValue(Thread.CurrentThread.ManagedThreadId, out result))
                    {
                        result = TimeZoneInfo.Local;
                    }
                    return result;
                }
            }

            void IDisposable.Dispose()
            {
                lock (timeZones)
                {
                    timeZones.Remove(threadId);
                    if (timeZones.Count == 0)
                    {
                        TreeReaderFactory.CreateTimeZone = null;
                        Thread.MemoryBarrier();
                    }
                }
            }
        }
    }
}
