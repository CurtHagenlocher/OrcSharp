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
    using System.Collections.Generic;
    using OrcSharp;
    using OrcSharp.External;
    using Xunit;

    /**
     * Test the ORC memory manager.
     */
    public class TestMemoryManager
    {
        private const int ERROR = 6; // 0.000001
        private const long configuredPoolSize = 9000000;

        private class NullCallback : MemoryManager.Callback
        {
            public bool checkMemory(double newScale)
            {
                return false;
            }
        }

        private class LoggingCallback : MemoryManager.Callback
        {
            private readonly List<double> log = new List<double>();

            public bool checkMemory(double newScale)
            {
                log.Add(newScale);
                return false;
            }

            public int LogLength {  get { return this.log.Count; } }

            public IEnumerable<double> Log { get { return this.log; } }
        }

        [Fact]
        public void testBasics()
        {
            MemoryManager mgr = new MemoryManager(configuredPoolSize);
            NullCallback callback = new NullCallback();
            long poolSize = mgr.getTotalMemoryPool();
            Assert.Equal(configuredPoolSize, poolSize);
            Assert.Equal(1.0, mgr.getAllocationScale(), 5);
            mgr.addWriter("p1", 1000, callback);
            Assert.Equal(1.0, mgr.getAllocationScale(), 5);
            mgr.addWriter("p1", poolSize / 2, callback);
            Assert.Equal(1.0, mgr.getAllocationScale(), 5);
            mgr.addWriter("p2", poolSize / 2, callback);
            Assert.Equal(1.0, mgr.getAllocationScale(), 5);
            mgr.addWriter("p3", poolSize / 2, callback);
            Assert.Equal(0.6666667, mgr.getAllocationScale(), 5);
            mgr.addWriter("p4", poolSize / 2, callback);
            Assert.Equal(0.5, mgr.getAllocationScale(), 6);
            mgr.addWriter("p4", 3 * poolSize / 2, callback);
            Assert.Equal(0.3333333, mgr.getAllocationScale(), 6);
            mgr.removeWriter("p1");
            mgr.removeWriter("p2");
            Assert.Equal(0.5, mgr.getAllocationScale(), 5);
            mgr.removeWriter("p4");
            Assert.Equal(1.0, mgr.getAllocationScale(), 5);
        }

#if MEMORYBEAN
        [Fact]
        public void testConfig()
        {
            Configuration conf = new Configuration();
            conf.set("hive.exec.orc.memory.pool", "0.9");
            MemoryManager mgr = new MemoryManager(conf);
            long mem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
            System.Console.WriteLine("Memory = " + mem);
            long pool = mgr.getTotalMemoryPool();
            Assert.True("Pool too small: " + pool, mem * 0.899 < pool);
            Assert.True("Pool too big: " + pool, pool < mem * 0.901);
        }
#endif

        [Fact]
        public void testCallback()
        {
            Configuration conf = new Configuration();
            MemoryManager mgr = new MemoryManager(configuredPoolSize);
            long pool = mgr.getTotalMemoryPool();
            LoggingCallback[] calls = new LoggingCallback[20];
            for (int i = 0; i < calls.Length; ++i)
            {
                calls[i] = new LoggingCallback();
                mgr.addWriter(i.ToString(), pool / 4, calls[i]);
            }
            // add enough rows to get the memory manager to check the limits
            for (int i = 0; i < 10000; ++i)
            {
                mgr.addedRow(1);
            }
            for (int call = 0; call < calls.Length; ++call)
            {
                Assert.Equal(2, calls[call].LogLength);
                foreach (double argument in calls[call].Log)
                {
                    Assert.Equal(0.2, argument, ERROR);
                }
            }
        }
    }
}
