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
    using Xunit;

    /**
     * Test the ORC memory manager.
     */
    public class TestMemoryManager
    {
        private static const double ERROR = 0.000001;

        private static class NullCallback : MemoryManager.Callback
        {
            public bool checkMemory(double newScale)
            {
                return false;
            }
        }

        [Fact]
        public void testBasics()
        {
            Configuration conf = new Configuration();
            MemoryManager mgr = new MemoryManager(conf);
            NullCallback callback = new NullCallback();
            long poolSize = mgr.getTotalMemoryPool();
            Assert.Equal(Math.round(ManagementFactory.getMemoryMXBean().
                getHeapMemoryUsage().getMax() * 0.5d), poolSize);
            Assert.Equal(1.0, mgr.getAllocationScale(), 0.00001);
            mgr.addWriter(new Path("p1"), 1000, callback);
            Assert.Equal(1.0, mgr.getAllocationScale(), 0.00001);
            mgr.addWriter(new Path("p1"), poolSize / 2, callback);
            Assert.Equal(1.0, mgr.getAllocationScale(), 0.00001);
            mgr.addWriter(new Path("p2"), poolSize / 2, callback);
            Assert.Equal(1.0, mgr.getAllocationScale(), 0.00001);
            mgr.addWriter(new Path("p3"), poolSize / 2, callback);
            Assert.Equal(0.6666667, mgr.getAllocationScale(), 0.00001);
            mgr.addWriter(new Path("p4"), poolSize / 2, callback);
            Assert.Equal(0.5, mgr.getAllocationScale(), 0.000001);
            mgr.addWriter(new Path("p4"), 3 * poolSize / 2, callback);
            Assert.Equal(0.3333333, mgr.getAllocationScale(), 0.000001);
            mgr.removeWriter(new Path("p1"));
            mgr.removeWriter(new Path("p2"));
            Assert.Equal(0.5, mgr.getAllocationScale(), 0.00001);
            mgr.removeWriter(new Path("p4"));
            Assert.Equal(1.0, mgr.getAllocationScale(), 0.00001);
        }

        [Fact]
        public void testConfig()
        {
            Configuration conf = new Configuration();
            conf.set("hive.exec.orc.memory.pool", "0.9");
            MemoryManager mgr = new MemoryManager(conf);
            long mem =
                ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
            System.err.print("Memory = " + mem);
            long pool = mgr.getTotalMemoryPool();
            assertTrue("Pool too small: " + pool, mem * 0.899 < pool);
            assertTrue("Pool too big: " + pool, pool < mem * 0.901);
        }

        private class DoubleMatcher : BaseMatcher<Double>
        {
            double expected;
            double error;

            DoubleMatcher(double expected, double error)
            {
                this.expected = expected;
                this.error = error;
            }

            public bool matches(Object val)
            {
                double dbl = (Double)val;
                return Math.abs(dbl - expected) <= error;
            }

            public void describeTo(Description description)
            {
                description.appendText("not sufficiently close to ");
                description.appendText(Double.toString(expected));
            }
        }

        private static DoubleMatcher closeTo(double value, double error)
        {
            return new DoubleMatcher(value, error);
        }

        [Fact]
        public void testCallback()
        {
            Configuration conf = new Configuration();
            MemoryManager mgr = new MemoryManager(conf);
            long pool = mgr.getTotalMemoryPool();
            MemoryManager.Callback[] calls = new MemoryManager.Callback[20];
            for (int i = 0; i < calls.length; ++i)
            {
                calls[i] = mock(typeof(MemoryManager.Callback));
                mgr.addWriter(new Path(Integer.toString(i)), pool / 4, calls[i]);
            }
            // add enough rows to get the memory manager to check the limits
            for (int i = 0; i < 10000; ++i)
            {
                mgr.addedRow(1);
            }
            for (int call = 0; call < calls.length; ++call)
            {
                verify(calls[call], times(2))
                    .checkMemory(doubleThat(closeTo(0.2, ERROR)));
            }
        }
    }
}
