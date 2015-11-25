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
    using OrcProto = global::orc.proto;

    public class TestStreamName
    {
        [Fact]
        public void test1()
        {
            StreamName s1 = new StreamName(3, OrcProto.Stream.Types.Kind.DATA);
            StreamName s2 = new StreamName(3,
                OrcProto.Stream.Types.Kind.DICTIONARY_DATA);
            StreamName s3 = new StreamName(5, OrcProto.Stream.Types.Kind.DATA);
            StreamName s4 = new StreamName(5,
                OrcProto.Stream.Types.Kind.DICTIONARY_DATA);
            StreamName s1p = new StreamName(3, OrcProto.Stream.Types.Kind.DATA);
            Assert.Equal(true, s1.Equals(s1));
            Assert.Equal(false, s1.Equals(s2));
            Assert.Equal(false, s1.Equals(s3));
            Assert.Equal(true, s1.Equals(s1p));
            Assert.Equal(true, s1.CompareTo(null) < 0);
            Assert.Equal(false, s1.Equals(null));
            Assert.Equal(true, s1.CompareTo(s2) < 0);
            Assert.Equal(true, s2.CompareTo(s3) < 0);
            Assert.Equal(true, s3.CompareTo(s4) < 0);
            Assert.Equal(true, s4.CompareTo(s1p) > 0);
            Assert.Equal(0, s1p.CompareTo(s1));
        }
    }
}
