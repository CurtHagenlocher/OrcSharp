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
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using OrcSharp;
    using OrcSharp.Serialization;
    using OrcSharp.Types;
    using Xunit;

    /**
     *
     */
    public class TestOrcTimezone2 : WithLocalDirectory
    {
        const string testFileName = "TestOrcTimezone1.orc";

        public TestOrcTimezone2()
            : base(testFileName)
        {
        }

        [Fact]
        public void testTimestampWriter()
        {
            string[] allTimeZones = TimeZoneInfo.GetSystemTimeZones().Select(tz => tz.Id).ToArray();
            Random rand = new Random(123);
            int len = allTimeZones.Length;
            int n = 500;
            for (int i = 0; i < n; i++)
            {
                int wIdx = rand.Next(len);
                int rIdx = rand.Next(len);
                testTimestampWriter(allTimeZones[wIdx], allTimeZones[rIdx]);
            }
        }

        private void testTimestampWriter(string writerTimeZone, string readerTimeZone)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(Timestamp));
            List<string> ts = new List<string>();

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .inspector(inspector)
                .stripeSize(100000)
                .bufferSize(10000)))
            using (TestHelpers.SetTimeZoneInfo(writerTimeZone))
            {

                ts.Add("2003-01-01 01:00:00.000000222");
                ts.Add("1999-01-01 02:00:00.999999999");
                ts.Add("1995-01-02 03:00:00.688888888");
                ts.Add("2002-01-01 04:00:00.1");
                ts.Add("2010-03-02 05:00:00.000009001");
                ts.Add("2005-01-01 06:00:00.000002229");
                ts.Add("2006-01-01 07:00:00.900203003");
                ts.Add("2003-01-01 08:00:00.800000007");
                ts.Add("1996-08-02 09:00:00.723100809");
                ts.Add("1998-11-02 10:00:00.857340643");
                ts.Add("2008-10-02 11:00:00.0");
                ts.Add("2037-01-01 00:00:00.000999");
                foreach (string t in ts)
                {
                    writer.addRow(Timestamp.Parse(t));
                }
            }

            using (TestHelpers.SetTimeZoneInfo(readerTimeZone))
            {
                Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
                using (RecordReader rows = reader.rows(null))
                {
                    int idx = 0;
                    while (rows.hasNext())
                    {
                        object row = rows.next();
                        Timestamp got = ((Timestamp)row);
                        Assert.Equal(ts[idx++], got.ToString());
                    }
                }
            }
        }
    }
}
