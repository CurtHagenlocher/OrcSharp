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
    using System.IO;
    using System.Linq;
    using OrcSharp;
    using OrcSharp.Serialization;
    using OrcSharp.Types;
    using Xunit;

    public class TestOrcTimezone1 : WithLocalDirectory
    {
        const string testFileName = "TestOrcTimezone1.orc";

        public TestOrcTimezone1()
            : base(testFileName)
        {
        }

        public static IEnumerable<object[]> TimeZoneData = new[]
        {
            /* Extreme timezones */
            new object[] {"GMT-12:00", "GMT+14:00"},
            /* No difference in DST */
            new object[] {"America/Los_Angeles", "America/Los_Angeles"}, /* same timezone both with DST */
            new object[] {"Europe/Berlin", "Europe/Berlin"}, /* same as above but europe */
            new object[] {"America/Phoenix", "Asia/Kolkata"} /* Writer no DST, Reader no DST */,
            new object[] {"Europe/Berlin", "America/Los_Angeles"} /* Writer DST, Reader DST */,
            new object[] {"Europe/Berlin", "America/Chicago"} /* Writer DST, Reader DST */,
            /* With DST difference */
            new object[] {"Europe/Berlin", "UTC"},
            new object[] {"UTC", "Europe/Berlin"} /* Writer no DST, Reader DST */,
            new object[] {"America/Los_Angeles", "Asia/Kolkata"} /* Writer DST, Reader no DST */,
            new object[] {"Europe/Berlin", "Asia/Kolkata"} /* Writer DST, Reader no DST */,
            /* Timezone offsets for the reader has changed historically */
            new object[] {"Asia/Saigon", "Pacific/Enderbury"},
            new object[] {"UTC", "Asia/Jerusalem"},

            // NOTE:
            // "1995-01-01 03:00:00.688888888" this is not a valid time in Pacific/Enderbury timezone.
            // On 1995-01-01 00:00:00 GMT offset moved from -11:00 hr to +13:00 which makes all values
            // on 1995-01-01 invalid. Try this with joda time
            // new MutableDateTime("1995-01-01", DateTimeZone.forTimeZone(readerTimeZone));
        };

        public static IEnumerable<object[]> ReaderTimeZoneData = TimeZoneData.Select(o => (string)o[1]).Distinct().Select(s => new object[] { s });

        [Theory, MemberData("TimeZoneData")]
        public void testTimestampWriter(string writerTimeZone, string readerTimeZone)
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
                ts.Add("1996-08-02 09:00:00.723100809");
                ts.Add("1999-01-01 02:00:00.999999999");
                ts.Add("1995-01-02 03:00:00.688888888");
                ts.Add("2002-01-01 04:00:00.1");
                ts.Add("2010-03-02 05:00:00.000009001");
                ts.Add("2005-01-01 06:00:00.000002229");
                ts.Add("2006-01-01 07:00:00.900203003");
                ts.Add("2003-01-01 08:00:00.800000007");
                ts.Add("1998-11-02 10:00:00.857340643");
                ts.Add("2008-10-02 11:00:00.0");
                ts.Add("2037-01-01 00:00:00.000999");
                ts.Add("2014-03-28 00:00:00.0");
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

        [Theory, MemberData("ReaderTimeZoneData")]
        public void testReadTimestampFormat_0_11(string readerTimeZone)
        {
            string oldFilePath = Path.Combine(TestHelpers.ResourcesDirectory, "orc-file-11-format.orc");
            using (TestHelpers.SetTimeZoneInfo(readerTimeZone))
            {
                Reader reader = OrcFile.createReader(oldFilePath, OrcFile.readerOptions(conf));

                StructObjectInspector readerInspector = (StructObjectInspector)reader.getObjectInspector();
                IList<StructField> fields = readerInspector.getAllStructFieldRefs();
                TimestampObjectInspector tso = (TimestampObjectInspector)readerInspector
                    .getStructFieldRef("ts").getFieldObjectInspector();

                using (RecordReader rows = reader.rows())
                {
                    object row = rows.next();
                    Assert.NotNull(row);
                    Assert.Equal(Timestamp.Parse("2000-03-12 15:00:00"),
                        tso.getPrimitiveJavaObject(readerInspector.getStructFieldData(row,
                            fields[12])));

                    // check the contents of second row
                    Assert.Equal(true, rows.hasNext());
                    rows.seekToRow(7499);
                    row = rows.next();
                    Assert.Equal(Timestamp.Parse("2000-03-12 15:00:01"),
                        tso.getPrimitiveJavaObject(readerInspector.getStructFieldData(row,
                            fields[12])));

                    Assert.Equal(false, rows.hasNext());
                }
            }
        }
    }
}
