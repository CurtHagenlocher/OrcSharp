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
    using System.Collections.Generic;
    using System.IO;
    using OrcSharp.External;
    using OrcSharp.Serialization;
    using OrcSharp.Types;
    using Xunit;

    /**
     * Test ColumnStatisticsImpl for ORC.
     */
    public class TestColumnStatistics : WithLocalDirectory
    {
        const string testFileName = "TestColumnStatistics.orc";

        public TestColumnStatistics()
            : base(testFileName)
        {
        }

        [Fact]
        public void testLongMerge()
        {
            TypeDescription schema = TypeDescription.createInt();

            ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
            ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
            stats1.updateInteger(10, 2);
            stats2.updateInteger(1, 1);
            stats2.updateInteger(1000, 1);
            stats1.merge(stats2);
            IntegerColumnStatistics typed = (IntegerColumnStatistics)stats1;
            Assert.Equal(1, typed.getMinimum());
            Assert.Equal(1000, typed.getMaximum());
            stats1.reset();
            stats1.updateInteger(-10, 1);
            stats1.updateInteger(10000, 1);
            stats1.merge(stats2);
            Assert.Equal(-10, typed.getMinimum());
            Assert.Equal(10000, typed.getMaximum());
        }

        [Fact]
        public void testDoubleMerge()
        {
            TypeDescription schema = TypeDescription.createDouble();

            ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
            ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
            stats1.updateDouble(10.0);
            stats1.updateDouble(100.0);
            stats2.updateDouble(1.0);
            stats2.updateDouble(1000.0);
            stats1.merge(stats2);
            DoubleColumnStatistics typed = (DoubleColumnStatistics)stats1;
            Assert.Equal(1.0, typed.getMinimum(), 3);
            Assert.Equal(1000.0, typed.getMaximum(), 3);
            stats1.reset();
            stats1.updateDouble(-10);
            stats1.updateDouble(10000);
            stats1.merge(stats2);
            Assert.Equal(-10, typed.getMinimum(), 3);
            Assert.Equal(10000, typed.getMaximum(), 3);
        }


        [Fact]
        public void testStringMerge()
        {
            TypeDescription schema = TypeDescription.createString();

            ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
            ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
            stats1.updateString("bob");
            stats1.updateString("david");
            stats1.updateString("charles");
            stats2.updateString("anne");
            byte[] erin = new byte[] { 0, 1, 2, 3, 4, 5, 101, 114, 105, 110 };
            stats2.updateString(erin, 6, 4, 5);
            Assert.Equal(24, ((StringColumnStatistics)stats2).getSum());
            stats1.merge(stats2);
            StringColumnStatistics typed = (StringColumnStatistics)stats1;
            Assert.Equal("anne", typed.getMinimum());
            Assert.Equal("erin", typed.getMaximum());
            Assert.Equal(39, typed.getSum());
            stats1.reset();
            stats1.updateString("aaa");
            stats1.updateString("zzz");
            stats1.merge(stats2);
            Assert.Equal("aaa", typed.getMinimum());
            Assert.Equal("zzz", typed.getMaximum());
        }

        [Fact]
        public void testDateMerge()
        {
            TypeDescription schema = TypeDescription.createDate();

            ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
            ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
            stats1.updateDate(1000);
            stats1.updateDate(100);
            stats2.updateDate(10);
            stats2.updateDate(2000);
            stats1.merge(stats2);
            DateColumnStatistics typed = (DateColumnStatistics)stats1;
            Assert.Equal(new Date(10), typed.getMinimum());
            Assert.Equal(new Date(2000), typed.getMaximum());
            stats1.reset();
            stats1.updateDate(-10);
            stats1.updateDate(10000);
            stats1.merge(stats2);
            Assert.Equal(new Date(-10), typed.getMinimum());
            Assert.Equal(new Date(10000), typed.getMaximum());
        }

        [Fact]
        public void testTimestampMerge()
        {
            TypeDescription schema = TypeDescription.createTimestamp();

            ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
            ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
            stats1.updateTimestamp(10);
            stats1.updateTimestamp(100);
            stats2.updateTimestamp(1);
            stats2.updateTimestamp(1000);
            stats1.merge(stats2);
            TimestampColumnStatistics typed = (TimestampColumnStatistics)stats1;
            Assert.Equal(new Timestamp(1), typed.getMinimum());
            Assert.Equal(new Timestamp(1000), typed.getMaximum());
            stats1.reset();
            stats1.updateTimestamp(new Timestamp(-10));
            stats1.updateTimestamp(new Timestamp(10000));
            stats1.merge(stats2);
            Assert.Equal(new Timestamp(-10), typed.getMinimum());
            Assert.Equal(new Timestamp(10000), typed.getMaximum());
        }

        [Fact]
        public void testDecimalMerge()
        {
            TypeDescription schema = TypeDescription.createDecimal()
                .withPrecision(38).withScale(16);

            ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
            ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
            stats1.updateDecimal(HiveDecimal.create(10));
            stats1.updateDecimal(HiveDecimal.create(100));
            stats2.updateDecimal(HiveDecimal.create(1));
            stats2.updateDecimal(HiveDecimal.create(1000));
            stats1.merge(stats2);
            DecimalColumnStatistics typed = (DecimalColumnStatistics)stats1;
            Assert.Equal(1, typed.getMinimum().longValue());
            Assert.Equal(1000, typed.getMaximum().longValue());
            stats1.reset();
            stats1.updateDecimal(HiveDecimal.create(-10));
            stats1.updateDecimal(HiveDecimal.create(10000));
            stats1.merge(stats2);
            Assert.Equal(-10, typed.getMinimum().longValue());
            Assert.Equal(10000, typed.getMaximum().longValue());
        }

        public class SimpleStruct
        {
            byte[] bytes1;
            string string1;

            public SimpleStruct(byte[] b1, string s1)
            {
                this.bytes1 = b1;
                this.string1 = s1;
            }
        }

        private static byte[] bytes(params int[] items)
        {
            byte[] result = new byte[items.Length];
            for (int i = 0; i < items.Length; ++i)
            {
                result[i] = (byte)items[i];
            }
            return result;
        }

        [Fact]
        public void testHasNull()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(SimpleStruct));

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .inspector(inspector)
                .rowIndexStride(1000)
                .stripeSize(10000)
                .bufferSize(10000)))
            {
                // STRIPE 1
                // RG1
                for (int i = 0; i < 1000; i++)
                {
                    writer.addRow(new SimpleStruct(bytes(1, 2, 3), "RG1"));
                }
                // RG2
                for (int i = 0; i < 1000; i++)
                {
                    writer.addRow(new SimpleStruct(bytes(1, 2, 3), null));
                }
                // RG3
                for (int i = 0; i < 1000; i++)
                {
                    writer.addRow(new SimpleStruct(bytes(1, 2, 3), "RG3"));
                }
                // RG4
                for (int i = 0; i < 1000; i++)
                {
                    writer.addRow(new SimpleStruct(bytes(1, 2, 3), null));
                }
                // RG5
                for (int i = 0; i < 1000; i++)
                {
                    writer.addRow(new SimpleStruct(bytes(1, 2, 3), null));
                }
                // STRIPE 2
                for (int i = 0; i < 5000; i++)
                {
                    writer.addRow(new SimpleStruct(bytes(1, 2, 3), null));
                }
                // STRIPE 3
                for (int i = 0; i < 5000; i++)
                {
                    writer.addRow(new SimpleStruct(bytes(1, 2, 3), "STRIPE-3"));
                }
                // STRIPE 4
                for (int i = 0; i < 5000; i++)
                {
                    writer.addRow(new SimpleStruct(bytes(1, 2, 3), null));
                }
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

            // check the file level stats
            ColumnStatistics[] stats = reader.getStatistics();
            Assert.Equal(20000, stats[0].getNumberOfValues());
            Assert.Equal(20000, stats[1].getNumberOfValues());
            Assert.Equal(7000, stats[2].getNumberOfValues());
            Assert.Equal(false, stats[0].hasNull());
            Assert.Equal(false, stats[1].hasNull());
            Assert.Equal(true, stats[2].hasNull());

            // check the stripe level stats
            List<StripeStatistics> stripeStats = reader.getStripeStatistics();
            // stripe 1 stats
            StripeStatistics ss1 = stripeStats[0];
            ColumnStatistics ss1_cs1 = ss1.getColumnStatistics()[0];
            ColumnStatistics ss1_cs2 = ss1.getColumnStatistics()[1];
            ColumnStatistics ss1_cs3 = ss1.getColumnStatistics()[2];
            Assert.Equal(false, ss1_cs1.hasNull());
            Assert.Equal(false, ss1_cs2.hasNull());
            Assert.Equal(true, ss1_cs3.hasNull());

            // stripe 2 stats
            StripeStatistics ss2 = stripeStats[1];
            ColumnStatistics ss2_cs1 = ss2.getColumnStatistics()[0];
            ColumnStatistics ss2_cs2 = ss2.getColumnStatistics()[1];
            ColumnStatistics ss2_cs3 = ss2.getColumnStatistics()[2];
            Assert.Equal(false, ss2_cs1.hasNull());
            Assert.Equal(false, ss2_cs2.hasNull());
            Assert.Equal(true, ss2_cs3.hasNull());

            // stripe 3 stats
            StripeStatistics ss3 = stripeStats[2];
            ColumnStatistics ss3_cs1 = ss3.getColumnStatistics()[0];
            ColumnStatistics ss3_cs2 = ss3.getColumnStatistics()[1];
            ColumnStatistics ss3_cs3 = ss3.getColumnStatistics()[2];
            Assert.Equal(false, ss3_cs1.hasNull());
            Assert.Equal(false, ss3_cs2.hasNull());
            Assert.Equal(false, ss3_cs3.hasNull());

            // stripe 4 stats
            StripeStatistics ss4 = stripeStats[3];
            ColumnStatistics ss4_cs1 = ss4.getColumnStatistics()[0];
            ColumnStatistics ss4_cs2 = ss4.getColumnStatistics()[1];
            ColumnStatistics ss4_cs3 = ss4.getColumnStatistics()[2];
            Assert.Equal(false, ss4_cs1.hasNull());
            Assert.Equal(false, ss4_cs2.hasNull());
            Assert.Equal(true, ss4_cs3.hasNull());

#if false
            // Test file dump
            TextWriter origOut = System.Console.Out;
            string outputFilename = "orc-file-has-null.out";
            FileOutputStream myOut = new FileOutputStream(workDir + File.separator + outputFilename);

            // replace stdout and run command
            System.Console.SetOut(new StreamWriter(myOut));
            FileDump.main(new String[] { testFilePath.toString(), "--rowindex=2" });
            System.Console.Out.Flush();
            System.SetOut(origOut);

            TestFileDump.checkOutput(outputFilename, workDir + File.separator + outputFilename);
#endif
        }
    }
}
