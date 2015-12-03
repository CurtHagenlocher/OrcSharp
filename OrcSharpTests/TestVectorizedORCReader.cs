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
    using System.IO;
    using System.Text;
    using OrcSharp.Serialization;
    using OrcSharp.Types;
    using Xunit;

    /**
    *
    * Class that tests ORC reader vectorization by comparing records that are
    * returned by "row by row" reader with batch reader.
    *
    */
    public class TestVectorizedORCReader : WithLocalDirectory
    {
        class MyRecord
        {
            private bool? bo;
            private byte? by;
            private int? i;
            private long? l;
            private short? s;
            private double? d;
            private string k;
            private Timestamp? t;
            private Date? dt;
            private HiveDecimal hd;

            public MyRecord(bool? bo, byte? by, int? i, long? l, short? s, double? d, string k,
                Timestamp? t, Date? dt, HiveDecimal hd)
            {
                this.bo = bo;
                this.by = by;
                this.i = i;
                this.l = l;
                this.s = s;
                this.d = d;
                this.k = k;
                this.t = t;
                this.dt = dt;
                this.hd = hd;
            }
        }

        const string testFileName = "TestVectorizedORCReader.orc";

        public TestVectorizedORCReader()
            : base(testFileName)
        {
        }

        [Fact]
        public void createFile()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(MyRecord));

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .stripeSize(100000)
                .compress(CompressionKind.ZLIB)
                .inspector(inspector)
                .bufferSize(10000)
                .rowIndexStride(10000)))
            {
                Random r1 = new Random(1);
                string[] words = TestHelpers.words;
                string[] dates = new string[] { "1991-02-28", "1970-01-31", "1950-04-23" };
                string[] decimalStrings = new string[]
            {
                "234.443", "10001000", "0.3333367", "67788798.0", "-234.443",
                "-10001000", "-0.3333367", "-67788798.0", "0"
            };
                for (int i = 0; i < 21000; ++i)
                {
                    if ((i % 7) != 0)
                    {
                        writer.addRow(new MyRecord(((i % 3) == 0), (byte)(i % 5), i, (long)200, (short)(300 + i), (double)(400 + i),
                            words[r1.Next(words.Length)], new Timestamp(DateTime.Now),
                            Date.Parse(dates[i % 3]), HiveDecimal.Parse(decimalStrings[i % decimalStrings.Length])));
                    }
                    else
                    {
                        writer.addRow(new MyRecord(null, null, i, (long)200, null, null, null, null, null, null));
                    }
                }
                writer.close();
            }
            checkVectorizedReader();
        }

        private void checkVectorizedReader()
        {
            Reader vreader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));
            Reader reader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));
            RecordReaderImpl vrr = (RecordReaderImpl)vreader.rows();
            RecordReaderImpl rr = (RecordReaderImpl)reader.rows();
            VectorizedRowBatch batch = null;

            // Check Vectorized ORC reader against ORC row reader
            while (vrr.hasNext())
            {
                batch = vrr.nextBatch(batch);
                for (int i = 0; i < batch.size; i++)
                {
                    OrcStruct row = (OrcStruct)rr.next();
                    for (int j = 0; j < batch.cols.Length; j++)
                    {
                        object a = (row.getFieldValue(j));
                        ColumnVector cv = batch.cols[j];
                        // if the value is repeating, use row 0
                        int rowId = cv.isRepeating ? 0 : i;

                        // make sure the null flag agrees
                        if (a == null)
                        {
                            Assert.True(!cv.noNulls && cv.isNull[rowId]);
                        }
                        else if (a is bool)
                        {

                            // bool values are stores a 1's and 0's, so convert and compare
                            long temp = (bool)a ? 1 : 0;
                            long b = ((LongColumnVector)cv).vector[rowId];
                            Assert.Equal(temp.ToString(), b.ToString());
                        }
                        else if (a is Timestamp)
                        {
                            // Timestamps are stored as long, so convert and compare
                            Timestamp t = (Timestamp)a;
                            // Timestamp.getTime() is overriden and is 
                            // long time = super.getTime();
                            // return (time + (nanos / 1000000));
                            long timeInNanoSec = (t.Milliseconds * 1000000)
                                + (t.getNanos() % 1000000);
                            long b = ((LongColumnVector)cv).vector[rowId];
                            Assert.Equal(timeInNanoSec.ToString(), b.ToString());

                        }
                        else if (a is Date)
                        {
                            // Dates are stored as long, so convert and compare

                            Date adt = (Date)a;
                            long b = ((LongColumnVector)cv).vector[rowId];
                            // Assert.Equal(adt, Date.daysToMillis((int)b));
                            Assert.Equal(adt.Days, (int)b);
                        }
                        else if (a is HiveDecimal)
                        {
                            // Decimals are stored as BigInteger, so convert and compare
                            HiveDecimal dec = (HiveDecimal)a;
                            HiveDecimal b = ((DecimalColumnVector)cv).vector[i];
                            Assert.Equal(dec, b);
                        }
                        else if (a is double)
                        {
                            double b = ((DoubleColumnVector)cv).vector[rowId];
                            Assert.Equal(a.ToString(), b.ToString());
                        }
                        else if (a is string)
                        {
                            BytesColumnVector bcv = (BytesColumnVector)cv;
                            string b = Encoding.UTF8.GetString(bcv.vector[rowId], bcv.start[rowId], bcv.length[rowId]);
                            Assert.Equal((string)a, b);
                        }
                        else if (a is int || a is long || a is byte || a is short)
                        {
                            Assert.Equal(a.ToString(),
                                ((LongColumnVector)cv).vector[rowId].ToString());
                        }
                        else
                        {
                            Assert.True(false);
                        }
                    }
                }

                // Check repeating
                Assert.Equal(false, batch.cols[0].isRepeating);
                Assert.Equal(false, batch.cols[1].isRepeating);
                Assert.Equal(false, batch.cols[2].isRepeating);
                Assert.Equal(true, batch.cols[3].isRepeating);
                Assert.Equal(false, batch.cols[4].isRepeating);
                Assert.Equal(false, batch.cols[5].isRepeating);
                Assert.Equal(false, batch.cols[6].isRepeating);
                Assert.Equal(false, batch.cols[7].isRepeating);
                Assert.Equal(false, batch.cols[8].isRepeating);
                Assert.Equal(false, batch.cols[9].isRepeating);

                // Check non null
                Assert.Equal(false, batch.cols[0].noNulls);
                Assert.Equal(false, batch.cols[1].noNulls);
                Assert.Equal(true, batch.cols[2].noNulls);
                Assert.Equal(true, batch.cols[3].noNulls);
                Assert.Equal(false, batch.cols[4].noNulls);
                Assert.Equal(false, batch.cols[5].noNulls);
                Assert.Equal(false, batch.cols[6].noNulls);
                Assert.Equal(false, batch.cols[7].noNulls);
                Assert.Equal(false, batch.cols[8].noNulls);
                Assert.Equal(false, batch.cols[9].noNulls);
            }
            Assert.Equal(false, rr.hasNext());
        }
    }
}
