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

namespace org.apache.hadoop.hive.ql.io.orc.external
{
    public class HadoopShims
    {
    }

    class ShimLoader
    {
        internal static HadoopShims getHadoopShims()
        {
            throw new System.NotImplementedException();
        }
    }

    interface WritableComparable<T>
    {
    }

    interface DataInput
    {
        byte readByte();

        int readInt();
    }

    interface DataOutput
    {
        void writeByte(int flags);

        void writeInt(int p);
    }

    class NullWritable
    {
        internal static NullWritable get()
        {
            throw new System.NotImplementedException();
        }
    }

    class RecordReader<K, V>
    {
    }

    interface StatsProvidingRecordReader
    {
    }

    class SerDeStats
    {
        internal void setRawDataSize(long p)
        {
            throw new System.NotImplementedException();
        }

        internal void setRowCount(long p)
        {
            throw new System.NotImplementedException();
        }
    }

    class FileUtils
    {
    }

    public class FileSplit
    {
        private Path path;
        private long p1;
        private long p2;
        private object p3;

        public FileSplit(Path path, long p1, long p2, object p3)
        {
            // TODO: Complete member initialization
            this.path = path;
            this.p1 = p1;
            this.p2 = p2;
            this.p3 = p3;
        }
        internal long getStart()
        {
            throw new System.NotImplementedException();
        }

        internal long getLength()
        {
            throw new System.NotImplementedException();
        }

        internal void write(DataOutput @out)
        {
            throw new System.NotImplementedException();
        }

        internal Path getPath()
        {
            throw new System.NotImplementedException();
        }

        internal void readFields(DataInput @in)
        {
            throw new System.NotImplementedException();
        }
    }

    interface ColumnarSplit
    {
    }

    class AcidUtils
    {
        public static object hiddenFileFilter { get; set; }
    }

    class AcidInputFormat
    {
    }
}
