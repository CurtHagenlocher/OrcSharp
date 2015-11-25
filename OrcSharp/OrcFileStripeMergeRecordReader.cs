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
    using System;
    using System.Collections.Generic;
    using org.apache.hadoop.hive.ql.io.orc.external;
    using OrcProto = global::orc.proto;


    public class OrcFileStripeMergeRecordReader : RecordReader<OrcFileKeyWrapper, OrcFileValueWrapper>
    {
        private Reader reader;
        private Path path;
        protected IEnumerator<StripeInformation> iter;
        protected List<OrcProto.StripeStatistics> stripeStatistics;
        private int stripeIdx;
        private long start;
        private long end;
        private bool skipFile;

        public OrcFileStripeMergeRecordReader(Configuration conf, FileSplit split)
        {
            path = split.getPath();
            start = split.getStart();
            end = start + split.getLength();
            FileSystem fs = path.getFileSystem(conf);
            this.reader = OrcFile.createReader(path, OrcFile.readerOptions(conf).filesystem(fs));
            this.iter = reader.getStripes().GetEnumerator();
            this.stripeIdx = 0;
            this.stripeStatistics = ((ReaderImpl)reader).getOrcProtoStripeStatistics();
        }

        public Type getKeyClass()
        {
            return typeof(OrcFileKeyWrapper);
        }

        public Type getValueClass()
        {
            return typeof(OrcFileValueWrapper);
        }

        public OrcFileKeyWrapper createKey()
        {
            return new OrcFileKeyWrapper();
        }

        public OrcFileValueWrapper createValue()
        {
            return new OrcFileValueWrapper();
        }

        public bool next(OrcFileKeyWrapper key, OrcFileValueWrapper value)
        {
            if (skipFile)
            {
                return false;
            }
            return nextStripe(key, value);
        }

        protected bool nextStripe(OrcFileKeyWrapper keyWrapper, OrcFileValueWrapper valueWrapper)
        {
            // missing stripe stats (old format). If numRows is 0 then its an empty file and no statistics
            // is present. We have to differentiate no stats (empty file) vs missing stats (old format).
            if ((stripeStatistics == null || stripeStatistics.Count == 0) && reader.getNumberOfRows() > 0)
            {
                keyWrapper.setInputPath(path);
                keyWrapper.setIsIncompatFile(true);
                skipFile = true;
                return true;
            }

            bool active = iter.MoveNext();
            while (active)
            {
                StripeInformation si = iter.Current;

                // if stripe offset is outside the split boundary then ignore the current
                // stripe as it will be handled by some other mapper.
                if (si.getOffset() >= start && si.getOffset() < end)
                {
                    valueWrapper.setStripeStatistics(stripeStatistics[stripeIdx++]);
                    valueWrapper.setStripeInformation(si);
                    active = iter.MoveNext();
                    if (!active)
                    {
                        valueWrapper.setLastStripeInFile(true);
                        valueWrapper.setUserMetadata(((ReaderImpl)reader).getOrcProtoUserMetadata());
                    }
                    keyWrapper.setInputPath(path);
                    keyWrapper.setCompression(reader.getCompression());
                    keyWrapper.setCompressBufferSize(reader.getCompressionSize());
                    keyWrapper.setVersion(reader.getFileVersion());
                    keyWrapper.setRowIndexStride(reader.getRowIndexStride());
                    keyWrapper.setTypes(reader.getTypes());
                }
                else
                {
                    stripeIdx++;
                    continue;
                }
                return true;
            }

            return false;
        }

        /**
         * Default progress will be based on number of files processed.
         * @return 0.0 to 1.0 of the input byte range
         */
        public float getProgress()
        {
            return 0.0f;
        }

        public long getPos()
        {
            return 0;
        }

        protected void seek(long pos)
        {
        }

        public long getStart()
        {
            return 0;
        }

        public void close()
        {
        }
    }
}
