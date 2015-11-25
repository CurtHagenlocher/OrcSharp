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

    /**
     * A MapReduce/Hive input format for ORC files.
     */
    public class VectorizedOrcInputFormat : FileInputFormat<NullWritable, VectorizedRowBatch>
        , InputFormatChecker, VectorizedInputFormatInterface
    {

        class VectorizedOrcRecordReader
            : RecordReader<NullWritable, VectorizedRowBatch>
        {
            private org.apache.hadoop.hive.ql.io.orc.RecordReader reader;
            private long offset;
            private long length;
            private float progress = 0.0f;
            private VectorizedRowBatchCtx rbCtx;
            private bool addPartitionCols = true;

            VectorizedOrcRecordReader(Reader file, Configuration conf,
                FileSplit fileSplit)
            {
                List<OrcProto.Type> types = file.getTypes();
                Reader.Options options = new Reader.Options();
                this.offset = fileSplit.getStart();
                this.length = fileSplit.getLength();
                options.range(offset, length);
                options.include(OrcInputFormat.genIncludedColumns(types, conf, true));
                OrcInputFormat.setSearchArgument(options, types, conf, true);

                this.reader = file.rowsOptions(options);
                try
                {
                    rbCtx = new VectorizedRowBatchCtx();
                    rbCtx.init(conf, fileSplit);
                }
                catch (Exception e)
                {
                    throw;
                }
            }

            public bool next(NullWritable key, VectorizedRowBatch value)
            {

                if (!reader.hasNext())
                {
                    return false;
                }
                try
                {
                    // Check and update partition cols if necessary. Ideally, this should be done
                    // in CreateValue as the partition is constant per split. But since Hive uses
                    // CombineHiveRecordReader and
                    // as this does not call CreateValue for each new RecordReader it creates, this check is
                    // required in next()
                    if (addPartitionCols)
                    {
                        rbCtx.addPartitionColsToBatch(value);
                        addPartitionCols = false;
                    }
                    reader.nextBatch(value);
                }
                catch (Exception e)
                {
                    throw;
                }
                progress = reader.getProgress();
                return true;
            }

            public NullWritable createKey()
            {
                return NullWritable.get();
            }

            public VectorizedRowBatch createValue()
            {
                try
                {
                    return rbCtx.createVectorizedRowBatch();
                }
                catch (Exception e)
                {
                    throw new Exception("Error creating a batch", e);
                }
            }

            public long getPos()
            {
                return offset + (long)(progress * length);
            }

            public void close()
            {
                reader.close();
            }

            public float getProgress()
            {
                return progress;
            }
        }

        public VectorizedOrcInputFormat()
        {
            // just set a really small lower bound
            setMinSplitSize(16 * 1024);
        }

        public RecordReader<NullWritable, VectorizedRowBatch>
            getRecordReader(InputSplit inputSplit, JobConf conf,
                Reporter reporter)
        {
            FileSplit fSplit = (FileSplit)inputSplit;
            reporter.setStatus(fSplit.ToString());

            Path path = fSplit.getPath();

            OrcFile.ReaderOptions opts = OrcFile.readerOptions(conf);
            if (fSplit is OrcSplit)
            {
                OrcSplit orcSplit = (OrcSplit)fSplit;
                if (orcSplit.hasFooter())
                {
                    opts.fileMetaInfo(orcSplit.getFileMetaInfo());
                }
            }
            Reader reader = OrcFile.createReader(path, opts);
            return new VectorizedOrcRecordReader(reader, conf, fSplit);
        }

        public bool validateInput(FileSystem fs, HiveConf conf, List<FileStatus> files)
        {
            if (files.Count <= 0)
            {
                return false;
            }
            foreach (FileStatus file in files)
            {
                try
                {
                    OrcFile.createReader(file.getPath(),
                        OrcFile.readerOptions(conf).filesystem(fs));
                }
                catch (System.IO.IOException e)
                {
                    return false;
                }
            }
            return true;
        }
    }
}
