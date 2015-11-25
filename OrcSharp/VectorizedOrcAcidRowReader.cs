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

    /**
     * Implement a RecordReader that stitches together base and delta files to
     * support tables and partitions stored in the ACID format. It works by using
     * the non-vectorized ACID reader and moving the data into a vectorized row
     * batch.
     */
    class VectorizedOrcAcidRowReader
        : org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch>
    {
        private AcidInputFormat.RowReader<OrcStruct> innerReader;
        private RecordIdentifier key;
        private OrcStruct value;
        private VectorizedRowBatchCtx rowBatchCtx;
        private ObjectInspector objectInspector;
        private DataOutputBuffer buffer = new DataOutputBuffer();

        VectorizedOrcAcidRowReader(AcidInputFormat.RowReader<OrcStruct> inner,
                                   Configuration conf,
                                   FileSplit split)
        {
            this.innerReader = inner;
            this.key = inner.createKey();
            this.rowBatchCtx = new VectorizedRowBatchCtx();
            this.value = inner.createValue();
            this.objectInspector = inner.getObjectInspector();
            try
            {
                rowBatchCtx.init(conf, split);
            }
            catch (ClassNotFoundException e)
            {
                throw new IOException("Failed to initialize context", e);
            }
            catch (SerDeException e)
            {
                throw new IOException("Failed to initialize context", e);
            }
            catch (InstantiationException e)
            {
                throw new IOException("Failed to initialize context", e);
            }
            catch (IllegalAccessException e)
            {
                throw new IOException("Failed to initialize context", e);
            }
            catch (HiveException e)
            {
                throw new IOException("Failed to initialize context", e);
            }
        }

        public bool next(NullWritable nullWritable,
                            VectorizedRowBatch vectorizedRowBatch
                            )
        {
            vectorizedRowBatch.reset();
            buffer.reset();
            if (!innerReader.next(key, value))
            {
                return false;
            }
            try
            {
                rowBatchCtx.addPartitionColsToBatch(vectorizedRowBatch);
            }
            catch (HiveException e)
            {
                throw new IOException("Problem adding partition column", e);
            }
            try
            {
                VectorizedBatchUtil.acidAddRowToBatch(value,
                    (StructObjectInspector)objectInspector,
                    vectorizedRowBatch.size++, vectorizedRowBatch, rowBatchCtx, buffer);
                while (vectorizedRowBatch.size < vectorizedRowBatch.selected.length &&
                    innerReader.next(key, value))
                {
                    VectorizedBatchUtil.acidAddRowToBatch(value,
                        (StructObjectInspector)objectInspector,
                        vectorizedRowBatch.size++, vectorizedRowBatch, rowBatchCtx, buffer);
                }
            }
            catch (HiveException he)
            {
                throw new IOException("error iterating", he);
            }
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
                return rowBatchCtx.createVectorizedRowBatch();
            }
            catch (HiveException e)
            {
                throw new RuntimeException("Error creating a batch", e);
            }
        }

        public long getPos()
        {
            return innerReader.getPos();
        }

        public void close()
        {
            innerReader.close();
        }

        public float getProgress()
        {
            return innerReader.getProgress();
        }
    }
}
