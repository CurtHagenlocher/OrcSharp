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

namespace org.apache.hadoop.hive.ql.io.orc.encoded
{
    using org.apache.hadoop.hive.ql.io.orc.external;
    using ColumnStreamData = EncodedColumnBatch<OrcBatchKey>.ColumnStreamData;

    /**
     * Stream utility.
     */
    public class StreamUtils
    {
        /**
         * Create SettableUncompressedStream from stream buffer.
         *
         * @param streamName - stream name
         * @param fileId - file id
         * @param streamBuffer - stream buffer
         * @return - SettableUncompressedStream
         * @
         */
        public static SettableUncompressedStream createSettableUncompressedStream(string streamName,
            long? fileId, ColumnStreamData streamBuffer)
        {
            if (streamBuffer == null)
            {
                return null;
            }

            DiskRangeInfo diskRangeInfo = createDiskRangeInfo(streamBuffer);
            return new SettableUncompressedStream(fileId, streamName, diskRangeInfo.getDiskRanges(),
                diskRangeInfo.getTotalLength());
        }

        /**
         * Converts stream buffers to disk ranges.
         * @param streamBuffer - stream buffer
         * @return - total length of disk ranges
         */
        public static DiskRangeInfo createDiskRangeInfo(ColumnStreamData streamBuffer)
        {
            DiskRangeInfo diskRangeInfo = new DiskRangeInfo(streamBuffer.getIndexBaseOffset());
            long offset = streamBuffer.getIndexBaseOffset(); // See ctor comment.
            // TODO: we should get rid of this
            foreach (MemoryBuffer memoryBuffer in streamBuffer.getCacheBuffers())
            {
                ByteBuffer buffer = memoryBuffer.getByteBufferDup();
                diskRangeInfo.addDiskRange(new RecordReaderImpl.BufferChunk(buffer, offset));
                offset += buffer.remaining();
            }
            return diskRangeInfo;
        }
    }
}
