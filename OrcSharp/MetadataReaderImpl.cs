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
    using System.Collections.Generic;
    using System.IO;
    using org.apache.hadoop.hive.ql.io.orc.external;
    using OrcProto = global::orc.proto;

    public class MetadataReaderImpl : MetadataReader
    {
        private Stream file;
        private CompressionCodec codec;
        private int bufferSize;
        private int typeCount;

        public MetadataReaderImpl(Stream file,
            CompressionCodec codec, int bufferSize, int typeCount)
        {
            this.file = file;
            this.codec = codec;
            this.bufferSize = bufferSize;
            this.typeCount = typeCount;
        }

        public RecordReaderImpl.Index readRowIndex(StripeInformation stripe,
            OrcProto.StripeFooter footer, bool[] included, OrcProto.RowIndex[] indexes,
            bool[] sargColumns, OrcProto.BloomFilterIndex[] bloomFilterIndices)
        {
            if (footer == null)
            {
                footer = readStripeFooter(stripe);
            }
            if (indexes == null)
            {
                indexes = new OrcProto.RowIndex[typeCount];
            }
            if (bloomFilterIndices == null)
            {
                bloomFilterIndices = new OrcProto.BloomFilterIndex[typeCount];
            }
            long offset = stripe.getOffset();
            IList<OrcProto.Stream> streams = footer.StreamsList;
            for (int i = 0; i < streams.Count; i++)
            {
                OrcProto.Stream stream = streams[i];
                OrcProto.Stream nextStream = null;
                if (i < streams.Count - 1)
                {
                    nextStream = streams[i + 1];
                }
                int col = (int)stream.Column;
                int len = (int)stream.Length;
                // row index stream and bloom filter are interlaced, check if the sarg column contains bloom
                // filter and combine the io to read row index and bloom filters for that column together
                if (stream.HasKind && (stream.Kind == OrcProto.Stream.Types.Kind.ROW_INDEX))
                {
                    bool readBloomFilter = false;
                    if (sargColumns != null && sargColumns[col] &&
                        nextStream.Kind == OrcProto.Stream.Types.Kind.BLOOM_FILTER)
                    {
                        len += (int)nextStream.Length;
                        i += 1;
                        readBloomFilter = true;
                    }
                    if ((included == null || included[col]) && indexes[col] == null)
                    {
                        byte[] buffer = new byte[len];
                        file.readFully(offset, buffer, 0, buffer.Length);
                        ByteBuffer bb = ByteBuffer.wrap(buffer);
                        indexes[col] = OrcProto.RowIndex.ParseFrom(InStream.create(null, "index",
                            new List<DiskRange> { new RecordReaderImpl.BufferChunk(bb, 0) },
                            (long)stream.Length, codec, bufferSize));
                        if (readBloomFilter)
                        {
                            bb.position((int)stream.Length);
                            bloomFilterIndices[col] = OrcProto.BloomFilterIndex.ParseFrom(InStream.create(
                                null, "bloom_filter", new List<DiskRange> { new RecordReaderImpl.BufferChunk(bb, 0) },
                                (long)nextStream.Length, codec, bufferSize));
                        }
                    }
                }
                offset += len;
            }

            RecordReaderImpl.Index index = new RecordReaderImpl.Index(indexes, bloomFilterIndices);
            return index;
        }

        public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe)
        {
            long offset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
            int tailLength = (int)stripe.getFooterLength();

            // read the footer
            ByteBuffer tailBuf = ByteBuffer.allocate(tailLength);
            file.readFully(offset, tailBuf.array(), tailBuf.arrayOffset(), tailLength);
            return OrcProto.StripeFooter.ParseFrom(InStream.createCodedInputStream(null, "footer",
                new List<DiskRange> { new RecordReaderImpl.BufferChunk(tailBuf, 0) },
                tailLength, codec, bufferSize));
        }

        public void close()
        {
            // TODO:
            // file.Close();
        }
    }
}
