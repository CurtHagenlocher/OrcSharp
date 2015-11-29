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
    using System.Collections.Generic;
    using OrcSharp.External;
    using OrcSharp.Serialization;
    using OrcSharp.Types;
    using OrcProto = global::orc.proto;
    using ByteString = Google.ProtocolBuffers.ByteString;
    using System.IO;
    using Google.ProtocolBuffers;
    using System.Text;

    /**
     * An ORC file writer. The file is divided into stripes, which is the natural
     * unit of work when reading. Each stripe is buffered in memory until the
     * memory reaches the stripe size and then it is written out broken down by
     * columns. Each column is written by a TreeWriter that is specific to that
     * type of column. TreeWriters may have children TreeWriters that handle the
     * sub-types. Each of the TreeWriters writes the column's data as a set of
     * streams.
     *
     * This class is unsynchronized like most Stream objects, so from the creation of an OrcFile and all
     * access to a single instance has to be from a single thread.
     * 
     * There are no known cases where these happen between different threads today.
     * 
     * Caveat: the MemoryManager is created during WriterOptions create, that has to be confined to a single
     * thread as well.
     * 
     */
    internal class WriterImpl : Writer, MemoryManager.Callback
    {
        private static Log LOG = LogFactory.getLog(typeof(WriterImpl));

        private const int HDFS_BUFFER_SIZE = 256 * 1024;
        private const int MIN_ROW_INDEX_STRIDE = 1000;

        // threshold above which buffer size will be automatically resized
        private const int COLUMN_COUNT_THRESHOLD = 1000;

        private string path;
        private long defaultStripeSize;
        private long adjustedStripeSize;
        private int rowIndexStride;
        private CompressionKind compress;
        private CompressionCodec codec;
        private bool addBlockPadding;
        private int bufferSize;
        private long blockSize;
        private double paddingTolerance;
        private TypeDescription schema;

        // the streams that make up the current stripe
        private Dictionary<StreamName, BufferedStream> streams =
          new Dictionary<StreamName, BufferedStream>();

        private Stream baseStream;
        private Stream rawWriter = null;
        // the compressed metadata information outStream
        private OutStream writer = null;
        // a protobuf outStream around streamFactory
        private CodedOutputStream protobufWriter = null;
        private long headerLength;
        private int columnCount;
        private long rowCount = 0;
        private long rowsInStripe = 0;
        private long rawDataSize = 0;
        private int rowsInIndex = 0;
        private int stripesAtLastFlush = -1;
        private List<OrcProto.StripeInformation> stripes =
          new List<OrcProto.StripeInformation>();
        private Dictionary<string, ByteString> userMetadata =
          new Dictionary<string, ByteString>();
        private StreamFactory streamFactory;
        private TreeWriter treeWriter;
        private bool buildIndex;
        private MemoryManager memoryManager;
        private OrcFile.Version version;
        private OrcFile.WriterOptions options;
        private OrcFile.WriterCallback callback;
        private OrcFile.EncodingStrategy encodingStrategy;
        private OrcFile.CompressionStrategy compressionStrategy;
        private bool[] bloomFilterColumns;
        private double bloomFilterFpp;
        private bool writeTimeZone;

        public WriterImpl(
            Stream stream,
            string path,
            OrcFile.WriterOptions options,
            ObjectInspector inspector,
            TypeDescription schema,
            long stripeSize,
            CompressionKind compress,
            int bufferSize,
            int rowIndexStride,
            MemoryManager memoryManager,
            bool addBlockPadding,
            OrcFile.Version version,
            OrcFile.WriterCallback callback,
            OrcFile.EncodingStrategy encodingStrategy,
            OrcFile.CompressionStrategy compressionStrategy,
            double paddingTolerance,
            long blockSizeValue,
            string bloomFilterColumnNames,
            double bloomFilterFpp)
        {
            this.baseStream = stream;
            this.streamFactory = new StreamFactory(this);
            this.path = path;
            this.options = options;
            this.callback = callback;
            this.schema = schema;
            this.adjustedStripeSize = stripeSize;
            this.defaultStripeSize = stripeSize;
            this.version = version;
            this.encodingStrategy = encodingStrategy;
            this.compressionStrategy = compressionStrategy;
            this.addBlockPadding = addBlockPadding;
            this.blockSize = blockSizeValue;
            this.paddingTolerance = paddingTolerance;
            this.compress = compress;
            this.rowIndexStride = rowIndexStride;
            this.memoryManager = memoryManager;
            buildIndex = rowIndexStride > 0;
            codec = createCodec(compress);
            int numColumns = schema.getMaximumId() + 1;
            this.bufferSize = getEstimatedBufferSize(defaultStripeSize, numColumns, bufferSize);
            if (version == OrcFile.Version.V_0_11)
            {
                /* do not write bloom filters for ORC v11 */
                this.bloomFilterColumns = new bool[schema.getMaximumId() + 1];
            }
            else
            {
                this.bloomFilterColumns =
                    OrcUtils.includeColumns(bloomFilterColumnNames, schema);
            }
            this.bloomFilterFpp = bloomFilterFpp;
            treeWriter = createTreeWriter(inspector, schema, streamFactory, false);
            if (buildIndex && rowIndexStride < MIN_ROW_INDEX_STRIDE)
            {
                throw new ArgumentException("Row stride must be at least " +
                    MIN_ROW_INDEX_STRIDE);
            }

            // ensure that we are able to handle callbacks before we register ourselves
            memoryManager.addWriter(path, stripeSize, this);
        }

        internal static int getEstimatedBufferSize(long stripeSize, int numColumns, int bs)
        {
            // The worst case is that there are 2 big streams per a column and
            // we want to guarantee that each stream gets ~10 buffers.
            // This keeps buffers small enough that we don't get really small stripe
            // sizes.
            int estBufferSize = (int)(stripeSize / (20 * numColumns));
            estBufferSize = getClosestBufferSize(estBufferSize);
            if (estBufferSize > bs)
            {
                estBufferSize = bs;
            }
            else
            {
                LOG.info("WIDE TABLE - Number of columns: " + numColumns +
                    " Chosen compression buffer size: " + estBufferSize);
            }
            return estBufferSize;
        }

        private static int getClosestBufferSize(int estBufferSize)
        {
            const int kb4 = 4 * 1024;
            const int kb8 = 8 * 1024;
            const int kb16 = 16 * 1024;
            const int kb32 = 32 * 1024;
            const int kb64 = 64 * 1024;
            const int kb128 = 128 * 1024;
            const int kb256 = 256 * 1024;
            if (estBufferSize <= kb4)
            {
                return kb4;
            }
            else if (estBufferSize > kb4 && estBufferSize <= kb8)
            {
                return kb8;
            }
            else if (estBufferSize > kb8 && estBufferSize <= kb16)
            {
                return kb16;
            }
            else if (estBufferSize > kb16 && estBufferSize <= kb32)
            {
                return kb32;
            }
            else if (estBufferSize > kb32 && estBufferSize <= kb64)
            {
                return kb64;
            }
            else if (estBufferSize > kb64 && estBufferSize <= kb128)
            {
                return kb128;
            }
            else
            {
                return kb256;
            }
        }

        public static CompressionCodec createCodec(CompressionKind kind)
        {
#if COMPRESSION
            switch (kind)
            {
                case CompressionKind.NONE:
                    return null;
                case CompressionKind.ZLIB:
                    return new ZlibCodec();
                case CompressionKind.SNAPPY:
                    return new SnappyCodec();
                case CompressionKind.LZO:
                    try
                    {
                        Class<CompressionCodec> lzo =
                            (Class<CompressionCodec>)
                              JavaUtils.loadClass("OrcSharp.LzoCodec");
                        return lzo.newInstance();
                    }
                    catch (ClassNotFoundException e)
                    {
                        throw new ArgumentException("LZO is not available.", e);
                    }
                    catch (InstantiationException e)
                    {
                        throw new ArgumentException("Problem initializing LZO", e);
                    }
                    catch (IllegalAccessException e)
                    {
                        throw new ArgumentException("Insufficient access to LZO", e);
                    }
                default:
                    throw new ArgumentException("Unknown compression codec: " +
                        kind);
            }
#endif
            return null;
        }

        public bool checkMemory(double newScale)
        {
            long limit = (long)Math.Round(adjustedStripeSize * newScale);
            long size = estimateStripeSize();
            if (LOG.isDebugEnabled())
            {
                LOG.debug("ORC writer " + path + " size = " + size + " limit = " + limit);
            }
            if (size > limit)
            {
                flushStripe();
                return true;
            }
            return false;
        }

        /**
         * This class is used to hold the contents of streams as they are buffered.
         * The TreeWriters write to the outStream and the codec compresses the
         * data as buffers fill up and stores them in the output list. When the
         * stripe is being written, the whole stream is written to the file.
         */
        private class BufferedStream : OutStream.OutputReceiver
        {
            public OutStream outStream;
            private List<ByteBuffer> _output = new List<ByteBuffer>();

            public BufferedStream(string name, int bufferSize, CompressionCodec codec)
            {
                outStream = new OutStream(name, bufferSize, codec, this);
            }

            /**
             * Receive a buffer from the compression codec.
             * @param buffer the buffer to save
             * @
             */
            public void output(ByteBuffer buffer)
            {
                _output.Add(buffer);
            }

            /**
             * Get the number of bytes in buffers that are allocated to this stream.
             * @return number of bytes in buffers
             */
            public long getBufferSize()
            {
                long result = 0;
                foreach (ByteBuffer buf in _output)
                {
                    result += buf.capacity();
                }
                return outStream.getBufferSize() + result;
            }

            /**
             * Flush the stream to the codec.
             * @
             */
            public void flush()
            {
                outStream.Flush();
            }

            /**
             * Clear all of the buffers.
             * @
             */
            public void clear()
            {
                outStream.clear();
                _output.Clear();
            }

            /**
             * Check the state of suppress flag in output stream
             * @return value of suppress flag
             */
            public bool isSuppressed()
            {
                return outStream.isSuppressed();
            }

            /**
             * Get the number of bytes that will be written to the output. Assumes
             * the stream has already been flushed.
             * @return the number of bytes
             */
            public long getOutputSize()
            {
                long result = 0;
                foreach (ByteBuffer buffer in _output)
                {
                    result += buffer.remaining();
                }
                return result;
            }

            /**
             * Write the saved compressed buffers to the OutputStream.
             * @param out the stream to write to
             * @
             */
            public void spillTo(Stream @out)
            {
                foreach (ByteBuffer buffer in _output)
                {
                    @out.Write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
                }
            }

            public override string ToString()
            {
                return outStream.ToString();
            }
        }

        /**
         * An output receiver that writes the ByteBuffers to the output stream
         * as they are received.
         */
        private class DirectStream : OutStream.OutputReceiver
        {
            private Stream _output;

            public DirectStream(Stream output)
            {
                this._output = output;
            }

            public void output(ByteBuffer buffer)
            {
                _output.Write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
            }
        }

        private class RowIndexPositionRecorder : PositionRecorder
        {
            private OrcProto.RowIndexEntry.Builder builder;

            public RowIndexPositionRecorder(OrcProto.RowIndexEntry.Builder builder)
            {
                this.builder = builder;
            }

            public void addPosition(long position)
            {
                builder.AddPositions((ulong)position);
            }
        }

        /**
         * Interface from the Writer to the TreeWriters. This limits the visibility
         * that the TreeWriters have into the Writer.
         */
        private class StreamFactory
        {
            private readonly WriterImpl writer;

            public StreamFactory(WriterImpl writer)
            {
                this.writer = writer;
            }

            /**
             * Create a stream to store part of a column.
             * @param column the column id for the stream
             * @param kind the kind of stream
             * @return The output outStream that the section needs to be written to.
             * @
             */
            public OutStream createStream(int column, OrcProto.Stream.Types.Kind kind)
            {
                StreamName name = new StreamName(column, kind);
                CompressionModifier[] modifiers;

                switch (kind)
                {
                    case OrcProto.Stream.Types.Kind.BLOOM_FILTER:
                    case OrcProto.Stream.Types.Kind.DATA:
                    case OrcProto.Stream.Types.Kind.DICTIONARY_DATA:
                        if (getCompressionStrategy() == OrcFile.CompressionStrategy.SPEED)
                        {
                            modifiers = new[] { CompressionModifier.FAST, CompressionModifier.TEXT };
                        }
                        else
                        {
                            modifiers = new[] { CompressionModifier.DEFAULT, CompressionModifier.TEXT };
                        }
                        break;
                    case OrcProto.Stream.Types.Kind.LENGTH:
                    case OrcProto.Stream.Types.Kind.DICTIONARY_COUNT:
                    case OrcProto.Stream.Types.Kind.PRESENT:
                    case OrcProto.Stream.Types.Kind.ROW_INDEX:
                    case OrcProto.Stream.Types.Kind.SECONDARY:
                        // easily compressed using the fastest modes
                        modifiers = new[] { CompressionModifier.FASTEST, CompressionModifier.BINARY };
                        break;
                    default:
                        LOG.warn("Missing ORC compression modifiers for " + kind);
                        modifiers = null;
                        break;
                }

                BufferedStream result = writer.streams.get(name);
                if (result == null)
                {
                    result = new BufferedStream(name.ToString(), writer.bufferSize,
                        writer.codec == null ? writer.codec : writer.codec.modify(modifiers));
                    writer.streams.Add(name, result);
                }
                return result.outStream;
            }

            /**
             * Get the next column id.
             * @return a number from 0 to the number of columns - 1
             */
            public int getNextColumnId()
            {
                return writer.columnCount++;
            }

            /**
             * Get the stride rate of the row index.
             */
            public int getRowIndexStride()
            {
                return writer.rowIndexStride;
            }

            /**
             * Should be building the row index.
             * @return true if we are building the index
             */
            public bool buildIndex()
            {
                return writer.buildIndex;
            }

            /**
             * Is the ORC file compressed?
             * @return are the streams compressed
             */
            public bool isCompressed()
            {
                return writer.codec != null;
            }

            /**
             * Get the encoding strategy to use.
             * @return encoding strategy
             */
            public OrcFile.EncodingStrategy getEncodingStrategy()
            {
                return writer.encodingStrategy;
            }

            /**
             * Get the compression strategy to use.
             * @return compression strategy
             */
            public OrcFile.CompressionStrategy getCompressionStrategy()
            {
                return writer.compressionStrategy;
            }

            /**
             * Get the bloom filter columns
             * @return bloom filter columns
             */
            public bool[] getBloomFilterColumns()
            {
                return writer.bloomFilterColumns;
            }

            /**
             * Get bloom filter false positive percentage.
             * @return fpp
             */
            public double getBloomFilterFPP()
            {
                return writer.bloomFilterFpp;
            }

            /**
             * Get the writer's configuration.
             * @return configuration
             */
            public OrcFile.WriterOptions getOptions()
            {
                return writer.options;
            }

            /**
             * Get the version of the file to write.
             */
            public OrcFile.Version getVersion()
            {
                return writer.version;
            }

            public void useWriterTimeZone(bool val)
            {
                writer.writeTimeZone = val;
            }

            public bool hasWriterTimeZone()
            {
                return writer.writeTimeZone;
            }
        }

        /**
         * The parent class of all of the writers for each column. Each column
         * is written by an instance of this class. The compound types (struct,
         * list, map, and union) have children tree writers that write the children
         * types.
         */
        private abstract class TreeWriter
        {
            protected int id;
            protected ObjectInspector inspector;
            private BitFieldWriter isPresent;
            private bool isCompressed;
            protected ColumnStatisticsImpl indexStatistics;
            protected ColumnStatisticsImpl stripeColStatistics;
            public ColumnStatisticsImpl fileStatistics;
            public TreeWriter[] childrenWriters;
            protected RowIndexPositionRecorder rowIndexPosition;
            private OrcProto.RowIndex.Builder rowIndex;
            private OrcProto.RowIndexEntry.Builder rowIndexEntry;
            private PositionedOutputStream rowIndexStream;
            private PositionedOutputStream bloomFilterStream;
            protected BloomFilter bloomFilter;
            protected bool createBloomFilter;
            private OrcProto.BloomFilterIndex.Builder bloomFilterIndex;
            private OrcProto.BloomFilter.Builder bloomFilterEntry;
            private bool foundNulls;
            private OutStream isPresentOutStream;
            public List<OrcProto.StripeStatistics.Builder> stripeStatsBuilders;
            private StreamFactory streamFactory;

            /**
             * Create a tree writer.
             * @param columnId the column id of the column to write
             * @param inspector the object inspector to use
             * @param schema the row schema
             * @param streamFactory limited access to the Writer's data.
             * @param nullable can the value be null?
             * @
             */
            protected TreeWriter(
                int columnId,
                ObjectInspector inspector,
                TypeDescription schema,
                StreamFactory streamFactory,
                bool nullable)
            {
                this.streamFactory = streamFactory;
                this.isCompressed = streamFactory.isCompressed();
                this.id = columnId;
                this.inspector = inspector;
                if (nullable)
                {
                    isPresentOutStream = streamFactory.createStream(id,
                        OrcProto.Stream.Types.Kind.PRESENT);
                    isPresent = new BitFieldWriter(isPresentOutStream, 1);
                }
                else
                {
                    isPresent = null;
                }
                this.foundNulls = false;
                createBloomFilter = streamFactory.getBloomFilterColumns()[columnId];
                indexStatistics = ColumnStatisticsImpl.create(schema);
                stripeColStatistics = ColumnStatisticsImpl.create(schema);
                fileStatistics = ColumnStatisticsImpl.create(schema);
                childrenWriters = new TreeWriter[0];
                rowIndex = OrcProto.RowIndex.CreateBuilder();
                rowIndexEntry = OrcProto.RowIndexEntry.CreateBuilder();
                rowIndexPosition = new RowIndexPositionRecorder(rowIndexEntry);
                stripeStatsBuilders = new List<OrcProto.StripeStatistics.Builder>();
                if (streamFactory.buildIndex())
                {
                    rowIndexStream = streamFactory.createStream(id, OrcProto.Stream.Types.Kind.ROW_INDEX);
                }
                else
                {
                    rowIndexStream = null;
                }
                if (createBloomFilter)
                {
                    bloomFilterEntry = OrcProto.BloomFilter.CreateBuilder();
                    bloomFilterIndex = OrcProto.BloomFilterIndex.CreateBuilder();
                    bloomFilterStream = streamFactory.createStream(id, OrcProto.Stream.Types.Kind.BLOOM_FILTER);
                    bloomFilter = new BloomFilter(streamFactory.getRowIndexStride(), streamFactory.getBloomFilterFPP());
                }
                else
                {
                    bloomFilterEntry = null;
                    bloomFilterIndex = null;
                    bloomFilterStream = null;
                    bloomFilter = null;
                }
            }

            protected OrcProto.RowIndex.Builder getRowIndex()
            {
                return rowIndex;
            }

            protected ColumnStatisticsImpl getStripeStatistics()
            {
                return stripeColStatistics;
            }

            protected ColumnStatisticsImpl getFileStatistics()
            {
                return fileStatistics;
            }

            protected OrcProto.RowIndexEntry.Builder getRowIndexEntry()
            {
                return rowIndexEntry;
            }

            public IntegerWriter createIntegerWriter(PositionedOutputStream output,
                                              bool signed, bool isDirectV2,
                                              StreamFactory writer)
            {
                if (isDirectV2)
                {
                    bool alignedBitpacking = false;
                    if (writer.getEncodingStrategy() == OrcFile.EncodingStrategy.SPEED)
                    {
                        alignedBitpacking = true;
                    }
                    return new RunLengthIntegerWriterV2(output, signed, alignedBitpacking);
                }
                else
                {
                    return new RunLengthIntegerWriter(output, signed);
                }
            }

            public bool isNewWriteFormat(StreamFactory writer)
            {
                return writer.getVersion() != OrcFile.Version.V_0_11;
            }

            /**
             * Add a new value to the column.
             * @param obj the object to write
             * @
             */
            public virtual void write(object obj)
            {
                if (obj != null)
                {
                    indexStatistics.increment();
                }
                else
                {
                    indexStatistics.setNull();
                }
                if (isPresent != null)
                {
                    isPresent.write(obj == null ? 0 : 1);
                    if (obj == null)
                    {
                        foundNulls = true;
                    }
                }
            }

            private void removeIsPresentPositions()
            {
                for (int i = 0; i < rowIndex.EntryCount; ++i)
                {
                    OrcProto.RowIndexEntry.Builder entry = rowIndex.GetEntry(i).ToBuilder();
                    IList<ulong> positions = entry.PositionsList;
                    // bit streams use 3 positions if uncompressed, 4 if compressed
                    positions = positions.subList(isCompressed ? 4 : 3, positions.Count);
                    entry.ClearPositions();
                    entry.AddRangePositions(positions);
                }
            }

            /**
             * Write the stripe out to the file.
             * @param builder the stripe footer that contains the information about the
             *                layout of the stripe. The TreeWriter is required to update
             *                the footer with its information.
             * @param requiredIndexEntries the number of index entries that are
             *                             required. this is to check to make sure the
             *                             row index is well formed.
             * @
             */
            public virtual void writeStripe(OrcProto.StripeFooter.Builder builder,
                             int requiredIndexEntries)
            {
                if (isPresent != null)
                {
                    isPresent.flush();

                    // if no nulls are found in a stream, then suppress the stream
                    if (!foundNulls)
                    {
                        isPresentOutStream.suppress();
                        // since isPresent bitstream is suppressed, update the index to
                        // remove the positions of the isPresent stream
                        if (rowIndexStream != null)
                        {
                            removeIsPresentPositions();
                        }
                    }
                }

                // merge stripe-level column statistics to file statistics and write it to
                // stripe statistics
                OrcProto.StripeStatistics.Builder stripeStatsBuilder = OrcProto.StripeStatistics.CreateBuilder();
                writeStripeStatistics(stripeStatsBuilder, this);
                stripeStatsBuilders.Add(stripeStatsBuilder);

                // reset the flag for next stripe
                foundNulls = false;

                builder.AddColumns(getEncoding());
                if (streamFactory.hasWriterTimeZone())
                {
                    builder.WriterTimezone = TimeZone.CurrentTimeZone.StandardName;
                }
                if (rowIndexStream != null)
                {
                    if (rowIndex.EntryCount != requiredIndexEntries)
                    {
                        throw new ArgumentException("Column has wrong number of " +
                             "index entries found: " + rowIndex.EntryCount + " expected: " +
                             requiredIndexEntries);
                    }
                    rowIndex.Build().WriteTo(rowIndexStream);
                    rowIndexStream.Flush();
                }
                rowIndex.Clear();
                rowIndexEntry.Clear();

                // write the bloom filter to out stream
                if (bloomFilterStream != null)
                {
                    bloomFilterIndex.Build().WriteTo(bloomFilterStream);
                    bloomFilterStream.Flush();
                    bloomFilterIndex.Clear();
                    bloomFilterEntry.Clear();
                }
            }

            private void writeStripeStatistics(OrcProto.StripeStatistics.Builder builder, TreeWriter treeWriter)
            {
                treeWriter.fileStatistics.merge(treeWriter.stripeColStatistics);
                builder.AddColStats(treeWriter.stripeColStatistics.serialize().Build());
                treeWriter.stripeColStatistics.reset();
                foreach (TreeWriter child in treeWriter.getChildrenWriters())
                {
                    writeStripeStatistics(builder, child);
                }
            }

            public TreeWriter[] getChildrenWriters()
            {
                return childrenWriters;
            }

            /**
             * Get the encoding for this column.
             * @return the information about the encoding of this column
             */
            public virtual OrcProto.ColumnEncoding getEncoding()
            {
                return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT);
            }

            /**
             * Create a row index entry with the previous location and the current
             * index statistics. Also merges the index statistics into the file
             * statistics before they are cleared. Finally, it records the start of the
             * next index and ensures all of the children columns also create an entry.
             * @
             */
            public virtual void createRowIndexEntry()
            {
                stripeColStatistics.merge(indexStatistics);
                rowIndexEntry.SetStatistics(indexStatistics.serialize());
                indexStatistics.reset();
                rowIndex.AddEntry(rowIndexEntry);
                rowIndexEntry.Clear();
                addBloomFilterEntry();
                recordPosition(rowIndexPosition);
                foreach (TreeWriter child in childrenWriters)
                {
                    child.createRowIndexEntry();
                }
            }

            protected void addBloomFilterEntry()
            {
                if (createBloomFilter)
                {
                    bloomFilterEntry.SetNumHashFunctions((uint)bloomFilter.getNumHashFunctions());
                    bloomFilterEntry.AddRangeBitset(bloomFilter.getBitSet());
                    bloomFilterIndex.AddBloomFilter(bloomFilterEntry.Build());
                    bloomFilter.reset();
                    bloomFilterEntry.Clear();
                }
            }

            /**
             * Record the current position in each of this column's streams.
             * @param recorder where should the locations be recorded
             * @
             */
            public virtual void recordPosition(PositionRecorder recorder)
            {
                if (isPresent != null)
                {
                    isPresent.getPosition(recorder);
                }
            }

            /**
             * Estimate how much memory the writer is consuming excluding the streams.
             * @return the number of bytes.
             */
            public virtual long estimateMemory()
            {
                long result = 0;
                foreach (TreeWriter child in childrenWriters)
                {
                    result += child.estimateMemory();
                }
                return result;
            }
        }

        private class BooleanTreeWriter : TreeWriter
        {
            private BitFieldWriter writer;

            public BooleanTreeWriter(int columnId,
                              ObjectInspector inspector,
                              TypeDescription schema,
                              StreamFactory writer,
                              bool nullable) :
                base(columnId, inspector, schema, writer, nullable)
            {
                PositionedOutputStream @out = writer.createStream(id,
                    OrcProto.Stream.Types.Kind.DATA);
                this.writer = new BitFieldWriter(@out, 1);
                recordPosition(rowIndexPosition);
            }

            public override void write(object obj)
            {
                base.write(obj);
                if (obj != null)
                {
                    bool val = ((BooleanObjectInspector)inspector).get(obj);
                    indexStatistics.updateBoolean(val);
                    writer.write(val ? 1 : 0);
                }
            }

            public override void writeStripe(OrcProto.StripeFooter.Builder builder,
                             int requiredIndexEntries)
            {
                base.writeStripe(builder, requiredIndexEntries);
                writer.flush();
                recordPosition(rowIndexPosition);
            }

            public override void recordPosition(PositionRecorder recorder)
            {
                base.recordPosition(recorder);
                writer.getPosition(recorder);
            }
        }

        private class ByteTreeWriter : TreeWriter
        {
            private RunLengthByteWriter writer;

            public ByteTreeWriter(int columnId,
                              ObjectInspector inspector,
                              TypeDescription schema,
                              StreamFactory writer,
                              bool nullable) :
                base(columnId, inspector, schema, writer, nullable)
            {
                this.writer = new RunLengthByteWriter(writer.createStream(id,
                    OrcProto.Stream.Types.Kind.DATA));
                recordPosition(rowIndexPosition);
            }

            public override void write(object obj)
            {
                base.write(obj);
                if (obj != null)
                {
                    byte val = ((ByteObjectInspector)inspector).get(obj);
                    indexStatistics.updateInteger(val);
                    if (createBloomFilter)
                    {
                        bloomFilter.addLong(val);
                    }
                    writer.write(val);
                }
            }

            public override void writeStripe(OrcProto.StripeFooter.Builder builder,
                             int requiredIndexEntries)
            {
                base.writeStripe(builder, requiredIndexEntries);
                writer.flush();
                recordPosition(rowIndexPosition);
            }

            public override void recordPosition(PositionRecorder recorder)
            {
                base.recordPosition(recorder);
                writer.getPosition(recorder);
            }
        }

        private class IntegerTreeWriter : TreeWriter
        {
            private IntegerWriter writer;
            private ShortObjectInspector shortInspector;
            private IntObjectInspector intInspector;
            private LongObjectInspector longInspector;
            private bool isDirectV2 = true;

            public IntegerTreeWriter(int columnId,
                              ObjectInspector inspector,
                              TypeDescription schema,
                              StreamFactory writer,
                              bool nullable) :
                base(columnId, inspector, schema, writer, nullable)
            {
                OutStream @out = writer.createStream(id,
                    OrcProto.Stream.Types.Kind.DATA);
                this.isDirectV2 = isNewWriteFormat(writer);
                this.writer = createIntegerWriter(@out, true, isDirectV2, writer);
                if (inspector is IntObjectInspector)
                {
                    intInspector = (IntObjectInspector)inspector;
                    shortInspector = null;
                    longInspector = null;
                }
                else
                {
                    intInspector = null;
                    if (inspector is LongObjectInspector)
                    {
                        longInspector = (LongObjectInspector)inspector;
                        shortInspector = null;
                    }
                    else
                    {
                        shortInspector = (ShortObjectInspector)inspector;
                        longInspector = null;
                    }
                }
                recordPosition(rowIndexPosition);
            }

            public override OrcProto.ColumnEncoding getEncoding()
            {
                if (isDirectV2)
                {
                    return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2);
                }
                return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT);
            }

            public override void write(object obj)
            {
                base.write(obj);
                if (obj != null)
                {
                    long val;
                    if (intInspector != null)
                    {
                        val = intInspector.get(obj);
                    }
                    else if (longInspector != null)
                    {
                        val = longInspector.get(obj);
                    }
                    else
                    {
                        val = shortInspector.get(obj);
                    }
                    indexStatistics.updateInteger(val);
                    if (createBloomFilter)
                    {
                        // integers are converted to longs in column statistics and during SARG evaluation
                        bloomFilter.addLong(val);
                    }
                    writer.write(val);
                }
            }

            public override void writeStripe(OrcProto.StripeFooter.Builder builder,
                             int requiredIndexEntries)
            {
                base.writeStripe(builder, requiredIndexEntries);
                writer.flush();
                recordPosition(rowIndexPosition);
            }

            public override void recordPosition(PositionRecorder recorder)
            {
                base.recordPosition(recorder);
                writer.getPosition(recorder);
            }
        }

        private class FloatTreeWriter : TreeWriter
        {
            private PositionedOutputStream stream;
            private SerializationUtils utils;

            public FloatTreeWriter(int columnId,
                              ObjectInspector inspector,
                              TypeDescription schema,
                              StreamFactory writer,
                              bool nullable) :
                base(columnId, inspector, schema, writer, nullable)
            {
                this.stream = writer.createStream(id,
                    OrcProto.Stream.Types.Kind.DATA);
                this.utils = new SerializationUtils();
                recordPosition(rowIndexPosition);
            }

            public override void write(object obj)
            {
                base.write(obj);
                if (obj != null)
                {
                    float val = ((FloatObjectInspector)inspector).get(obj);
                    indexStatistics.updateDouble(val);
                    if (createBloomFilter)
                    {
                        // floats are converted to doubles in column statistics and during SARG evaluation
                        bloomFilter.addDouble(val);
                    }
                    utils.writeFloat(stream, val);
                }
            }

            public override void writeStripe(OrcProto.StripeFooter.Builder builder,
                             int requiredIndexEntries)
            {
                base.writeStripe(builder, requiredIndexEntries);
                stream.Flush();
                recordPosition(rowIndexPosition);
            }

            public override void recordPosition(PositionRecorder recorder)
            {
                base.recordPosition(recorder);
                stream.getPosition(recorder);
            }
        }

        private class DoubleTreeWriter : TreeWriter
        {
            private PositionedOutputStream stream;
            private SerializationUtils utils;

            public DoubleTreeWriter(int columnId,
                            ObjectInspector inspector,
                            TypeDescription schema,
                            StreamFactory writer,
                            bool nullable) :
                base(columnId, inspector, schema, writer, nullable)
            {
                this.stream = writer.createStream(id,
                    OrcProto.Stream.Types.Kind.DATA);
                this.utils = new SerializationUtils();
                recordPosition(rowIndexPosition);
            }

            public override void write(object obj)
            {
                base.write(obj);
                if (obj != null)
                {
                    double val = ((DoubleObjectInspector)inspector).get(obj);
                    indexStatistics.updateDouble(val);
                    if (createBloomFilter)
                    {
                        bloomFilter.addDouble(val);
                    }
                    utils.writeDouble(stream, val);
                }
            }

            public override void writeStripe(OrcProto.StripeFooter.Builder builder,
                             int requiredIndexEntries)
            {
                base.writeStripe(builder, requiredIndexEntries);
                stream.Flush();
                recordPosition(rowIndexPosition);
            }

            public override void recordPosition(PositionRecorder recorder)
            {
                base.recordPosition(recorder);
                stream.getPosition(recorder);
            }
        }

        private class StringTreeWriter : TreeWriter
        {
            private const int INITIAL_DICTIONARY_SIZE = 4096;
            private OutStream stringOutput;
            private IntegerWriter lengthOutput;
            private IntegerWriter rowOutput;
            private StringRedBlackTree dictionary =
                new StringRedBlackTree(INITIAL_DICTIONARY_SIZE);
            private DynamicIntArray rows = new DynamicIntArray();
            private PositionedOutputStream directStreamOutput;
            private IntegerWriter directLengthOutput;
            private List<OrcProto.RowIndexEntry> savedRowIndex =
                new List<OrcProto.RowIndexEntry>();
            private bool buildIndex;
            private List<long> rowIndexValueCount = new List<long>();
            // If the number of keys in a dictionary is greater than this fraction of
            //the total number of non-null rows, turn off dictionary encoding
            private double dictionaryKeySizeThreshold;
            private bool useDictionaryEncoding = true;
            private bool isDirectV2 = true;
            private bool doneDictionaryCheck;
            private bool strideDictionaryCheck;

            public StringTreeWriter(int columnId,
                             ObjectInspector inspector,
                             TypeDescription schema,
                             StreamFactory writer,
                             bool nullable) :
                base(columnId, inspector, schema, writer, nullable)
            {
                this.isDirectV2 = isNewWriteFormat(writer);
                stringOutput = writer.createStream(id,
                    OrcProto.Stream.Types.Kind.DICTIONARY_DATA);
                lengthOutput = createIntegerWriter(writer.createStream(id,
                    OrcProto.Stream.Types.Kind.LENGTH), false, isDirectV2, writer);
                rowOutput = createIntegerWriter(writer.createStream(id,
                    OrcProto.Stream.Types.Kind.DATA), false, isDirectV2, writer);
                recordPosition(rowIndexPosition);
                rowIndexValueCount.Add(0L);
                buildIndex = writer.buildIndex();
                directStreamOutput = writer.createStream(id, OrcProto.Stream.Types.Kind.DATA);
                directLengthOutput = createIntegerWriter(writer.createStream(id,
                    OrcProto.Stream.Types.Kind.LENGTH), false, isDirectV2, writer);
                OrcFile.WriterOptions options = writer.getOptions();
                dictionaryKeySizeThreshold = options.getDictionaryKeySizeThreshold();
                strideDictionaryCheck = options.getStrideDictionaryCheck();
                doneDictionaryCheck = false;
            }

            /**
             * Method to retrieve text values from the value object, which can be overridden
             * by subclasses.
             * @param obj  value
             * @return Text text value from obj
             */
            string getTextValue(object obj)
            {
                return ((StringObjectInspector)inspector).get(obj);
            }

            public override void write(object obj)
            {
                base.write(obj);
                if (obj != null)
                {
                    string val = getTextValue(obj);
                    byte[] encodedVal = Encoding.UTF8.GetBytes(val);
                    if (useDictionaryEncoding || !strideDictionaryCheck)
                    {
                        rows.add(dictionary.add(encodedVal));
                    }
                    else
                    {
                        // write data and length
                        directStreamOutput.Write(encodedVal, 0, encodedVal.Length);
                        directLengthOutput.write(encodedVal.Length);
                    }
                    indexStatistics.updateString(val);
                    if (createBloomFilter)
                    {
                        bloomFilter.addBytes(encodedVal, 0, encodedVal.Length);
                    }
                }
            }

            private bool checkDictionaryEncoding()
            {
                if (!doneDictionaryCheck)
                {
                    // Set the flag indicating whether or not to use dictionary encoding
                    // based on whether or not the fraction of distinct keys over number of
                    // non-null rows is less than the configured threshold
                    float ratio = rows.size() > 0 ? (float)(dictionary.Size) / rows.size() : 0.0f;
                    useDictionaryEncoding = !isDirectV2 || ratio <= dictionaryKeySizeThreshold;
                    doneDictionaryCheck = true;
                }
                return useDictionaryEncoding;
            }

            public override void writeStripe(OrcProto.StripeFooter.Builder builder,
                             int requiredIndexEntries)
            {
                // if rows in stripe is less than dictionaryCheckAfterRows, dictionary
                // checking would not have happened. So do it again here.
                checkDictionaryEncoding();

                if (useDictionaryEncoding)
                {
                    flushDictionary();
                }
                else
                {
                    // flushout any left over entries from dictionary
                    if (rows.size() > 0)
                    {
                        flushDictionary();
                    }

                    // suppress the stream for every stripe if dictionary is disabled
                    stringOutput.suppress();
                }

                // we need to build the rowindex before calling super, since it
                // writes it out.
                base.writeStripe(builder, requiredIndexEntries);
                stringOutput.Flush();
                lengthOutput.flush();
                rowOutput.flush();
                directStreamOutput.Flush();
                directLengthOutput.flush();
                // reset all of the fields to be ready for the next stripe.
                dictionary.clear();
                savedRowIndex.Clear();
                rowIndexValueCount.Clear();
                recordPosition(rowIndexPosition);
                rowIndexValueCount.Add(0L);

                if (!useDictionaryEncoding)
                {
                    // record the start positions of first index stride of next stripe i.e
                    // beginning of the direct streams when dictionary is disabled
                    recordDirectStreamPosition();
                }
            }

            private void flushDictionary()
            {
                int[] dumpOrder = new int[dictionary.Size];

                if (useDictionaryEncoding)
                {
                    // Write the dictionary by traversing the red-black tree writing out
                    // the bytes and lengths; and creating the map from the original order
                    // to the sorted order.

                    dictionary.visit(new StringWriterTreeVisitor(this, dumpOrder));
                }
                else
                {
                    // for direct encoding, we don't want the dictionary data stream
                    stringOutput.suppress();
                }
                int length = rows.size();
                int rowIndexEntry = 0;
                OrcProto.RowIndex.Builder rowIndex = getRowIndex();
                // write the values translated into the dump order.
                for (int i = 0; i <= length; ++i)
                {
                    // now that we are writing out the row values, we can finalize the
                    // row index
                    if (buildIndex)
                    {
                        while (i == rowIndexValueCount[rowIndexEntry] &&
                            rowIndexEntry < savedRowIndex.Count)
                        {
                            OrcProto.RowIndexEntry.Builder @base =
                                savedRowIndex[rowIndexEntry++].ToBuilder();
                            if (useDictionaryEncoding)
                            {
                                rowOutput.getPosition(new RowIndexPositionRecorder(@base));
                            }
                            else
                            {
                                PositionRecorder posn = new RowIndexPositionRecorder(@base);
                                directStreamOutput.getPosition(posn);
                                directLengthOutput.getPosition(posn);
                            }
                            rowIndex.AddEntry(@base.Build());
                        }
                    }
                    if (i != length)
                    {
                        if (useDictionaryEncoding)
                        {
                            rowOutput.write(dumpOrder[rows.get(i)]);
                        }
                        else
                        {
                            byte[] text = Encoding.UTF8.GetBytes(dictionary.getText(rows.get(i)));
                            directStreamOutput.Write(text, 0, text.Length);
                            directLengthOutput.write(text.Length);
                        }
                    }
                }
                rows.clear();
            }

            class StringWriterTreeVisitor : StringRedBlackTree.Visitor
            {
                private readonly StringTreeWriter writer;
                private readonly int[] dumpOrder;
                private int currentId = 0;

                public StringWriterTreeVisitor(StringTreeWriter writer, int[] dumpOrder)
                {
                    this.writer = writer;
                    this.dumpOrder = dumpOrder;
                }

                public void visit(StringRedBlackTree.VisitorContext context)
                {
                    context.writeBytes(writer.stringOutput);
                    writer.lengthOutput.write(context.getLength());
                    dumpOrder[context.getOriginalPosition()] = currentId++;
                }
            }

            public override OrcProto.ColumnEncoding getEncoding()
            {
                // Returns the encoding used for the last call to writeStripe
                if (useDictionaryEncoding)
                {
                    if (isDirectV2)
                    {
                        return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DICTIONARY_V2, dictionary.Size);
                    }
                    return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DICTIONARY, dictionary.Size);
                }
                else
                {
                    if (isDirectV2)
                    {
                        return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2);
                    }
                    return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT);
                }
            }

            /**
             * This method doesn't call the super method, because unlike most of the
             * other TreeWriters, this one can't record the position in the streams
             * until the stripe is being flushed. Therefore it saves all of the entries
             * and augments them with the information as the stripe is written.
             * @
             */
            public override void createRowIndexEntry()
            {
                getStripeStatistics().merge(indexStatistics);
                OrcProto.RowIndexEntry.Builder rowIndexEntry = getRowIndexEntry();
                rowIndexEntry.SetStatistics(indexStatistics.serialize());
                indexStatistics.reset();
                OrcProto.RowIndexEntry @base = rowIndexEntry.Build();
                savedRowIndex.Add(@base);
                rowIndexEntry.Clear();
                addBloomFilterEntry();
                recordPosition(rowIndexPosition);
                rowIndexValueCount.Add(rows.size());
                if (strideDictionaryCheck)
                {
                    checkDictionaryEncoding();
                }
                if (!useDictionaryEncoding)
                {
                    if (rows.size() > 0)
                    {
                        flushDictionary();
                        // just record the start positions of next index stride
                        recordDirectStreamPosition();
                    }
                    else
                    {
                        // record the start positions of next index stride
                        recordDirectStreamPosition();
                        getRowIndex().AddEntry(@base);
                    }
                }
            }

            private void recordDirectStreamPosition()
            {
                directStreamOutput.getPosition(rowIndexPosition);
                directLengthOutput.getPosition(rowIndexPosition);
            }

            public override long estimateMemory()
            {
                return rows.getSizeInBytes() + dictionary.getSizeInBytes();
            }
        }

        /**
         * Under the covers, char is written to ORC the same way as string.
         */
        private class CharTreeWriter : StringTreeWriter
        {

            public CharTreeWriter(int columnId,
                ObjectInspector inspector,
                TypeDescription schema,
                StreamFactory writer,
                bool nullable) :
                base(columnId, inspector, schema, writer, nullable)
            {
            }

            /**
             * Override base class implementation to support char values.
             */
            string getTextValue(object obj)
            {
                return ((HiveCharObjectInspector)inspector).get(obj);
            }
        }

        /**
         * Under the covers, varchar is written to ORC the same way as string.
         */
        private class VarcharTreeWriter : StringTreeWriter
        {

            public VarcharTreeWriter(int columnId,
                ObjectInspector inspector,
                TypeDescription schema,
                StreamFactory writer,
                bool nullable) :
                base(columnId, inspector, schema, writer, nullable)
            {
            }

            /**
             * Override base class implementation to support varchar values.
             */
            string getTextValue(object obj)
            {
                return ((HiveVarcharObjectInspector)inspector).get();
            }
        }

        private class BinaryTreeWriter : TreeWriter
        {
            private PositionedOutputStream stream;
            private IntegerWriter length;
            private bool isDirectV2 = true;

            public BinaryTreeWriter(int columnId,
                             ObjectInspector inspector,
                             TypeDescription schema,
                             StreamFactory writer,
                             bool nullable) :
                base(columnId, inspector, schema, writer, nullable)
            {
                this.stream = writer.createStream(id,
                    OrcProto.Stream.Types.Kind.DATA);
                this.isDirectV2 = isNewWriteFormat(writer);
                this.length = createIntegerWriter(writer.createStream(id,
                    OrcProto.Stream.Types.Kind.LENGTH), false, isDirectV2, writer);
                recordPosition(rowIndexPosition);
            }

            public override OrcProto.ColumnEncoding getEncoding()
            {
                if (isDirectV2)
                {
                    return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2);
                }
                return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT);
            }

            public override void write(object obj)
            {
                base.write(obj);
                if (obj != null)
                {
                    byte[] val = ((BinaryObjectInspector)inspector).get(obj);
                    stream.Write(val, 0, val.Length);
                    length.write(val.Length);
                    indexStatistics.updateBinary(val);
                    if (createBloomFilter)
                    {
                        bloomFilter.addBytes(val, 0, val.Length);
                    }
                }
            }

            public override void writeStripe(OrcProto.StripeFooter.Builder builder,
                             int requiredIndexEntries)
            {
                base.writeStripe(builder, requiredIndexEntries);
                stream.Flush();
                length.flush();
                recordPosition(rowIndexPosition);
            }

            public override void recordPosition(PositionRecorder recorder)
            {
                base.recordPosition(recorder);
                stream.getPosition(recorder);
                length.getPosition(recorder);
            }
        }

        internal const int MILLIS_PER_SECOND = 1000;
        internal const string BASE_TIMESTAMP_STRING = "2015-01-01 00:00:00";

        private class TimestampTreeWriter : TreeWriter
        {
            private IntegerWriter seconds;
            private IntegerWriter nanos;
            private bool isDirectV2;
            private long base_timestamp;

            public TimestampTreeWriter(int columnId,
                             ObjectInspector inspector,
                             TypeDescription schema,
                             StreamFactory writer,
                             bool nullable) :
                base(columnId, inspector, schema, writer, nullable)
            {
                this.isDirectV2 = isNewWriteFormat(writer);
                this.seconds = createIntegerWriter(writer.createStream(id,
                    OrcProto.Stream.Types.Kind.DATA), true, isDirectV2, writer);
                this.nanos = createIntegerWriter(writer.createStream(id,
                    OrcProto.Stream.Types.Kind.SECONDARY), false, isDirectV2, writer);
                recordPosition(rowIndexPosition);
                // for unit tests to set different time zones
                this.base_timestamp = DateTime.Parse(BASE_TIMESTAMP_STRING).getTimestamp() / MILLIS_PER_SECOND;
                writer.useWriterTimeZone(true);
            }

            public override OrcProto.ColumnEncoding getEncoding()
            {
                if (isDirectV2)
                {
                    return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2);
                }
                return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT);
            }

            public override void write(object obj)
            {
                base.write(obj);
                if (obj != null)
                {
                    DateTime val = ((TimestampObjectInspector)inspector).get(obj);
                    indexStatistics.updateTimestamp(val);
                    seconds.write((val.getTimestamp() / MILLIS_PER_SECOND) - base_timestamp);
                    nanos.write(formatNanos(val.getNanos()));
                    if (createBloomFilter)
                    {
                        bloomFilter.addLong(val.getTimestamp());
                    }
                }
            }

            public override void writeStripe(OrcProto.StripeFooter.Builder builder,
                             int requiredIndexEntries)
            {
                base.writeStripe(builder, requiredIndexEntries);
                seconds.flush();
                nanos.flush();
                recordPosition(rowIndexPosition);
            }

            private static long formatNanos(int nanos)
            {
                if (nanos == 0)
                {
                    return 0;
                }
                else if (nanos % 100 != 0)
                {
                    return ((long)nanos) << 3;
                }
                else
                {
                    nanos /= 100;
                    int trailingZeros = 1;
                    while (nanos % 10 == 0 && trailingZeros < 7)
                    {
                        nanos /= 10;
                        trailingZeros += 1;
                    }
                    return ((long)nanos) << 3 | trailingZeros;
                }
            }

            public override void recordPosition(PositionRecorder recorder)
            {
                base.recordPosition(recorder);
                seconds.getPosition(recorder);
                nanos.getPosition(recorder);
            }
        }

        private class DateTreeWriter : TreeWriter
        {
            private IntegerWriter writer;
            private bool isDirectV2;

            public DateTreeWriter(int columnId,
                           ObjectInspector inspector,
                           TypeDescription schema,
                           StreamFactory writer,
                           bool nullable) :
                base(columnId, inspector, schema, writer, nullable)
            {
                OutStream @out = writer.createStream(id,
                    OrcProto.Stream.Types.Kind.DATA);
                this.isDirectV2 = isNewWriteFormat(writer);
                this.writer = createIntegerWriter(@out, true, isDirectV2, writer);
                recordPosition(rowIndexPosition);
            }

            public override void write(object obj)
            {
                base.write(obj);
                if (obj != null)
                {
                    DateTime val = ((DateObjectInspector)inspector).get(obj);
                    indexStatistics.updateDate(val);
                    writer.write(val.getDays());
                    if (createBloomFilter)
                    {
                        bloomFilter.addLong(val.getDays());
                    }
                }
            }

            public override void writeStripe(OrcProto.StripeFooter.Builder builder, int requiredIndexEntries)
            {
                base.writeStripe(builder, requiredIndexEntries);
                writer.flush();
                recordPosition(rowIndexPosition);
            }

            public override void recordPosition(PositionRecorder recorder)
            {
                base.recordPosition(recorder);
                writer.getPosition(recorder);
            }

            public override OrcProto.ColumnEncoding getEncoding()
            {
                if (isDirectV2)
                {
                    return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2);
                }
                return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT);
            }
        }

        private class DecimalTreeWriter : TreeWriter
        {
            private PositionedOutputStream valueStream;
            private IntegerWriter scaleStream;
            private bool isDirectV2;

            public DecimalTreeWriter(int columnId,
                                ObjectInspector inspector,
                                TypeDescription schema,
                                StreamFactory writer,
                                bool nullable) :
                base(columnId, inspector, schema, writer, nullable)
            {
                this.isDirectV2 = isNewWriteFormat(writer);
                valueStream = writer.createStream(id, OrcProto.Stream.Types.Kind.DATA);
                this.scaleStream = createIntegerWriter(writer.createStream(id,
                    OrcProto.Stream.Types.Kind.SECONDARY), true, isDirectV2, writer);
                recordPosition(rowIndexPosition);
            }

            public override OrcProto.ColumnEncoding getEncoding()
            {
                if (isDirectV2)
                {
                    return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2);
                }
                return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT);
            }

            public override void write(object obj)
            {
                base.write(obj);
                if (obj != null)
                {
                    HiveDecimal @decimal = ((HiveDecimalObjectInspector)inspector).get(obj);
                    if (@decimal == null)
                    {
                        return;
                    }
                    SerializationUtils.writeBigInteger(valueStream,
                        @decimal.unscaledValue());
                    scaleStream.write(@decimal.scale());
                    indexStatistics.updateDecimal(@decimal);
                    if (createBloomFilter)
                    {
                        bloomFilter.addString(@decimal.ToString());
                    }
                }
            }

            public override void writeStripe(OrcProto.StripeFooter.Builder builder,
                             int requiredIndexEntries)
            {
                base.writeStripe(builder, requiredIndexEntries);
                valueStream.Flush();
                scaleStream.flush();
                recordPosition(rowIndexPosition);
            }

            public override void recordPosition(PositionRecorder recorder)
            {
                base.recordPosition(recorder);
                valueStream.getPosition(recorder);
                scaleStream.getPosition(recorder);
            }
        }

        private class StructTreeWriter : TreeWriter
        {
            private IList<StructField> fields;

            public StructTreeWriter(int columnId,
                             ObjectInspector inspector,
                             TypeDescription schema,
                             StreamFactory writer,
                             bool nullable) :
                base(columnId, inspector, schema, writer, nullable)
            {
                IList<TypeDescription> children = schema.getChildren();
                StructObjectInspector structObjectInspector =
                  (StructObjectInspector)inspector;
                fields = structObjectInspector.getAllStructFieldRefs();
                childrenWriters = new TreeWriter[children.Count];
                for (int i = 0; i < childrenWriters.Length; ++i)
                {
                    ObjectInspector childOI = i < fields.Count ?
                        fields[i].getFieldObjectInspector() : null;
                    childrenWriters[i] = createTreeWriter(
                      childOI, children[i], writer,
                      true);
                }
                recordPosition(rowIndexPosition);
            }

            public override void write(object obj)
            {
                base.write(obj);
                if (obj != null)
                {
                    StructObjectInspector insp = (StructObjectInspector)inspector;
                    for (int i = 0; i < fields.Count; ++i)
                    {
                        StructField field = fields[i];
                        TreeWriter writer = childrenWriters[i];
                        writer.write(insp.getStructFieldData(obj, field));
                    }
                }
            }

            public override void writeStripe(OrcProto.StripeFooter.Builder builder,
                             int requiredIndexEntries)
            {
                base.writeStripe(builder, requiredIndexEntries);
                foreach (TreeWriter child in childrenWriters)
                {
                    child.writeStripe(builder, requiredIndexEntries);
                }
                recordPosition(rowIndexPosition);
            }
        }

        private class ListTreeWriter : TreeWriter
        {
            private IntegerWriter lengths;
            private bool isDirectV2;

            public ListTreeWriter(int columnId,
                           ObjectInspector inspector,
                           TypeDescription schema,
                           StreamFactory writer,
                           bool nullable) :
                base(columnId, inspector, schema, writer, nullable)
            {
                this.isDirectV2 = isNewWriteFormat(writer);
                ObjectInspector childOI =
                  ((ListObjectInspector)inspector).getListElementObjectInspector();
                childrenWriters = new TreeWriter[1];
                childrenWriters[0] =
                  createTreeWriter(childOI, schema.getChildren()[0], writer, true);
                lengths = createIntegerWriter(writer.createStream(columnId,
                    OrcProto.Stream.Types.Kind.LENGTH), false, isDirectV2, writer);
                recordPosition(rowIndexPosition);
            }

            public override OrcProto.ColumnEncoding getEncoding()
            {
                if (isDirectV2)
                {
                    return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2);
                }
                return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT);
            }

            public override void write(object obj)
            {
                base.write(obj);
                if (obj != null)
                {
                    ListObjectInspector insp = (ListObjectInspector)inspector;
                    int len = insp.getListLength(obj);
                    lengths.write(len);
                    if (createBloomFilter)
                    {
                        bloomFilter.addLong(len);
                    }
                    for (int i = 0; i < len; ++i)
                    {
                        childrenWriters[0].write(insp.getListElement(obj, i));
                    }
                }
            }

            public override void writeStripe(OrcProto.StripeFooter.Builder builder,
                             int requiredIndexEntries)
            {
                base.writeStripe(builder, requiredIndexEntries);
                lengths.flush();
                foreach (TreeWriter child in childrenWriters)
                {
                    child.writeStripe(builder, requiredIndexEntries);
                }
                recordPosition(rowIndexPosition);
            }

            public override void recordPosition(PositionRecorder recorder)
            {
                base.recordPosition(recorder);
                lengths.getPosition(recorder);
            }
        }

        private class MapTreeWriter : TreeWriter
        {
            private IntegerWriter lengths;
            private bool isDirectV2;

            public MapTreeWriter(int columnId,
                          ObjectInspector inspector,
                          TypeDescription schema,
                          StreamFactory writer,
                          bool nullable) :
                base(columnId, inspector, schema, writer, nullable)
            {
                this.isDirectV2 = isNewWriteFormat(writer);
                MapObjectInspector insp = (MapObjectInspector)inspector;
                childrenWriters = new TreeWriter[2];
                IList<TypeDescription> children = schema.getChildren();
                childrenWriters[0] =
                  createTreeWriter(insp.getMapKeyObjectInspector(), children[0],
                                   writer, true);
                childrenWriters[1] =
                  createTreeWriter(insp.getMapValueObjectInspector(), children[1],
                                   writer, true);
                lengths = createIntegerWriter(writer.createStream(columnId,
                    OrcProto.Stream.Types.Kind.LENGTH), false, isDirectV2, writer);
                recordPosition(rowIndexPosition);
            }

            public override OrcProto.ColumnEncoding getEncoding()
            {
                if (isDirectV2)
                {
                    return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2);
                }
                return MakeEncoding(OrcProto.ColumnEncoding.Types.Kind.DIRECT);
            }

            public override void write(object obj)
            {
                base.write(obj);
                if (obj != null)
                {
                    MapObjectInspector insp = (MapObjectInspector)inspector;
                    // this sucks, but it will have to do until we can get a better
                    // accessor in the MapObjectInspector.
                    IDictionary<object, object> valueMap = insp.getMap(obj);
                    lengths.write(valueMap.Count);
                    if (createBloomFilter)
                    {
                        bloomFilter.addLong(valueMap.Count);
                    }
                    foreach (KeyValuePair<object, object> entry in valueMap)
                    {
                        childrenWriters[0].write(entry.Key);
                        childrenWriters[1].write(entry.Value);
                    }
                }
            }

            public override void writeStripe(OrcProto.StripeFooter.Builder builder,
                             int requiredIndexEntries)
            {
                base.writeStripe(builder, requiredIndexEntries);
                lengths.flush();
                foreach (TreeWriter child in childrenWriters)
                {
                    child.writeStripe(builder, requiredIndexEntries);
                }
                recordPosition(rowIndexPosition);
            }

            public override void recordPosition(PositionRecorder recorder)
            {
                base.recordPosition(recorder);
                lengths.getPosition(recorder);
            }
        }

        private class UnionTreeWriter : TreeWriter
        {
            private RunLengthByteWriter tags;

            public UnionTreeWriter(int columnId,
                          ObjectInspector inspector,
                          TypeDescription schema,
                          StreamFactory writer,
                          bool nullable) :
                base(columnId, inspector, schema, writer, nullable)
            {
                UnionObjectInspector insp = (UnionObjectInspector)inspector;
                IList<ObjectInspector> choices = insp.getObjectInspectors();
                IList<TypeDescription> children = schema.getChildren();
                childrenWriters = new TreeWriter[children.Count];
                for (int i = 0; i < childrenWriters.Length; ++i)
                {
                    childrenWriters[i] = createTreeWriter(choices[i],
                                                          children[i], writer, true);
                }
                tags =
                  new RunLengthByteWriter(writer.createStream(columnId,
                      OrcProto.Stream.Types.Kind.DATA));
                recordPosition(rowIndexPosition);
            }

            public override void write(object obj)
            {
                base.write(obj);
                if (obj != null)
                {
                    UnionObjectInspector insp = (UnionObjectInspector)inspector;
                    byte tag = insp.getTag(obj);
                    tags.write(tag);
                    if (createBloomFilter)
                    {
                        bloomFilter.addLong(tag);
                    }
                    childrenWriters[tag].write(insp.getField(obj));
                }
            }

            public override void writeStripe(OrcProto.StripeFooter.Builder builder,
                             int requiredIndexEntries)
            {
                base.writeStripe(builder, requiredIndexEntries);
                tags.flush();
                foreach (TreeWriter child in childrenWriters)
                {
                    child.writeStripe(builder, requiredIndexEntries);
                }
                recordPosition(rowIndexPosition);
            }

            public override void recordPosition(PositionRecorder recorder)
            {
                base.recordPosition(recorder);
                tags.getPosition(recorder);
            }
        }

        private static TreeWriter createTreeWriter(ObjectInspector inspector,
                                                   TypeDescription schema,
                                                   StreamFactory streamFactory,
                                                   bool nullable)
        {
            switch (schema.getCategory())
            {
                case Category.BOOLEAN:
                    return new BooleanTreeWriter(streamFactory.getNextColumnId(),
                        inspector, schema, streamFactory, nullable);
                case Category.BYTE:
                    return new ByteTreeWriter(streamFactory.getNextColumnId(),
                        inspector, schema, streamFactory, nullable);
                case Category.SHORT:
                case Category.INT:
                case Category.LONG:
                    return new IntegerTreeWriter(streamFactory.getNextColumnId(),
                        inspector, schema, streamFactory, nullable);
                case Category.FLOAT:
                    return new FloatTreeWriter(streamFactory.getNextColumnId(),
                        inspector, schema, streamFactory, nullable);
                case Category.DOUBLE:
                    return new DoubleTreeWriter(streamFactory.getNextColumnId(),
                        inspector, schema, streamFactory, nullable);
                case Category.STRING:
                    return new StringTreeWriter(streamFactory.getNextColumnId(),
                        inspector, schema, streamFactory, nullable);
                case Category.CHAR:
                    return new CharTreeWriter(streamFactory.getNextColumnId(),
                        inspector, schema, streamFactory, nullable);
                case Category.VARCHAR:
                    return new VarcharTreeWriter(streamFactory.getNextColumnId(),
                        inspector, schema, streamFactory, nullable);
                case Category.BINARY:
                    return new BinaryTreeWriter(streamFactory.getNextColumnId(),
                        inspector, schema, streamFactory, nullable);
                case Category.TIMESTAMP:
                    return new TimestampTreeWriter(streamFactory.getNextColumnId(),
                        inspector, schema, streamFactory, nullable);
                case Category.DATE:
                    return new DateTreeWriter(streamFactory.getNextColumnId(),
                        inspector, schema, streamFactory, nullable);
                case Category.DECIMAL:
                    return new DecimalTreeWriter(streamFactory.getNextColumnId(),
                        inspector, schema, streamFactory, nullable);
                case Category.STRUCT:
                    return new StructTreeWriter(streamFactory.getNextColumnId(),
                        inspector, schema, streamFactory, nullable);
                case Category.MAP:
                    return new MapTreeWriter(streamFactory.getNextColumnId(), inspector,
                        schema, streamFactory, nullable);
                case Category.LIST:
                    return new ListTreeWriter(streamFactory.getNextColumnId(), inspector,
                        schema, streamFactory, nullable);
                case Category.UNION:
                    return new UnionTreeWriter(streamFactory.getNextColumnId(), inspector,
                        schema, streamFactory, nullable);
                default:
                    throw new ArgumentException("Bad category: " +
                        schema.getCategory());
            }
        }

        private static void writeTypes(OrcProto.Footer.Builder builder,
                                       TypeDescription schema)
        {
            OrcProto.Type.Builder type = OrcProto.Type.CreateBuilder();
            IList<TypeDescription> children = schema.getChildren();
            switch (schema.getCategory())
            {
                case Category.BOOLEAN:
                    type.Kind = OrcProto.Type.Types.Kind.BOOLEAN;
                    break;
                case Category.BYTE:
                    type.Kind = OrcProto.Type.Types.Kind.BYTE;
                    break;
                case Category.SHORT:
                    type.Kind = OrcProto.Type.Types.Kind.SHORT;
                    break;
                case Category.INT:
                    type.Kind = OrcProto.Type.Types.Kind.INT;
                    break;
                case Category.LONG:
                    type.Kind = OrcProto.Type.Types.Kind.LONG;
                    break;
                case Category.FLOAT:
                    type.Kind = OrcProto.Type.Types.Kind.FLOAT;
                    break;
                case Category.DOUBLE:
                    type.Kind = OrcProto.Type.Types.Kind.DOUBLE;
                    break;
                case Category.STRING:
                    type.Kind = OrcProto.Type.Types.Kind.STRING;
                    break;
                case Category.CHAR:
                    type.Kind = OrcProto.Type.Types.Kind.CHAR;
                    type.MaximumLength = (uint)schema.getMaxLength();
                    break;
                case Category.VARCHAR:
                    type.Kind = OrcProto.Type.Types.Kind.VARCHAR;
                    type.MaximumLength = (uint)schema.getMaxLength();
                    break;
                case Category.BINARY:
                    type.Kind = OrcProto.Type.Types.Kind.BINARY;
                    break;
                case Category.TIMESTAMP:
                    type.Kind = OrcProto.Type.Types.Kind.TIMESTAMP;
                    break;
                case Category.DATE:
                    type.Kind = OrcProto.Type.Types.Kind.DATE;
                    break;
                case Category.DECIMAL:
                    type.Kind = OrcProto.Type.Types.Kind.DECIMAL;
                    type.Precision = (uint)schema.getPrecision();
                    type.Scale = (uint)schema.getScale();
                    break;
                case Category.LIST:
                    type.Kind = OrcProto.Type.Types.Kind.LIST;
                    type.AddSubtypes((uint)children[0].getId());
                    break;
                case Category.MAP:
                    type.Kind = OrcProto.Type.Types.Kind.MAP;
                    foreach (TypeDescription t in children)
                    {
                        type.AddSubtypes((uint)t.getId());
                    }
                    break;
                case Category.STRUCT:
                    type.Kind = OrcProto.Type.Types.Kind.STRUCT;
                    foreach (TypeDescription t in children)
                    {
                        type.AddSubtypes((uint)t.getId());
                    }
                    foreach (string field in schema.getFieldNames())
                    {
                        type.AddFieldNames(field);
                    }
                    break;
                case Category.UNION:
                    type.Kind = OrcProto.Type.Types.Kind.UNION;
                    foreach (TypeDescription t in children)
                    {
                        type.AddSubtypes((uint)t.getId());
                    }
                    break;
                default:
                    throw new ArgumentException("Unknown category: " +
                      schema.getCategory());
            }
            builder.AddTypes(type);
            if (children != null)
            {
                foreach (TypeDescription child in children)
                {
                    writeTypes(builder, child);
                }
            }
        }

        Stream getStream()
        {
            if (rawWriter == null)
            {
                rawWriter = baseStream;
                rawWriter.Write(Encoding.UTF8.GetBytes(OrcFile.MAGIC), 0, 3);
                headerLength = rawWriter.Position;
                writer = new OutStream("metadata", bufferSize, codec, new DirectStream(rawWriter));
                protobufWriter = CodedOutputStream.CreateInstance(writer);
            }
            return rawWriter;
        }

        void createRowIndexEntry()
        {
            treeWriter.createRowIndexEntry();
            rowsInIndex = 0;
        }

        private void flushStripe()
        {
            getStream();
            if (buildIndex && rowsInIndex != 0)
            {
                createRowIndexEntry();
            }
            if (rowsInStripe != 0)
            {
                if (callback != null)
                {
                    callback.preStripeWrite(this);
                }
                // finalize the data for the stripe
                int requiredIndexEntries = rowIndexStride == 0 ? 0 :
                    (int)((rowsInStripe + rowIndexStride - 1) / rowIndexStride);
                OrcProto.StripeFooter.Builder builder =
                    OrcProto.StripeFooter.CreateBuilder();
                treeWriter.writeStripe(builder, requiredIndexEntries);
                long indexSize = 0;
                long dataSize = 0;
                foreach (KeyValuePair<StreamName, BufferedStream> pair in streams)
                {
                    BufferedStream stream = pair.Value;
                    if (!stream.isSuppressed())
                    {
                        stream.flush();
                        StreamName name = pair.Key;
                        long streamSize = pair.Value.getOutputSize();
                        OrcProto.Stream.Builder streamBuilder = OrcProto.Stream.CreateBuilder();
                        streamBuilder.Column = (uint)name.getColumn();
                        streamBuilder.Kind = name.getKind();
                        streamBuilder.Length = (ulong)streamSize;
                        builder.AddStreams(streamBuilder);
                        if (StreamName.Area.INDEX == name.getArea())
                        {
                            indexSize += streamSize;
                        }
                        else
                        {
                            dataSize += streamSize;
                        }
                    }
                }
                OrcProto.StripeFooter footer = builder.Build();

                // Do we need to pad the file so the stripe doesn't straddle a block
                // boundary?
                long start = rawWriter.Position;
                long currentStripeSize = indexSize + dataSize + footer.SerializedSize;
                long available = blockSize - (start % blockSize);
                long overflow = currentStripeSize - adjustedStripeSize;
                float availRatio = (float)available / (float)defaultStripeSize;

                if (availRatio > 0.0f && availRatio < 1.0f
                    && availRatio > paddingTolerance)
                {
                    // adjust default stripe size to fit into remaining space, also adjust
                    // the next stripe for correction based on the current stripe size
                    // and user specified padding tolerance. Since stripe size can overflow
                    // the default stripe size we should apply this correction to avoid
                    // writing portion of last stripe to next hdfs block.
                    double correction = overflow > 0 ? (double)overflow
                        / (double)adjustedStripeSize : 0.0;

                    // correction should not be greater than user specified padding
                    // tolerance
                    correction = correction > paddingTolerance ? paddingTolerance
                        : correction;

                    // adjust next stripe size based on current stripe estimate correction
                    adjustedStripeSize = (long)((1.0f - correction) * (availRatio * defaultStripeSize));
                }
                else if (availRatio >= 1.0)
                {
                    adjustedStripeSize = defaultStripeSize;
                }

                if (availRatio < paddingTolerance && addBlockPadding)
                {
                    long padding = blockSize - (start % blockSize);
                    byte[] pad = new byte[(int)Math.Min(HDFS_BUFFER_SIZE, padding)];
                    LOG.info(String.Format("Padding ORC by {0} bytes (<=  {1} * {2})",
                        padding, availRatio, defaultStripeSize));
                    start += padding;
                    while (padding > 0)
                    {
                        int writeLen = (int)Math.Min(padding, pad.Length);
                        rawWriter.Write(pad, 0, writeLen);
                        padding -= writeLen;
                    }
                    adjustedStripeSize = defaultStripeSize;
                }
                else if (currentStripeSize < blockSize
                  && (start % blockSize) + currentStripeSize > blockSize)
                {
                    // even if you don't pad, reset the default stripe size when crossing a
                    // block boundary
                    adjustedStripeSize = defaultStripeSize;
                }

                // write out the data streams
                foreach (KeyValuePair<StreamName, BufferedStream> pair in streams)
                {
                    BufferedStream stream = pair.Value;
                    if (!stream.isSuppressed())
                    {
                        stream.spillTo(rawWriter);
                    }
                    stream.clear();
                }
                footer.WriteTo(protobufWriter);
                protobufWriter.Flush();
                writer.Flush();
                long footerLength = rawWriter.Position - start - dataSize - indexSize;
                OrcProto.StripeInformation.Builder dirEntry = OrcProto.StripeInformation.CreateBuilder();
                dirEntry.Offset = (ulong)start;
                dirEntry.NumberOfRows = (ulong)rowsInStripe;
                dirEntry.IndexLength = (ulong)indexSize;
                dirEntry.DataLength = (ulong)dataSize;
                dirEntry.FooterLength = (ulong)footerLength;
                stripes.Add(dirEntry.Build());
                rowCount += rowsInStripe;
                rowsInStripe = 0;
            }
        }

        private long computeRawDataSize()
        {
            return getRawDataSize(treeWriter, schema);
        }

        private long getRawDataSize(TreeWriter child, TypeDescription schema)
        {
            long total = 0;
            long numVals = child.fileStatistics.getNumberOfValues();
            switch (schema.getCategory())
            {
                case Category.BOOLEAN:
                case Category.BYTE:
                case Category.SHORT:
                case Category.INT:
                case Category.FLOAT:
                    return numVals * JavaDataModel.Four;
                case Category.LONG:
                case Category.DOUBLE:
                    return numVals * JavaDataModel.Eight;
                case Category.STRING:
                case Category.VARCHAR:
                case Category.CHAR:
                    // ORC strings are converted to java Strings. so use JavaDataModel to
                    // compute the overall size of strings
                    StringColumnStatistics scs = (StringColumnStatistics)child.fileStatistics;
                    numVals = numVals == 0 ? 1 : numVals;
                    int avgStringLen = (int)(scs.getSum() / numVals);
                    return numVals * JavaDataModel.lengthForStringOfLength(avgStringLen);
                case Category.DECIMAL:
                    return numVals * JavaDataModel.lengthOfDecimal();
                case Category.DATE:
                    return numVals * JavaDataModel.lengthOfDate();
                case Category.BINARY:
                    // get total length of binary blob
                    BinaryColumnStatistics bcs = (BinaryColumnStatistics)child.fileStatistics;
                    return bcs.getSum();
                case Category.TIMESTAMP:
                    return numVals * JavaDataModel.lengthOfTimestamp();
                case Category.LIST:
                case Category.MAP:
                case Category.UNION:
                case Category.STRUCT:
                    {
                        TreeWriter[] childWriters = child.getChildrenWriters();
                        IList<TypeDescription> childTypes = schema.getChildren();
                        for (int i = 0; i < childWriters.Length; ++i)
                        {
                            total += getRawDataSize(childWriters[i], childTypes[i]);
                        }
                        break;
                    }
                default:
                    LOG.debug("Unknown object inspector category.");
                    break;
            }
            return total;
        }

        private OrcProto.CompressionKind writeCompressionKind(CompressionKind kind)
        {
            switch (kind)
            {
                case CompressionKind.NONE: return OrcProto.CompressionKind.NONE;
                case CompressionKind.ZLIB: return OrcProto.CompressionKind.ZLIB;
                case CompressionKind.SNAPPY: return OrcProto.CompressionKind.SNAPPY;
                case CompressionKind.LZO: return OrcProto.CompressionKind.LZO;
                default:
                    throw new ArgumentException("Unknown compression " + kind);
            }
        }

        private void writeFileStatistics(OrcProto.Footer.Builder builder,
                                         TreeWriter writer)
        {
            builder.AddStatistics(writer.fileStatistics.serialize());
            foreach (TreeWriter child in writer.getChildrenWriters())
            {
                writeFileStatistics(builder, child);
            }
        }

        private int writeMetadata(long bodyLength)
        {
            getStream();
            OrcProto.Metadata.Builder builder = OrcProto.Metadata.CreateBuilder();
            foreach (OrcProto.StripeStatistics.Builder ssb in treeWriter.stripeStatsBuilders)
            {
                builder.AddStripeStats(ssb.Build());
            }

            long startPosn = rawWriter.Position;
            OrcProto.Metadata metadata = builder.Build();
            metadata.WriteTo(protobufWriter);
            protobufWriter.Flush();
            writer.Flush();
            return (int)(rawWriter.Position - startPosn);
        }

        private int writeFooter(long bodyLength)
        {
            getStream();
            OrcProto.Footer.Builder builder = OrcProto.Footer.CreateBuilder();
            builder.ContentLength = (ulong)bodyLength;
            builder.HeaderLength = (ulong)headerLength;
            builder.NumberOfRows = (ulong)rowCount;
            builder.RowIndexStride = (uint)rowIndexStride;
            // populate raw data size
            rawDataSize = computeRawDataSize();
            // serialize the types
            writeTypes(builder, schema);
            // add the stripe information
            foreach (OrcProto.StripeInformation stripe in stripes)
            {
                builder.AddStripes(stripe);
            }
            // add the column statistics
            writeFileStatistics(builder, treeWriter);
            // add all of the user metadata
            foreach (KeyValuePair<string, ByteString> entry in userMetadata)
            {
                OrcProto.UserMetadataItem.Builder metaBuilder = OrcProto.UserMetadataItem.CreateBuilder();
                metaBuilder.Name = entry.Key;
                metaBuilder.Value = entry.Value;
                builder.AddMetadata(metaBuilder.Build());
            }
            long startPosn = rawWriter.Position;
            OrcProto.Footer footer = builder.Build();
            footer.WriteTo(protobufWriter);
            protobufWriter.Flush();
            writer.Flush();
            return (int)(rawWriter.Position - startPosn);
        }

        private int writePostScript(int footerLength, int metadataLength)
        {
            OrcProto.PostScript.Builder builder = OrcProto.PostScript.CreateBuilder();
            builder.Compression = writeCompressionKind(compress);
            builder.FooterLength = (ulong)footerLength;
            builder.MetadataLength = (ulong)metadataLength;
            builder.Magic = OrcFile.MAGIC;
            builder.AddVersion((uint)OrcFile.VersionHelper.getMajor(version));
            builder.AddVersion((uint)OrcFile.VersionHelper.getMinor(version));
            builder.WriterVersion = (uint)(int)OrcFile.WriterVersion.HIVE_4243;
            if (compress != CompressionKind.NONE)
            {
                builder.CompressionBlockSize = (ulong)bufferSize;
            }
            OrcProto.PostScript ps = builder.Build();
            // need to write this uncompressed
            long startPosn = rawWriter.Position;
            ps.WriteTo(rawWriter);
            long length = rawWriter.Position - startPosn;
            if (length > 255)
            {
                throw new ArgumentException("PostScript too large at " + length);
            }
            return (int)length;
        }

        private long estimateStripeSize()
        {
            long result = 0;
            foreach (BufferedStream stream in streams.Values)
            {
                result += stream.getBufferSize();
            }
            result += treeWriter.estimateMemory();
            return result;
        }

        public TypeDescription getSchema()
        {
            return schema;
        }

        public void addUserMetadata(string name, ByteBuffer value)
        {
            userMetadata[name] = ByteString.CopyFrom(value.contents());
        }

        public void addRow(object row)
        {
            treeWriter.write(row);
            rowsInStripe += 1;
            if (buildIndex)
            {
                rowsInIndex += 1;

                if (rowsInIndex >= rowIndexStride)
                {
                    createRowIndexEntry();
                }
            }
            memoryManager.addedRow(1);
        }

        void IDisposable.Dispose()
        {
            close();
        }

        public void close()
        {
            if (baseStream == null)
            {
                return;
            }
            if (callback != null)
            {
                callback.preFooterWrite(this);
            }
            // remove us from the memory manager so that we don't get any callbacks
            memoryManager.removeWriter(path);
            // actually close the file
            flushStripe();
            int metadataLength = writeMetadata(rawWriter.Position);
            int footerLength = writeFooter(rawWriter.Position - metadataLength);
            rawWriter.WriteByte((byte)writePostScript(footerLength, metadataLength));
            // rawWriter.Close();
            baseStream = null;
        }

        /**
         * Raw data size will be compute when writing the file footer. Hence raw data
         * size value will be available only after closing the writer.
         */
        public long getRawDataSize()
        {
            return rawDataSize;
        }

        /**
         * Row count gets updated when flushing the stripes. To get accurate row
         * count call this method after writer is closed.
         */
        public long getNumberOfRows()
        {
            return rowCount;
        }

        public long writeIntermediateFooter()
        {
            // flush any buffered rows
            flushStripe();
            // write a footer
            if (stripesAtLastFlush != stripes.Count)
            {
                if (callback != null)
                {
                    callback.preFooterWrite(this);
                }
                int metaLength = writeMetadata(rawWriter.Position);
                int footLength = writeFooter(rawWriter.Position - metaLength);
                rawWriter.WriteByte((byte)writePostScript(footLength, metaLength));
                stripesAtLastFlush = stripes.Count;
                rawWriter.Flush();
            }
            return rawWriter.Position;
        }

        public void appendStripe(byte[] stripe, int offset, int length,
            StripeInformation stripeInfo,
            OrcProto.StripeStatistics stripeStatistics)
        {
            checkArgument(stripe != null, "Stripe must not be null");
            checkArgument(length <= stripe.Length,
                "Specified length must not be greater specified array length");
            checkArgument(stripeInfo != null, "Stripe information must not be null");
            checkArgument(stripeStatistics != null,
                "Stripe statistics must not be null");

            getStream();
            long start = rawWriter.Position;
            long availBlockSpace = blockSize - (start % blockSize);

            // see if stripe can fit in the current hdfs block, else pad the remaining
            // space in the block
            if (length < blockSize && length > availBlockSpace &&
                addBlockPadding)
            {
                byte[] pad = new byte[(int)Math.Min(HDFS_BUFFER_SIZE, availBlockSpace)];
                LOG.info(String.Format("Padding ORC by {0} bytes while merging..",
                    availBlockSpace));
                start += availBlockSpace;
                while (availBlockSpace > 0)
                {
                    int writeLen = (int)Math.Min(availBlockSpace, pad.Length);
                    rawWriter.Write(pad, 0, writeLen);
                    availBlockSpace -= writeLen;
                }
            }

            rawWriter.Write(stripe, 0, stripe.Length);
            rowsInStripe = (long)stripeStatistics.ColStatsList[0].NumberOfValues;
            rowCount += rowsInStripe;

            // since we have already written the stripe, just update stripe statistics
            treeWriter.stripeStatsBuilders.Add(stripeStatistics.ToBuilder());

            // update file level statistics
            updateFileStatistics(stripeStatistics);

            // update stripe information
            OrcProto.StripeInformation.Builder dirEntry = OrcProto.StripeInformation.CreateBuilder();
            dirEntry.Offset = (ulong)start;
            dirEntry.NumberOfRows = (ulong)rowsInStripe;
            dirEntry.IndexLength = (ulong)stripeInfo.getIndexLength();
            dirEntry.DataLength = (ulong)stripeInfo.getDataLength();
            dirEntry.FooterLength = (ulong)stripeInfo.getFooterLength();
            stripes.Add(dirEntry.Build());

            // reset it after writing the stripe
            rowsInStripe = 0;
        }

        static void checkArgument(bool condition, string message)
        {
            if (!condition)
            {
                throw new ArgumentException(message);
            }
        }

        private void updateFileStatistics(OrcProto.StripeStatistics stripeStatistics)
        {
            IList<OrcProto.ColumnStatistics> cs = stripeStatistics.ColStatsList;
            List<TreeWriter> allWriters = getAllColumnTreeWriters(treeWriter);
            for (int i = 0; i < allWriters.Count; i++)
            {
                allWriters[i].fileStatistics.merge(ColumnStatisticsImpl.deserialize(cs[i]));
            }
        }

        private List<TreeWriter> getAllColumnTreeWriters(TreeWriter rootTreeWriter)
        {
            List<TreeWriter> result = new List<TreeWriter>();
            getAllColumnTreeWritersImpl(rootTreeWriter, result);
            return result;
        }

        private void getAllColumnTreeWritersImpl(TreeWriter tw, List<TreeWriter> result)
        {
            result.Add(tw);
            foreach (TreeWriter child in tw.childrenWriters)
            {
                getAllColumnTreeWritersImpl(child, result);
            }
        }

        public void appendUserMetadata(List<OrcProto.UserMetadataItem> userMetadata)
        {
            if (userMetadata != null)
            {
                foreach (OrcProto.UserMetadataItem item in userMetadata)
                {
                    this.userMetadata[item.Name] = item.Value;
                }
            }
        }

        static OrcProto.ColumnEncoding MakeEncoding(
            OrcProto.ColumnEncoding.Types.Kind kind,
            int? dictionarySize = null)
        {
            OrcProto.ColumnEncoding.Builder builder = OrcProto.ColumnEncoding.CreateBuilder();
            builder.Kind = kind;
            if (dictionarySize != null)
            {
                builder.DictionarySize = (uint)dictionarySize.Value;
            }
            return builder.Build();
        }
    }
}
