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
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using Google.ProtocolBuffers;
    using OrcSharp.External;
    using OrcSharp.Query;
    using OrcSharp.Serialization;
    using OrcSharp.Types;
    using OrcProto = global::orc.proto;

    public class ReaderImpl : Reader
    {
        private static readonly Logger LOG = LoggerFactory.getLog(typeof(ReaderImpl));

        private const int DIRECTORY_SIZE_GUESS = 16 * 1024;

        protected Func<Stream> streamCreator;
        protected string path;
        protected CompressionKind compressionKind;
        protected CompressionCodec codec;
        protected int bufferSize;
        private IList<OrcProto.StripeStatistics> stripeStats;
        private int metadataSize;
        protected IList<OrcProto.Type> types;
        private IList<OrcProto.UserMetadataItem> userMetadata;
        private IList<OrcProto.ColumnStatistics> fileStats;
        private IList<StripeInformation> stripes;
        protected int rowIndexStride;
        private long contentLength, numberOfRows;

        private ObjectInspector inspector;
        private long deserializedSize = -1;
        protected Configuration conf;
        private IList<int> versionList;
        private OrcFile.WriterVersion writerVersion;

        //serialized footer - Keeping this around for use by getFileMetaInfo()
        // will help avoid cpu cycles spend in deserializing at cost of increased
        // memory footprint.
        private ByteBuffer footerByteBuffer;
        // Same for metastore cache - maintains the same background buffer, but includes postscript.
        // This will only be set if the file footer/metadata was read from disk.
        private ByteBuffer footerMetaAndPsBuffer;

        public class StripeInformationImpl : StripeInformation
        {
            private OrcProto.StripeInformation stripe;

            public StripeInformationImpl(OrcProto.StripeInformation stripe)
            {
                this.stripe = stripe;
            }

            public long getOffset()
            {
                return (long)stripe.Offset;
            }

            public long getLength()
            {
                return (long)stripe.DataLength + getIndexLength() + getFooterLength();
            }

            public long getDataLength()
            {
                return (long)stripe.DataLength;
            }

            public long getFooterLength()
            {
                return (long)stripe.FooterLength;
            }

            public long getIndexLength()
            {
                return (long)stripe.IndexLength;
            }

            public long getNumberOfRows()
            {
                return (long)stripe.NumberOfRows;
            }

            public override string ToString()
            {
                return "offset: " + getOffset() + " data: " + getDataLength() +
                  " rows: " + getNumberOfRows() + " tail: " + getFooterLength() +
                  " index: " + getIndexLength();
            }
        }

        public long getNumberOfRows()
        {
            return numberOfRows;
        }

        public List<string> getMetadataKeys()
        {
            List<string> result = new List<string>();
            foreach (OrcProto.UserMetadataItem item in userMetadata)
            {
                result.Add(item.Name);
            }
            return result;
        }

        public ByteBuffer getMetadataValue(string key)
        {
            foreach (OrcProto.UserMetadataItem item in userMetadata)
            {
                if (item.HasName && item.Name.Equals(key))
                {
                    return item.Value.asReadOnlyByteBuffer();
                }
            }
            throw new ArgumentException("Can't find user metadata " + key);
        }

        public bool hasMetadataValue(string key)
        {
            foreach (OrcProto.UserMetadataItem item in userMetadata)
            {
                if (item.HasName && item.Name.Equals(key))
                {
                    return true;
                }
            }
            return false;
        }

        public CompressionKind getCompression()
        {
            return compressionKind;
        }

        public int getCompressionSize()
        {
            return bufferSize;
        }

        public IList<StripeInformation> getStripes()
        {
            return stripes;
        }

        public ObjectInspector getObjectInspector()
        {
            return inspector;
        }

        public long getContentLength()
        {
            return contentLength;
        }

        public IList<OrcProto.Type> getTypes()
        {
            return types;
        }

        public OrcFile.Version getFileVersion()
        {
            foreach (OrcFile.Version version in Enum.GetValues(typeof(OrcFile.Version)))
            {
                if (OrcFile.VersionHelper.getMajor(version) == versionList[0] &&
                    OrcFile.VersionHelper.getMinor(version) == versionList[1])
                {
                    return version;
                }
            }
            return OrcFile.Version.V_0_11;
        }

        public OrcFile.WriterVersion getWriterVersion()
        {
            return writerVersion;
        }

        public int getRowIndexStride()
        {
            return rowIndexStride;
        }

        public ColumnStatistics[] getStatistics()
        {
            ColumnStatistics[] result = new ColumnStatistics[types.Count];
            for (int i = 0; i < result.Length; ++i)
            {
                result[i] = ColumnStatisticsImpl.deserialize(fileStats[i]);
            }
            return result;
        }

        /**
         * Ensure this is an ORC file to prevent users from trying to read text
         * files or RC files as ORC files.
         * @param in the file being read
         * @param path the filename for error messages
         * @param psLen the postscript length
         * @param buffer the tail of the file
         * @
         */
        static void ensureOrcFooter(Stream @in,
                                            string path,
                                            int psLen,
                                            ByteBuffer buffer)
        {
            int len = OrcFile.MAGIC.Length;
            if (psLen < len + 1)
            {
                throw new FormatException("Malformed ORC file " + path +
                    ". Invalid postscript length " + psLen);
            }
            int offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - 1 - len;
            byte[] array = buffer.array();
            // now look for the magic string at the end of the postscript.
            if (!Lists.AreEqual(array, offset, OrcFile.MAGIC, len))
            {
                // If it isn't there, this may be the 0.11.0 version of ORC.
                // Read the first 3 bytes of the file to check for the header
                byte[] header = new byte[len];
                @in.readFully(0, header, 0, len);
                // if it isn't there, this isn't an ORC file
                if (!Lists.AreEqual(header, 0, OrcFile.MAGIC, len))
                {
                    throw new FormatException("Malformed ORC file " + path +
                        ". Invalid postscript.");
                }
            }
        }

        /**
         * Build a version string out of an array.
         * @param version the version number as a list
         * @return the human readable form of the version string
         */
        private static string versionString(IList<int> version)
        {
            StringBuilder buffer = new StringBuilder();
            for (int i = 0; i < version.Count; ++i)
            {
                if (i != 0)
                {
                    buffer.Append('.');
                }
                buffer.Append(version[i]);
            }
            return buffer.ToString();
        }

        /**
         * Check to see if this ORC file is from a future version and if so,
         * warn the user that we may not be able to read all of the column encodings.
         * @param log the logger to write any error message to
         * @param path the data source path for error messages
         * @param version the version of hive that wrote the file.
         */
        static void checkOrcVersion(Logger log, string path, IList<int> version)
        {
            if (version.Count >= 1)
            {
                int major = version[0];
                int minor = 0;
                if (version.Count >= 2)
                {
                    minor = version[1];
                }
                if (major > OrcFile.VersionHelper.CURRENT_Major ||
                    (major == OrcFile.VersionHelper.CURRENT_Major &&
                     minor > OrcFile.VersionHelper.CURRENT_Minor))
                {
                    log.warn(path + " was written by a future ORC version " +
                             versionString(version) +
                             ". This file may not be readable by this version of ORC.");
                }
            }
        }

        /**
        * Constructor that let's the user specify additional options.
         * @param path pathname for file
         * @param options options for reading
         * @
         */
        public ReaderImpl(Func<Stream> streamCreator, string path, OrcFile.ReaderOptions options)
        {
            this.streamCreator = streamCreator;
            this.path = path;
            this.conf = options.getConfiguration();

            FileMetadata fileMetadata = options.getFileMetadata();
            if (fileMetadata != null)
            {
                this.compressionKind = fileMetadata.getCompressionKind();
                this.bufferSize = fileMetadata.getCompressionBufferSize();
                this.codec = WriterImpl.createCodec(compressionKind);
                this.metadataSize = fileMetadata.getMetadataSize();
                this.stripeStats = fileMetadata.getStripeStats();
                this.versionList = fileMetadata.getVersionList();
                this.writerVersion = OrcFile.WriterVersionHelpers.from(fileMetadata.getWriterVersionNum());
                this.types = fileMetadata.getTypes();
                this.rowIndexStride = fileMetadata.getRowIndexStride();
                this.contentLength = fileMetadata.getContentLength();
                this.numberOfRows = fileMetadata.getNumberOfRows();
                this.fileStats = fileMetadata.getFileStats();
                this.stripes = fileMetadata.getStripes();
                this.inspector = OrcStruct.createObjectInspector(0, fileMetadata.getTypes());
                this.footerByteBuffer = null; // not cached and not needed here
                this.userMetadata = null; // not cached and not needed here
                this.footerMetaAndPsBuffer = null;
            }
            else
            {
                FileMetaInfo footerMetaData;
                if (options.getFileMetaInfo() != null)
                {
                    footerMetaData = options.getFileMetaInfo();
                    this.footerMetaAndPsBuffer = null;
                }
                else
                {
                    using (Stream file = streamCreator())
                    {
                        footerMetaData = extractMetaInfoFromFooter(file, path, options.getMaxLength());
                        this.footerMetaAndPsBuffer = footerMetaData.footerMetaAndPsBuffer;
                    }
                }
                MetaInfoObjExtractor rInfo =
                    new MetaInfoObjExtractor(footerMetaData.compressionKind,
                                             footerMetaData.bufferSize,
                                             footerMetaData.metadataSize,
                                             footerMetaData.footerBuffer
                                             );
                this.footerByteBuffer = footerMetaData.footerBuffer;
                this.compressionKind = rInfo.compressionKind;
                this.codec = rInfo.codec;
                this.bufferSize = rInfo.bufferSize;
                this.metadataSize = rInfo.metadataSize;
                this.stripeStats = rInfo.metadata.StripeStatsList;
                this.types = rInfo.footer.TypesList;
                this.rowIndexStride = (int)rInfo.footer.RowIndexStride;
                this.contentLength = (int)rInfo.footer.ContentLength;
                this.numberOfRows = (int)rInfo.footer.NumberOfRows;
                this.userMetadata = rInfo.footer.MetadataList;
                this.fileStats = rInfo.footer.StatisticsList;
                this.inspector = rInfo.inspector;
                this.versionList = footerMetaData.versionList.Select(v => (int)v).ToList();
                this.writerVersion = footerMetaData.writerVersion;
                this.stripes = convertProtoStripesToStripes(rInfo.footer.StripesList);
            }
        }
        /**
         * Get the WriterVersion based on the ORC file postscript.
         * @param writerVersion the integer writer version
         * @return
         */
        static OrcFile.WriterVersion getWriterVersion(int writerVersion)
        {
            foreach (OrcFile.WriterVersion version in Enum.GetValues(typeof(OrcFile.WriterVersion)))
            {
                if ((int)version == writerVersion)
                {
                    return version;
                }
            }
            return OrcFile.WriterVersion.FUTURE;
        }

        /** Extracts the necessary metadata from an externally store buffer (fullFooterBuffer). */
        public static FooterInfo extractMetaInfoFromFooter(ByteBuffer bb, string srcPath)
        {
            // Read the PostScript. Be very careful as some parts of this historically use bb position
            // and some use absolute offsets that have to take position into account.
            int baseOffset = bb.position();
            int lastByteAbsPos = baseOffset + bb.remaining() - 1;
            int psLen = bb.get(lastByteAbsPos) & 0xff;
            int psAbsPos = lastByteAbsPos - psLen;
            OrcProto.PostScript ps = extractPostScript(bb, srcPath, psLen, psAbsPos);
            Debug.Assert(baseOffset == bb.position());

            // Extract PS information.
            int footerSize = (int)ps.FooterLength, metadataSize = (int)ps.MetadataLength,
                footerAbsPos = psAbsPos - footerSize, metadataAbsPos = footerAbsPos - metadataSize;
            CompressionKind compressionKind = (CompressionKind)Enum.Parse(typeof(CompressionKind), ps.Compression.ToString(), true);
            CompressionCodec codec = WriterImpl.createCodec(compressionKind);
            int bufferSize = (int)ps.CompressionBlockSize;
            bb.position(metadataAbsPos);
            bb.mark();

            // Extract metadata and footer.
            OrcProto.Metadata metadata = extractMetadata(
                bb, metadataAbsPos, metadataSize, codec, bufferSize);
            List<StripeStatistics> stats = new List<StripeStatistics>(metadata.StripeStatsCount);
            foreach (OrcProto.StripeStatistics ss in metadata.StripeStatsList)
            {
                stats.Add(new StripeStatistics(ss.ColStatsList));
            }
            OrcProto.Footer footer = extractFooter(bb, footerAbsPos, footerSize, codec, bufferSize);
            bb.position(metadataAbsPos);
            bb.limit(psAbsPos);
            // TODO: do we need footer buffer here? FileInfo/FileMetaInfo is a mess...
            FileMetaInfo fmi = new FileMetaInfo(
                compressionKind, bufferSize, metadataSize, bb, extractWriterVersion(ps));
            return new FooterInfo(stats, footer, fmi);
        }

        private static OrcProto.Footer extractFooter(ByteBuffer bb, int footerAbsPos,
            int footerSize, CompressionCodec codec, int bufferSize)
        {
            bb.position(footerAbsPos);
            bb.limit(footerAbsPos + footerSize);
            return OrcProto.Footer.ParseFrom(InStream.createCodedInputStream(null, "footer",
                new List<DiskRange> { new RecordReaderImpl.BufferChunk(bb, 0) }, footerSize, codec, bufferSize));
        }

        private static OrcProto.Metadata extractMetadata(ByteBuffer bb, int metadataAbsPos,
            int metadataSize, CompressionCodec codec, int bufferSize)
        {
            bb.position(metadataAbsPos);
            bb.limit(metadataAbsPos + metadataSize);
            return OrcProto.Metadata.ParseFrom(InStream.createCodedInputStream(null, "metadata",
                new List<DiskRange> { new RecordReaderImpl.BufferChunk(bb, 0) }, metadataSize, codec, bufferSize));
        }

        private static OrcProto.PostScript extractPostScript(ByteBuffer bb, string path,
            int psLen, int psAbsOffset)
        {
            // TODO: when PB is upgraded to 2.6, newInstance(ByteBuffer) method should be used here.
            Debug.Assert(bb.hasArray());
            CodedInputStream @in = CodedInputStream.CreateInstance(
                bb.array(), bb.arrayOffset() + psAbsOffset, psLen);
            OrcProto.PostScript ps = OrcProto.PostScript.ParseFrom(@in);
            checkOrcVersion(LOG, path, ps.VersionList.flip());

            // Check compression codec.
            switch (ps.Compression)
            {
                case OrcProto.CompressionKind.NONE:
                case OrcProto.CompressionKind.ZLIB:
                case OrcProto.CompressionKind.SNAPPY:
                case OrcProto.CompressionKind.LZO:
                    break;
                default:
                    throw new ArgumentException("Unknown compression");
            }
            return ps;
        }

        private static FileMetaInfo extractMetaInfoFromFooter(Stream file, string path, long maxFileLength)
        {
            // figure out the size of the file using the option or filesystem
            long size;
            if (maxFileLength == Int64.MaxValue)
            {
                // size = fs.getFileStatus(path).getLen();
                size = file.Length;
            }
            else
            {
                size = maxFileLength;
            }

            //read last bytes into buffer to get PostScript
            int readSize = (int)Math.Min(size, DIRECTORY_SIZE_GUESS);
            ByteBuffer buffer = ByteBuffer.allocate(readSize);
            Debug.Assert(buffer.position() == 0);
            file.readFully((size - readSize),
                buffer.array(), buffer.arrayOffset(), readSize);
            buffer.position(0);

            //read the PostScript
            //get length of PostScript
            int psLen = buffer.get(readSize - 1) & 0xff;
            ensureOrcFooter(file, path, psLen, buffer);
            int psOffset = readSize - 1 - psLen;
            OrcProto.PostScript ps = extractPostScript(buffer, path, psLen, psOffset);

            int footerSize = (int)ps.FooterLength;
            int metadataSize = (int)ps.MetadataLength;
            OrcFile.WriterVersion writerVersion = extractWriterVersion(ps);

            //check if extra bytes need to be read
            ByteBuffer fullFooterBuffer = null;
            int extra = Math.Max(0, psLen + 1 + footerSize + metadataSize - readSize);
            if (extra > 0)
            {
                //more bytes need to be read, seek back to the right place and read extra bytes
                ByteBuffer extraBuf = ByteBuffer.allocate(extra + readSize);
                file.readFully((size - readSize - extra), extraBuf.array(),
                    extraBuf.arrayOffset() + extraBuf.position(), extra);
                extraBuf.position(extra);
                //append with already read bytes
                extraBuf.put(buffer);
                buffer = extraBuf;
                buffer.position(0);
                fullFooterBuffer = buffer.slice();
                buffer.limit(footerSize + metadataSize);
            }
            else
            {
                //footer is already in the bytes in buffer, just adjust position, length
                buffer.position(psOffset - footerSize - metadataSize);
                fullFooterBuffer = buffer.slice();
                buffer.limit(psOffset);
            }

            // remember position for later
            buffer.mark();

            CompressionKind compressionKind = (CompressionKind)Enum.Parse(
                typeof(CompressionKind), ps.Compression.ToString(), true);
            return new FileMetaInfo(
                compressionKind,
                (int)ps.CompressionBlockSize,
                (int)ps.MetadataLength,
                buffer,
                ps.VersionList,
                writerVersion,
                fullFooterBuffer);
        }

        private static OrcFile.WriterVersion extractWriterVersion(OrcProto.PostScript ps)
        {
            return (ps.HasWriterVersion
                ? getWriterVersion((int)ps.WriterVersion) : OrcFile.WriterVersion.ORIGINAL);
        }

        private static List<StripeInformation> convertProtoStripesToStripes(
            IList<OrcProto.StripeInformation> stripes)
        {
            List<StripeInformation> result = new List<StripeInformation>(stripes.Count);
            foreach (OrcProto.StripeInformation info in stripes)
            {
                result.Add(new StripeInformationImpl(info));
            }
            return result;
        }

        /**
         * MetaInfoObjExtractor - has logic to create the values for the fields in ReaderImpl
         *  from serialized fields.
         * As the fields are final, the fields need to be initialized in the constructor and
         *  can't be done in some helper function. So this helper class is used instead.
         *
         */
        private class MetaInfoObjExtractor
        {
            internal CompressionKind compressionKind;
            internal CompressionCodec codec;
            internal int bufferSize;
            internal int metadataSize;
            internal OrcProto.Metadata metadata;
            internal OrcProto.Footer footer;
            internal ObjectInspector inspector;

            public MetaInfoObjExtractor(CompressionKind compressionKind, int bufferSize, int metadataSize,
                ByteBuffer footerBuffer)
            {
                this.compressionKind = compressionKind;
                this.bufferSize = bufferSize;
                this.codec = WriterImpl.createCodec(compressionKind);
                this.metadataSize = metadataSize;

                int position = footerBuffer.position();
                int footerBufferSize = footerBuffer.limit() - footerBuffer.position() - metadataSize;

                this.metadata = extractMetadata(footerBuffer, position, metadataSize, codec, bufferSize);
                this.footer = extractFooter(
                    footerBuffer, position + metadataSize, footerBufferSize, codec, bufferSize);

                footerBuffer.position(position);
                this.inspector = OrcStruct.createObjectInspector(0, footer.TypesList);
            }
        }

        public FileMetaInfo getFileMetaInfo()
        {
            return new FileMetaInfo(compressionKind, bufferSize,
                metadataSize, footerByteBuffer, versionList.flip(), writerVersion, footerMetaAndPsBuffer);
        }

        /** Same as FileMetaInfo, but with extra fields. FileMetaInfo is serialized for splits
         * and so we don't just add fields to it, it's already messy and confusing. */
        public class FooterInfo
        {
            private OrcProto.Footer footer;
            private List<StripeStatistics> metadata;
            private List<StripeInformation> stripes;
            private FileMetaInfo fileMetaInfo;

            internal FooterInfo(
                List<StripeStatistics> metadata, OrcProto.Footer footer, FileMetaInfo fileMetaInfo)
            {
                this.metadata = metadata;
                this.footer = footer;
                this.fileMetaInfo = fileMetaInfo;
                this.stripes = convertProtoStripesToStripes(footer.StripesList);
            }

            public OrcProto.Footer getFooter()
            {
                return footer;
            }

            public List<StripeStatistics> getMetadata()
            {
                return metadata;
            }

            public FileMetaInfo getFileMetaInfo()
            {
                return fileMetaInfo;
            }

            public List<StripeInformation> getStripes()
            {
                return stripes;
            }
        }

        public ByteBuffer getSerializedFileFooter()
        {
            return footerMetaAndPsBuffer;
        }

        public RecordReader rows()
        {
            return rowsOptions(new RecordReaderOptions());
        }

        public RecordReader rowsOptions(RecordReaderOptions options)
        {
            LOG.info("Reading ORC rows from " + path + " with " + options);
            bool[] include = options.getInclude();
            // if included columns is null, then include all columns
            if (include == null)
            {
                include = new bool[types.Count];
                Arrays.fill(include, true);
                options.include(include);
            }
            return new RecordReaderImpl(this.getStripes(), streamCreator, path,
                options, types, codec, bufferSize, rowIndexStride, conf);
        }


        public RecordReader rows(bool[] include)
        {
            return rowsOptions(new RecordReaderOptions().include(include));
        }

        public RecordReader rows(long offset, long length, bool[] include)
        {
            return rowsOptions(new RecordReaderOptions().include(include).range(offset, length));
        }

        public RecordReader rows(long offset, long length, bool[] include,
                                 SearchArgument sarg, String[] columnNames)
        {
            return rowsOptions(new RecordReaderOptions().include(include).range(offset, length)
                .searchArgument(sarg, columnNames));
        }

        public long getRawDataSize()
        {
            // if the deserializedSize is not computed, then compute it, else
            // return the already computed size. since we are reading from the footer
            // we don't have to compute deserialized size repeatedly
            if (deserializedSize == -1)
            {
                List<int> indices = new List<int>(fileStats.Count);
                for (int i = 0; i < fileStats.Count; ++i)
                {
                    indices.Add(i);
                }
                deserializedSize = getRawDataSizeFromColIndices(indices);
            }
            return deserializedSize;
        }

        public long getRawDataSizeFromColIndices(List<int> colIndices)
        {
            return getRawDataSizeFromColIndices(colIndices, types, fileStats);
        }

        public static long getRawDataSizeFromColIndices(
            List<int> colIndices, IList<OrcProto.Type> types,
            IList<OrcProto.ColumnStatistics> stats)
        {
            long result = 0;
            foreach (int colIdx in colIndices)
            {
                result += getRawDataSizeOfColumn(colIdx, types, stats);
            }
            return result;
        }

        private static long getRawDataSizeOfColumn(int colIdx, IList<OrcProto.Type> types,
            IList<OrcProto.ColumnStatistics> stats)
        {
            OrcProto.ColumnStatistics colStat = stats[colIdx];
            long numVals = (long)colStat.NumberOfValues;
            OrcProto.Type type = types[colIdx];

            switch (type.Kind)
            {
                case OrcProto.Type.Types.Kind.BINARY:
                    // old orc format doesn't support binary statistics. checking for binary
                    // statistics is not required as protocol buffers takes care of it.
                    return colStat.BinaryStatistics.Sum;
                case OrcProto.Type.Types.Kind.STRING:
                case OrcProto.Type.Types.Kind.CHAR:
                case OrcProto.Type.Types.Kind.VARCHAR:
                    // old orc format doesn't support sum for string statistics. checking for
                    // existence is not required as protocol buffers takes care of it.

                    // ORC strings are deserialized to java strings. so use java data model's
                    // string size
                    numVals = numVals == 0 ? 1 : numVals;
                    int avgStrLen = (int)(colStat.StringStatistics.Sum / numVals);
                    return numVals * JavaDataModel.lengthForStringOfLength(avgStrLen);
                case OrcProto.Type.Types.Kind.TIMESTAMP:
                    return numVals * JavaDataModel.lengthOfTimestamp();
                case OrcProto.Type.Types.Kind.DATE:
                    return numVals * JavaDataModel.lengthOfDate();
                case OrcProto.Type.Types.Kind.DECIMAL:
                    return numVals * JavaDataModel.lengthOfDecimal();
                case OrcProto.Type.Types.Kind.DOUBLE:
                case OrcProto.Type.Types.Kind.LONG:
                    return numVals * JavaDataModel.Eight;
                case OrcProto.Type.Types.Kind.FLOAT:
                case OrcProto.Type.Types.Kind.INT:
                case OrcProto.Type.Types.Kind.SHORT:
                case OrcProto.Type.Types.Kind.BOOLEAN:
                case OrcProto.Type.Types.Kind.BYTE:
                    return numVals * JavaDataModel.Four;
                default:
                    LOG.debug("Unknown primitive category: " + type.Kind);
                    break;
            }

            return 0;
        }

        public long getRawDataSizeOfColumns(List<string> colNames)
        {
            List<int> colIndices = getColumnIndicesFromNames(colNames);
            return getRawDataSizeFromColIndices(colIndices);
        }

        private List<int> getColumnIndicesFromNames(List<string> colNames)
        {
            // top level struct
            OrcProto.Type type = types[0];
            List<int> colIndices = new List<int>();
            IList<string> fieldNames = type.FieldNamesList;
            int fieldIdx = 0;
            foreach (string colName in colNames)
            {
                fieldIdx = fieldNames.IndexOf(colName);
                if (fieldIdx < 0)
                {
                    string s = "Cannot find field for: " + colName + " in ";
                    foreach (string fn in fieldNames)
                    {
                        s += fn + ", ";
                    }
                    LOG.warn(s);
                    continue;
                }

                // a single field may span multiple columns. find start and end column
                // index for the requested field
                int idxStart = (int)type.SubtypesList[fieldIdx];

                int idxEnd;

                // if the specified is the last field and then end index will be last
                // column index
                if (fieldIdx + 1 > fieldNames.Count - 1)
                {
                    idxEnd = getLastIdx() + 1;
                }
                else
                {
                    idxEnd = (int)type.SubtypesList[fieldIdx + 1];
                }

                // if start index and end index are same then the field is a primitive
                // field else complex field (like map, list, struct, union)
                if (idxStart == idxEnd)
                {
                    // simple field
                    colIndices.Add(idxStart);
                }
                else
                {
                    // complex fields spans multiple columns
                    for (int i = idxStart; i < idxEnd; i++)
                    {
                        colIndices.Add(i);
                    }
                }
            }
            return colIndices;
        }

        private int getLastIdx()
        {
            HashSet<uint> indices = new HashSet<uint>();
            foreach (OrcProto.Type type in types)
            {
                indices.UnionWith(type.SubtypesList);
            }
            return (int)indices.Max();
        }

        public IList<OrcProto.StripeStatistics> getOrcProtoStripeStatistics()
        {
            return stripeStats;
        }

        public IList<OrcProto.ColumnStatistics> getOrcProtoFileStatistics()
        {
            return fileStats;
        }

        public List<StripeStatistics> getStripeStatistics()
        {
            List<StripeStatistics> result = new List<StripeStatistics>(stripeStats.Count);
            foreach (OrcProto.StripeStatistics ss in stripeStats)
            {
                result.Add(new StripeStatistics(ss.ColStatsList));
            }
            return result;
        }

        public IList<OrcProto.UserMetadataItem> getOrcProtoUserMetadata()
        {
            return userMetadata;
        }

        public MetadataReader metadata()
        {
            return new MetadataReaderImpl(streamCreator, codec, bufferSize, types.Count);
        }

        public IList<int> getVersionList()
        {
            return versionList;
        }

        public int getMetadataSize()
        {
            return metadataSize;
        }

        public DataReader createDefaultDataReader(bool useZeroCopy)
        {
            return RecordReaderUtils.createDefaultDataReader(streamCreator, path, useZeroCopy, codec);
        }
    }
}
