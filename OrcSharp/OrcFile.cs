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
    using System.IO;
    using org.apache.hadoop.hive.ql.io.orc.external;

    /// <summary>
    /// Contains factory methods to read or write ORC files.
    /// </summary>
    public static class OrcFile
    {
        public const string MAGIC = "ORC";

        /**
         * Create a version number for the ORC file format, so that we can add
         * non-forward compatible changes in the future. To make it easier for users
         * to understand the version numbers, we use the Hive release number that
         * first wrote that version of ORC files.
         *
         * Thus, if you add new encodings or other non-forward compatible changes
         * to ORC files, which prevent the old reader from reading the new format,
         * you should change these variable to reflect the next Hive release number.
         * Non-forward compatible changes should never be added in patch releases.
         *
         * Do not make any changes that break backwards compatibility, which would
         * prevent the new reader from reading ORC files generated by any released
         * version of Hive.
         */
        public enum Version
        {
            V_0_11, // ("0.11", 0, 11),
            V_0_12, // ("0.12", 0, 12);
        }

        public static class VersionHelper
        {
            public static Version CURRENT = Version.V_0_12;
            public static int CURRENT_Major = getMajor(CURRENT);
            public static int CURRENT_Minor = getMinor(CURRENT);

            public static Version byName(string name)
            {
                switch (name)
                {
                    case "0.11": return Version.V_0_11;
                    case "0.12": return Version.V_0_12;
                    default: throw new ArgumentException("Unknown ORC version " + name);
                }
            }

            public static string getName(Version version)
            {
                switch (version)
                {
                    case Version.V_0_11: return "0.11";
                    case Version.V_0_12: return "0.12";
                    default: throw new ArgumentException("Unknown ORC version " + version);
                }
            }

            /**
             * Get the major version number.
             */
            public static int getMajor(Version version)
            {
                return 0;
            }

            /**
             * Get the minor version number.
             */
            public static int getMinor(Version version)
            {
                switch (version)
                {
                    case Version.V_0_11: return 11;
                    case Version.V_0_12: return 12;
                    default: throw new ArgumentException("Unknown ORC version " + version);
                }
            }
        }

        /**
         * Records the version of the writer in terms of which bugs have been fixed.
         * For bugs in the writer, but the old readers already read the new data
         * correctly, bump this version instead of the Version.
         */
        public enum WriterVersion
        {
            ORIGINAL = 0,
            HIVE_8732 = 1, // corrupted stripe/file maximum column statistics
            HIVE_4243 = 2, // use real column names from Hive tables

            // Don't use any magic numbers here except for the below:
            FUTURE = Int32.MaxValue // a version from a future writer
        }

        public enum EncodingStrategy
        {
            SPEED, COMPRESSION
        }

        public enum CompressionStrategy
        {
            SPEED, COMPRESSION
        }

        /**
         * Create an ORC file reader.
         * @param fs file system
         * @param path file name to read from
         * @return a new ORC file reader.
         * @
         */
        public static Reader createReader(FileSystem fs, string path)
        {
            ReaderOptions opts = new ReaderOptions(new Configuration());
            opts.filesystem(fs);
            return new ReaderImpl(path, opts);
        }

        public class ReaderOptions
        {
            private Configuration conf;
            private FileSystem _filesystem;
            private FileMetaInfo _fileMetaInfo; // TODO: this comes from some place.
            private long _maxLength = Int64.MaxValue;
            private FileMetadata fullFileMetadata; // Propagate from LLAP cache.

            public ReaderOptions(Configuration conf)
            {
                this.conf = conf;
            }
            ReaderOptions fileMetaInfo(FileMetaInfo info)
            {
                _fileMetaInfo = info;
                return this;
            }

            public ReaderOptions filesystem(FileSystem fs)
            {
                _filesystem = fs;
                return this;
            }

            public ReaderOptions maxLength(long val)
            {
                _maxLength = val;
                return this;
            }

            public ReaderOptions fileMetadata(FileMetadata metadata)
            {
                this.fullFileMetadata = metadata;
                return this;
            }

            public Configuration getConfiguration()
            {
                return conf;
            }

            public FileSystem getFilesystem()
            {
                return _filesystem;
            }

            public FileMetaInfo getFileMetaInfo()
            {
                return _fileMetaInfo;
            }

            public long getMaxLength()
            {
                return _maxLength;
            }

            public FileMetadata getFileMetadata()
            {
                return fullFileMetadata;
            }
        }

        public static ReaderOptions readerOptions(Configuration conf)
        {
            return new ReaderOptions(conf);
        }

        public static Reader createReader(string path, ReaderOptions options)
        {
            return new ReaderImpl(path, options);
        }

        public interface WriterCallback
        {
            void preStripeWrite(Writer writer);
            void preFooterWrite(Writer writer);
        }

        /**
         * Options for creating ORC file writers.
         */
        public class WriterOptions
        {
            internal Configuration configuration;
            internal bool explicitSchema = false;
            internal TypeDescription schema = null;
            internal ObjectInspector _inspector = null;
            internal long stripeSizeValue;
            internal long blockSizeValue;
            internal int rowIndexStrideValue;
            internal int bufferSizeValue;
            internal bool blockPaddingValue;
            internal CompressionKind compressValue;
            internal MemoryManager memoryManagerValue;
            internal Version versionValue;
            internal WriterCallback _callback;
            internal EncodingStrategy _encodingStrategy;
            internal CompressionStrategy compressionStrategy;
            internal double _paddingTolerance;
            internal String _bloomFilterColumns;
            internal double _bloomFilterFpp;

            public WriterOptions(Properties tableProperties, Configuration conf)
            {
                configuration = conf;
                memoryManagerValue = getMemoryManager(conf);
                stripeSizeValue = OrcConf.STRIPE_SIZE.getLong(tableProperties, conf);
                blockSizeValue = OrcConf.BLOCK_SIZE.getLong(tableProperties, conf);
                rowIndexStrideValue =
                    (int)OrcConf.ROW_INDEX_STRIDE.getLong(tableProperties, conf);
                bufferSizeValue = (int)OrcConf.BUFFER_SIZE.getLong(tableProperties,
                    conf);
                blockPaddingValue =
                    OrcConf.BLOCK_PADDING.getBoolean(tableProperties, conf);
                compressValue = (CompressionKind)Enum.Parse(
                    typeof(CompressionKind),
                    OrcConf.COMPRESS.getString(tableProperties, conf),
                    true);
                String versionName = OrcConf.WRITE_FORMAT.getString(tableProperties,
                    conf);
                versionValue = VersionHelper.byName(versionName);
                String enString = OrcConf.ENCODING_STRATEGY.getString(tableProperties,
                    conf);
                _encodingStrategy = (EncodingStrategy)Enum.Parse(typeof(EncodingStrategy), enString, true);

                String compString =
                    OrcConf.COMPRESSION_STRATEGY.getString(tableProperties, conf);
                compressionStrategy = (CompressionStrategy)Enum.Parse(typeof(CompressionStrategy), compString, true);

                _paddingTolerance =
                    OrcConf.BLOCK_PADDING_TOLERANCE.getDouble(tableProperties, conf);

                _bloomFilterColumns = OrcConf.BLOOM_FILTER_COLUMNS.getString(tableProperties,
                    conf);
                _bloomFilterFpp = OrcConf.BLOOM_FILTER_FPP.getDouble(tableProperties,
                    conf);
            }

            /**
             * Set the stripe size for the file. The writer stores the contents of the
             * stripe in memory until this memory limit is reached and the stripe
             * is flushed to the HDFS file and the next stripe started.
             */
            public WriterOptions stripeSize(long value)
            {
                stripeSizeValue = value;
                return this;
            }

            /**
             * Set the file system block size for the file. For optimal performance,
             * set the block size to be multiple factors of stripe size.
             */
            public WriterOptions blockSize(long value)
            {
                blockSizeValue = value;
                return this;
            }

            /**
             * Set the distance between entries in the row index. The minimum value is
             * 1000 to prevent the index from overwhelming the data. If the stride is
             * set to 0, no indexes will be included in the file.
             */
            public WriterOptions rowIndexStride(int value)
            {
                rowIndexStrideValue = value;
                return this;
            }

            /**
             * The size of the memory buffers used for compressing and storing the
             * stripe in memory.
             */
            public WriterOptions bufferSize(int value)
            {
                bufferSizeValue = value;
                return this;
            }

            /**
             * Sets whether the HDFS blocks are padded to prevent stripes from
             * straddling blocks. Padding improves locality and thus the speed of
             * reading, but costs space.
             */
            public WriterOptions blockPadding(bool value)
            {
                blockPaddingValue = value;
                return this;
            }

            /**
             * Sets the encoding strategy that is used to encode the data.
             */
            public WriterOptions encodingStrategy(EncodingStrategy strategy)
            {
                _encodingStrategy = strategy;
                return this;
            }

            /**
             * Sets the tolerance for block padding as a percentage of stripe size.
             */
            public WriterOptions paddingTolerance(double value)
            {
                _paddingTolerance = value;
                return this;
            }

            /**
             * Comma separated values of column names for which bloom filter is to be created.
             */
            public WriterOptions bloomFilterColumns(String columns)
            {
                _bloomFilterColumns = columns;
                return this;
            }

            /**
             * Specify the false positive probability for bloom filter.
             * @param fpp - false positive probability
             * @return this
             */
            public WriterOptions bloomFilterFpp(double fpp)
            {
                _bloomFilterFpp = fpp;
                return this;
            }

            /**
             * Sets the generic compression that is used to compress the data.
             */
            public WriterOptions compress(CompressionKind value)
            {
                compressValue = value;
                return this;
            }

            /**
             * A required option that sets the object inspector for the rows. If
             * setSchema is not called, it also defines the schema.
             */
            public WriterOptions inspector(ObjectInspector value)
            {
                _inspector = value;
                if (!explicitSchema)
                {
                    schema = OrcUtils.convertTypeInfo(
                        TypeInfoUtils.getTypeInfoFromObjectInspector(value));
                }
                return this;
            }

            /**
             * Set the schema for the file. This is a required parameter.
             * @param schema the schema for the file.
             * @return this
             */
            public WriterOptions setSchema(TypeDescription schema)
            {
                this.explicitSchema = true;
                this.schema = schema;
                return this;
            }

            /**
             * Sets the version of the file that will be written.
             */
            public WriterOptions version(Version value)
            {
                versionValue = value;
                return this;
            }

            /**
             * Add a listener for when the stripe and file are about to be closed.
             * @param callback the object to be called when the stripe is closed
             * @return this
             */
            public WriterOptions callback(WriterCallback callback)
            {
                _callback = callback;
                return this;
            }

            /**
             * A package local option to set the memory manager.
             */
            WriterOptions memory(MemoryManager value)
            {
                memoryManagerValue = value;
                return this;
            }

            // the assumption is only one ORC writer open at a time, which holds true for
            // most of the cases. HIVE-6455 forces single writer case.
            public long getMemoryAvailableForORC()
            {
                double maxLoad = OrcConf.MEMORY_POOL.getDouble(configuration);
                long maxMemory = 1024*1024*1024; // 1 GB
                long totalMemoryPool = (long)Math.Round(maxMemory * maxLoad);
                return totalMemoryPool;
            }

            public double getDictionaryKeySizeThreshold()
            {
                return OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.getDouble(configuration);
            }

            public bool getStrideDictionaryCheck()
            {
                return OrcConf.ROW_INDEX_STRIDE_DICTIONARY_CHECK.getBoolean(configuration);
            }
        }

        /**
         * Create a set of writer options based on a configuration.
         * @param conf the configuration to use for values
         * @return A WriterOptions object that can be modified
         */
        public static WriterOptions writerOptions(Configuration conf)
        {
            return new WriterOptions(null, conf);
        }

        /**
         * Create a set of write options based on a set of table properties and
         * configuration.
         * @param tableProperties the properties of the table
         * @param conf the configuration of the query
         * @return a WriterOptions object that can be modified
         */
        public static WriterOptions writerOptions(Properties tableProperties, Configuration conf)
        {
            return new WriterOptions(tableProperties, conf);
        }

        /**
         * Create an ORC file writer. This is the public interface for creating
         * writers going forward and new options will only be added to this method.
         * @param path filename to write to
         * @param opts the options
         * @return a new ORC file writer
         * @
         */
        public static Writer createWriter(string path, Stream stream, WriterOptions opts)
        {
            return new WriterImpl(stream, path, opts, opts._inspector,
                                  opts.schema,
                                  opts.stripeSizeValue, opts.compressValue,
                                  opts.bufferSizeValue, opts.rowIndexStrideValue,
                                  opts.memoryManagerValue, opts.blockPaddingValue,
                                  opts.versionValue, opts._callback,
                                  opts._encodingStrategy, opts.compressionStrategy,
                                  opts._paddingTolerance, opts.blockSizeValue,
                                  opts._bloomFilterColumns, opts._bloomFilterFpp);
        }

        private static MemoryManager memoryManager = null;

        private static /* synchronized */ MemoryManager getMemoryManager(Configuration conf)
        {
            if (memoryManager == null)
            {
                memoryManager = new MemoryManager(10000000);
            }
            return memoryManager;
        }
    }
}