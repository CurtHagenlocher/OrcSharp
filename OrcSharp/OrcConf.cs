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
    using System.Globalization;
    using org.apache.hadoop.hive.ql.io.orc.external;

    /**
     * Define the configuration properties that Orc understands.
     */
    public enum OrcConf
    {
        STRIPE_SIZE,
        BLOCK_SIZE,
        ENABLE_INDEXES,
        ROW_INDEX_STRIDE,
        BUFFER_SIZE,
        BLOCK_PADDING,
        COMPRESS,
        WRITE_FORMAT,
        ENCODING_STRATEGY,
        COMPRESSION_STRATEGY,
        BLOCK_PADDING_TOLERANCE,
        BLOOM_FILTER_FPP,
        USE_ZEROCOPY,
        SKIP_CORRUPT_DATA,
        MEMORY_POOL,
        DICTIONARY_KEY_SIZE_THRESHOLD,
        ROW_INDEX_STRIDE_DICTIONARY_CHECK,
        BLOOM_FILTER_COLUMNS,
    }

    class OrcConfDetails
    {
        readonly internal OrcConf value;
        readonly internal string attribute;
        readonly internal string hiveConfName;
        readonly internal object defaultValue;
        readonly internal string description;

        OrcConfDetails(OrcConf value, string attribute,
                string hiveConfName,
                object defaultValue,
                string description)
        {
            this.value = value;
            this.attribute = attribute;
            this.hiveConfName = hiveConfName;
            this.defaultValue = defaultValue;
            this.description = description;
        }

        internal static readonly OrcConfDetails[] details = new OrcConfDetails[] {
            new OrcConfDetails(OrcConf.STRIPE_SIZE, "orc.stripe.size", "hive.exec.orc.default.stripe.size",
                64L * 1024 * 1024,
                "Define the default ORC stripe size, in bytes." ),
            new OrcConfDetails(OrcConf.BLOCK_SIZE, "orc.block.size", "hive.exec.orc.default.block.size",
                256L * 1024 * 1024,
                "Define the default file system block size for ORC files."),
            new OrcConfDetails(OrcConf.ENABLE_INDEXES, "orc.create.index", "orc.create.index", true,
                "Should the ORC writer create indexes as part of the file."),
            new OrcConfDetails(OrcConf.ROW_INDEX_STRIDE, "orc.row.index.stride",
                "hive.exec.orc.default.row.index.stride", 10000,
                "Define the default ORC index stride in number of rows. (Stride is the\n"+
                " number of rows n index entry represents.)"),
            new OrcConfDetails(OrcConf.BUFFER_SIZE, "orc.compress.size", "hive.exec.orc.default.buffer.size",
                  256 * 1024, "Define the default ORC buffer size, in bytes."),
            new OrcConfDetails(OrcConf.BLOCK_PADDING, "orc.block.padding", "hive.exec.orc.default.block.padding",
                  true,
                  "Define whether stripes should be padded to the HDFS block boundaries."),
            new OrcConfDetails(OrcConf.COMPRESS, "orc.compress", "hive.exec.orc.default.compress", "ZLIB",
                  "Define the default compression codec for ORC file"),
            new OrcConfDetails(OrcConf.WRITE_FORMAT, "orc.write.format", "hive.exec.orc.write.format", "0.12",
                  "Define the version of the file to write. Possible values are 0.11 and\n"+
                      " 0.12. If this parameter is not defined, ORC will use the run\n" +
                      " length encoding (RLE) introduced in Hive 0.12."),
            new OrcConfDetails(OrcConf.ENCODING_STRATEGY, "orc.encoding.strategy", "hive.exec.orc.encoding.strategy",
                  "SPEED",
                  "Define the encoding strategy to use while writing data. Changing this\n"+
                      "will only affect the light weight encoding for integers. This\n" +
                      "flag will not change the compression level of higher level\n" +
                      "compression codec (like ZLIB)."),
            new OrcConfDetails(OrcConf.COMPRESSION_STRATEGY, "orc.compression.strategy",
                  "hive.exec.orc.compression.strategy", "SPEED",
                  "Define the compression strategy to use while writing data.\n" +
                      "This changes the compression level of higher level compression\n" +
                      "codec (like ZLIB)."),
            new OrcConfDetails(OrcConf.BLOCK_PADDING_TOLERANCE, "orc.block.padding.tolerance",
                  "hive.exec.orc.block.padding.tolerance", 0.05,
                  "Define the tolerance for block padding as a decimal fraction of\n" +
                      "stripe size (for example, the default value 0.05 is 5% of the\n" +
                      "stripe size). For the defaults of 64Mb ORC stripe and 256Mb HDFS\n" +
                      "blocks, the default block padding tolerance of 5% will\n" +
                      "reserve a maximum of 3.2Mb for padding within the 256Mb block.\n" +
                      "In that case, if the available size within the block is more than\n"+
                      "3.2Mb, a new smaller stripe will be inserted to fit within that\n" +
                      "space. This will make sure that no stripe written will block\n" +
                      " boundaries and cause remote reads within a node local task."),
            new OrcConfDetails(OrcConf.BLOOM_FILTER_FPP, "orc.bloom.filter.fpp", "orc.default.bloom.fpp", 0.05,
                  "Define the default false positive probability for bloom filters."),
            new OrcConfDetails(OrcConf.USE_ZEROCOPY, "orc.use.zerocopy", "hive.exec.orc.zerocopy", false,
                  "Use zerocopy reads with ORC. (This requires Hadoop 2.3 or later.)"),
            new OrcConfDetails(OrcConf.SKIP_CORRUPT_DATA, "orc.skip.corrupt.data", "hive.exec.orc.skip.corrupt.data",
                  false,
                  "If ORC reader encounters corrupt data, this value will be used to\n" +
                      "determine whether to skip the corrupt data or throw exception.\n" +
                      "The default behavior is to throw exception."),
            new OrcConfDetails( OrcConf.MEMORY_POOL, "orc.memory.pool", "hive.exec.orc.memory.pool", 0.5,
                  "Maximum fraction of heap that can be used by ORC file writers"),
            new OrcConfDetails( OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD, "orc.dictionary.key.threshold",
                  "hive.exec.orc.dictionary.key.size.threshold",
                  0.8,
                  "If the number of distinct keys in a dictionary is greater than this\n" +
                      "fraction of the total number of non-null rows, turn off \n" +
                      "dictionary encoding.  Use 1 to always use dictionary encoding."),
            new OrcConfDetails(OrcConf.ROW_INDEX_STRIDE_DICTIONARY_CHECK, "orc.dictionary.early.check",
                  "hive.orc.row.index.stride.dictionary.check",
                  true,
                  "If enabled dictionary check will happen after first row index stride\n" +
                      "(default 10000 rows) else dictionary check will happen before\n" +
                      "writing first stripe. In both cases, the decision to use\n" +
                      "dictionary or not will be retained thereafter."),
            new OrcConfDetails(OrcConf.BLOOM_FILTER_COLUMNS, "orc.bloom.filter.columns", "orc.bloom.filter.columns",
                  "", "List of columns to create bloom filters for when writing.")
            };

        internal static OrcConfDetails Get(OrcConf orcConf)
        {
            return details[(int)orcConf];
        }

        private string lookupValue(Properties tbl, Configuration conf)
        {
            string result = null;
            if (tbl != null)
            {
                result = tbl.getProperty(attribute);
            }
            if (result == null && conf != null)
            {
                result = conf.get(attribute);
                if (result == null)
                {
                    result = conf.get(hiveConfName);
                }
            }
            return result;
        }

        public long getLong(Properties tbl, Configuration conf)
        {
            string value = lookupValue(tbl, conf);
            if (value != null)
            {
                return Int64.Parse(value);
            }
            return ((IConvertible)defaultValue).ToInt64(CultureInfo.InvariantCulture);
        }

        public string getString(Properties tbl, Configuration conf)
        {
            string value = lookupValue(tbl, conf);
            return value == null ? (String)defaultValue : value;
        }

        public bool getBoolean(Properties tbl, Configuration conf)
        {
            string value = lookupValue(tbl, conf);
            if (value != null)
            {
                return Boolean.Parse(value);
            }
            return (bool)defaultValue;
        }

        public double getDouble(Properties tbl, Configuration conf)
        {
            string value = lookupValue(tbl, conf);
            if (value != null)
            {
                return Double.Parse(value);
            }
            return ((IConvertible)defaultValue).ToDouble(CultureInfo.InvariantCulture);
        }
    }

    public static class OrcConfHelpers
    {
        public static string getAttribute(this OrcConf orcConf)
        {
            return OrcConfDetails.details[(int)orcConf].attribute;
        }

        public static string getHiveConfName(this OrcConf orcConf)
        {
            return OrcConfDetails.details[(int)orcConf].hiveConfName;
        }

        public static object getDefaultValue(this OrcConf orcConf)
        {
            return OrcConfDetails.details[(int)orcConf].defaultValue;
        }

        public static string getDescription(this OrcConf orcConf)
        {
            return OrcConfDetails.details[(int)orcConf].attribute;
        }

        public static long getLong(this OrcConf orcConf, Properties tbl, Configuration conf)
        {
            return OrcConfDetails.details[(int)orcConf].getLong(tbl, conf);
        }

        public static long getLong(this OrcConf orcConf, Configuration conf)
        {
            return OrcConfDetails.details[(int)orcConf].getLong(null, conf);
        }

        public static string getString(this OrcConf orcConf, Properties tbl, Configuration conf)
        {
            return OrcConfDetails.details[(int)orcConf].getString(tbl, conf);
        }

        public static string getString(this OrcConf orcConf, Configuration conf)
        {
            return OrcConfDetails.details[(int)orcConf].getString(null, conf);
        }

        public static bool getBoolean(this OrcConf orcConf, Properties tbl, Configuration conf)
        {
            return OrcConfDetails.details[(int)orcConf].getBoolean(tbl, conf);
        }

        public static bool getBoolean(this OrcConf orcConf, Configuration conf)
        {
            return OrcConfDetails.details[(int)orcConf].getBoolean(null, conf);
        }

        public static double getDouble(this OrcConf orcConf, Properties tbl, Configuration conf)
        {
            return OrcConfDetails.details[(int)orcConf].getDouble(tbl, conf);
        }

        public static double getDouble(this OrcConf orcConf, Configuration conf)
        {
            return OrcConfDetails.details[(int)orcConf].getDouble(null, conf);
        }
    }
}
