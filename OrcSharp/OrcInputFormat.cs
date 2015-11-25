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
    using System.Diagnostics;
    using System.IO;
    using org.apache.hadoop.hive.ql.io.orc.external;
    using org.apache.hadoop.hive.ql.io.orc.query;
    using OrcProto = global::orc.proto;
    using Path = org.apache.hadoop.hive.ql.io.orc.external.Path;

    /**
     * A MapReduce/Hive input format for ORC files.
     * <p>
     * This class : both the classic InputFormat, which stores the rows
     * directly, and AcidInputFormat, which stores a series of events with the
     * following schema:
     * <pre>
     *   class AcidEvent&lt;ROW&gt; {
     *     enum ACTION {INSERT, UPDATE, DELETE}
     *     ACTION operation;
     *     long originalTransaction;
     *     int bucket;
     *     long rowId;
     *     long currentTransaction;
     *     ROW row;
     *   }
     * </pre>
     * Each AcidEvent object corresponds to an update event. The
     * originalTransaction, bucket, and rowId are the unique identifier for the row.
     * The operation and currentTransaction are the operation and the transaction
     * that added this event. Insert and update events include the entire row, while
     * delete events have null for row.
     */
    public class OrcInputFormat
    /* : InputFormat<NullWritable, OrcStruct>,
  InputFormatChecker, VectorizedInputFormatInterface, LlapWrappableInputFormatInterface,
    AcidInputFormat<NullWritable, OrcStruct>, CombineHiveInputFormat.AvoidSplitCombination */
    {

        enum SplitStrategyKind
        {
            HYBRID,
            BI,
            ETL
        }

        private static Log LOG = LogFactory.getLog(typeof(OrcInputFormat));
        private static bool isDebugEnabled = LOG.isDebugEnabled();
        static HadoopShims SHIMS = ShimLoader.getHadoopShims();
        static String MIN_SPLIT_SIZE =
            SHIMS.getHadoopConfNames().get("MAPREDMINSPLITSIZE");
        static String MAX_SPLIT_SIZE =
            SHIMS.getHadoopConfNames().get("MAPREDMAXSPLITSIZE");

        private static long DEFAULT_MIN_SPLIT_SIZE = 16 * 1024 * 1024;
        private static long DEFAULT_MAX_SPLIT_SIZE = 256 * 1024 * 1024;

        /**
         * When picking the hosts for a split that crosses block boundaries,
         * drop any host that has fewer than MIN_INCLUDED_LOCATION of the
         * number of bytes available on the host with the most.
         * If host1 has 10MB of the split, host2 has 20MB, and host3 has 18MB the
         * split will contain host2 (100% of host2) and host3 (90% of host2). Host1
         * with 50% will be dropped.
         */
        private static double MIN_INCLUDED_LOCATION = 0.80;

        public bool shouldSkipCombine(Path path, Configuration conf)
        {
            return (conf.get(AcidUtils.CONF_ACID_KEY) != null) || AcidUtils.isAcid(path, conf);
        }

        private class OrcRecordReader : RecordReader<NullWritable, OrcStruct>, StatsProvidingRecordReader
        {
            private RecordReader reader;
            private long offset;
            private long length;
            private int numColumns;
            private float progress = 0.0f;
            private Reader file;
            private SerDeStats stats;


            OrcRecordReader(Reader file, Configuration conf, FileSplit split)
            {
                List<OrcProto.Type> types = file.getTypes();
                this.file = file;
                numColumns = (types.Count == 0) ? 0 : types[0].SubtypesCount;
                this.offset = split.getStart();
                this.length = split.getLength();
                this.reader = createReaderFromFile(file, conf, offset, length);
                this.stats = new SerDeStats();
            }

            public bool next(NullWritable key, OrcStruct value)
            {
                if (reader.hasNext())
                {
                    reader.next(value);
                    progress = reader.getProgress();
                    return true;
                }
                else
                {
                    return false;
                }
            }

            public NullWritable createKey()
            {
                return NullWritable.get();
            }

            public OrcStruct createValue()
            {
                return new OrcStruct(numColumns);
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

            public SerDeStats getStats()
            {
                stats.setRawDataSize(file.getRawDataSize());
                stats.setRowCount(file.getNumberOfRows());
                return stats;
            }
        }

        /**
         * Get the root column for the row. In ACID format files, it is offset by
         * the extra metadata columns.
         * @param isOriginal is the file in the original format?
         * @return the column number for the root of row.
         */
        private static int getRootColumn(bool isOriginal)
        {
            return isOriginal ? 0 : (OrcRecordUpdater.ROW + 1);
        }

        public static RecordReader createReaderFromFile(Reader file,
                                                        Configuration conf,
                                                        long offset, long length
                                                        )
        {
            Reader.Options options = new Reader.Options().range(offset, length);
            bool _isOriginal = isOriginal(file);
            List<OrcProto.Type> types = file.getTypes();
            options.include(genIncludedColumns(types, conf, _isOriginal));
            setSearchArgument(options, types, conf, _isOriginal);
            return file.rowsOptions(options);
        }

        public static bool isOriginal(Reader file)
        {
            return !file.hasMetadataValue(OrcRecordUpdater.ACID_KEY_INDEX_NAME);
        }

        /**
         * Recurse down into a type subtree turning on all of the sub-columns.
         * @param types the types of the file
         * @param result the global view of columns that should be included
         * @param typeId the root of tree to enable
         * @param rootColumn the top column
         */
        private static void includeColumnRecursive(List<OrcProto.Type> types,
                                                   bool[] result,
                                                   int typeId,
                                                   int rootColumn)
        {
            result[typeId - rootColumn] = true;
            OrcProto.Type type = types[typeId];
            int children = type.SubtypesCount;
            for (int i = 0; i < children; ++i)
            {
                includeColumnRecursive(types, result, (int)type.SubtypesList[i], rootColumn);
            }
        }

        /**
         * Modifies the SARG, replacing column names with column indexes in target table schema. This
         * basically does the same thing as all the shennannigans with included columns, except for the
         * last step where ORC gets direct subtypes of root column and uses the ordered match to map
         * table columns to file columns. The numbers put into predicate leaf should allow to go into
         * said subtypes directly by index to get the proper index in the file.
         * This won't work with schema evolution, although it's probably much easier to reason about
         * if schema evolution was to be supported, because this is a clear boundary between table
         * schema columns and all things ORC. None of the ORC stuff is used here and none of the
         * table schema stuff is used after that - ORC doesn't need a bunch of extra crap to apply
         * the SARG thus modified.
         */
        public static void translateSargToTableColIndexes(
            SearchArgument sarg, Configuration conf, int rootColumn)
        {
            string nameStr = getNeededColumnNamesString(conf), idStr = getSargColumnIDsString(conf);
            string[] knownNames = nameStr.Split(',');
            string[] idStrs = (idStr == null) ? null : idStr.Split(',');
            Debug.Assert(idStrs == null || knownNames.Length == idStrs.Length);
            Dictionary<string, int> nameIdMap = new Dictionary<string, int>();
            for (int i = 0; i < knownNames.Length; ++i)
            {
                nameIdMap[knownNames[i]] = idStrs != null ? Int32.Parse(idStrs[i]) : i;
            }
            List<PredicateLeaf> leaves = sarg.getLeaves();
            for (int i = 0; i < leaves.Count; ++i)
            {
                PredicateLeaf pl = leaves[i];
                int colId = nameIdMap.get(pl.getColumnName());
                String newColName = RecordReaderImpl.encodeTranslatedSargColumn(rootColumn, colId);
                SearchArgumentFactory.setPredicateLeafColumn(pl, newColName);
            }
            if (LOG.isDebugEnabled())
            {
                LOG.debug("SARG translated into " + sarg);
            }
        }

        public static bool[] genIncludedColumns(
            List<OrcProto.Type> types, List<int> included, bool isOriginal)
        {
            int rootColumn = getRootColumn(isOriginal);
            int numColumns = types.Count - rootColumn;
            bool[] result = new bool[numColumns];
            result[0] = true;
            OrcProto.Type root = types[rootColumn];
            for (int i = 0; i < root.SubtypesCount; ++i)
            {
                if (included.Contains(i))
                {
                    includeColumnRecursive(types, result, (int)root.SubtypesList[i], rootColumn);
                }
            }
            return result;
        }

        /**
         * Take the configuration and figure out which columns we need to include.
         * @param types the types for the file
         * @param conf the configuration
         * @param isOriginal is the file in the original format?
         */
        public static bool[] genIncludedColumns(
            List<OrcProto.Type> types, Configuration conf, bool isOriginal)
        {
            if (!ColumnProjectionUtils.isReadAllColumns(conf))
            {
                List<int> included = ColumnProjectionUtils.getReadColumnIDs(conf);
                return genIncludedColumns(types, included, isOriginal);
            }
            else
            {
                return null;
            }
        }

        public static String[] getSargColumnNames(String[] originalColumnNames,
            List<OrcProto.Type> types, bool[] includedColumns, bool isOriginal)
        {
            int rootColumn = getRootColumn(isOriginal);
            String[] columnNames = new String[types.Count - rootColumn];
            int i = 0;
            // The way this works is as such. originalColumnNames is the equivalent on getNeededColumns
            // from TSOP. They are assumed to be in the same order as the columns in ORC file, AND they are
            // assumed to be equivalent to the columns in includedColumns (because it was generated from
            // the same column list at some point in the past), minus the subtype columns. Therefore, when
            // we go thru all the top level ORC file columns that are included, in order, they match
            // originalColumnNames. This way, we do not depend on names stored inside ORC for SARG leaf
            // column name resolution (see mapSargColumns method).
            foreach (int columnId in types[rootColumn].SubtypesList)
            {
                if (includedColumns == null || includedColumns[columnId - rootColumn])
                {
                    // this is guaranteed to be positive because types only have children
                    // ids greater than their own id.
                    columnNames[columnId - rootColumn] = originalColumnNames[i++];
                }
            }
            return columnNames;
        }

        public static void setSearchArgument(Reader.Options options,
                                      List<OrcProto.Type> types,
                                      Configuration conf,
                                      bool isOriginal)
        {
            String neededColumnNames = getNeededColumnNamesString(conf);
            if (neededColumnNames == null)
            {
                LOG.debug("No ORC pushdown predicate - no column names");
                options.searchArgument(null, null);
                return;
            }
            SearchArgument sarg = ConvertAstToSearchArg.createFromConf(conf);
            if (sarg == null)
            {
                LOG.debug("No ORC pushdown predicate");
                options.searchArgument(null, null);
                return;
            }

            if (LOG.isInfoEnabled())
            {
                LOG.info("ORC pushdown predicate: " + sarg);
            }
            options.searchArgument(sarg, getSargColumnNames(
                neededColumnNames.Split(','), types, options.getInclude(), isOriginal));
        }

        static bool canCreateSargFromConf(Configuration conf)
        {
            if (getNeededColumnNamesString(conf) == null)
            {
                LOG.debug("No ORC pushdown predicate - no column names");
                return false;
            }
            if (!ConvertAstToSearchArg.canCreateFromConf(conf))
            {
                LOG.debug("No ORC pushdown predicate");
                return false;
            }
            return true;
        }

        private static String[] extractNeededColNames(
            List<OrcProto.Type> types, Configuration conf, bool[] include, bool isOriginal)
        {
            return extractNeededColNames(types, getNeededColumnNamesString(conf), include, isOriginal);
        }

        private static String[] extractNeededColNames(
            List<OrcProto.Type> types, String columnNamesString, bool[] include, bool isOriginal)
        {
            return getSargColumnNames(columnNamesString.Split(','), types, include, isOriginal);
        }

        private static String getNeededColumnNamesString(Configuration conf)
        {
            return conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
        }

        private static String getSargColumnIDsString(Configuration conf)
        {
            return conf.getBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, true) ? null
                : conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);
        }
        public bool validateInput(FileSystem fs, HiveConf conf, List<FileStatus> files)
        {
            if (Utilities.isVectorMode(conf))
            {
                return new VectorizedOrcInputFormat().validateInput(fs, conf, files);
            }

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
                catch (IOException e)
                {
                    return false;
                }
            }
            return true;
        }

        /**
         * Get the list of input {@link Path}s for the map-reduce job.
         *
         * @param conf The configuration of the job
         * @return the list of input {@link Path}s for the map-reduce job.
         */
        static Path[] getInputPaths(Configuration conf)
        {
            String dirs = conf.get("mapred.input.dir");
            if (dirs == null)
            {
                throw new IOException("Configuration mapred.input.dir is not defined.");
            }
            String[] list = StringUtils.split(dirs);
            Path[] result = new Path[list.Length];
            for (int i = 0; i < list.Length; i++)
            {
                result[i] = new Path(StringUtils.unEscapeString(list[i]));
            }
            return result;
        }

        /**
         * The global information about the split generation that we pass around to
         * the different worker threads.
         */
        class Context
        {
            private Configuration conf;

            // We store all caches in variables to change the main one based on config.
            // This is not thread safe between different split generations (and wasn't anyway).
            private FooterCache footerCache;
            private static LocalCache localCache;
            private static MetastoreCache metaCache;
            private static ExecutorService threadPool = null;
            private int numBuckets;
            private long maxSize;
            private long minSize;
            private int minSplits;
            private bool footerInSplits;
            private bool cacheStripeDetails;
            private AtomicInteger cacheHitCounter = new AtomicInteger(0);
            private AtomicInteger numFilesCounter = new AtomicInteger(0);
            private ValidTxnList transactionList;
            private SplitStrategyKind splitStrategyKind;
            private SearchArgument sarg;

            Context(Configuration conf, int minSplits = 1)
            {
                this.conf = conf;
                this.sarg = ConvertAstToSearchArg.createFromConf(conf);
                minSize = conf.getLong(MIN_SPLIT_SIZE, DEFAULT_MIN_SPLIT_SIZE);
                maxSize = conf.getLong(MAX_SPLIT_SIZE, DEFAULT_MAX_SPLIT_SIZE);
                String ss = conf.get(ConfVars.HIVE_ORC_SPLIT_STRATEGY.varname);
                if (ss == null || ss.Equals(SplitStrategyKind.HYBRID.ToString()))
                {
                    splitStrategyKind = SplitStrategyKind.HYBRID;
                }
                else
                {
                    LOG.info("Enforcing " + ss + " ORC split strategy");
                    splitStrategyKind = SplitStrategyKind.valueOf(ss);
                }
                footerInSplits = HiveConf.getBoolVar(conf,
                    ConfVars.HIVE_ORC_INCLUDE_FILE_FOOTER_IN_SPLITS);
                numBuckets =
                    Math.Max(conf.getInt(hive_metastoreConstants.BUCKET_COUNT, 0), 0);
                LOG.debug("Number of buckets specified by conf file is " + numBuckets);
                int cacheStripeDetailsSize = HiveConf.getIntVar(conf,
                    ConfVars.HIVE_ORC_CACHE_STRIPE_DETAILS_SIZE);
                int numThreads = HiveConf.getIntVar(conf,
                    ConfVars.HIVE_ORC_COMPUTE_SPLITS_NUM_THREADS);

                cacheStripeDetails = (cacheStripeDetailsSize > 0);

                this.minSplits = Math.Min(cacheStripeDetailsSize, minSplits);

                lock (typeof(Context))
                {
                    if (threadPool == null)
                    {
                        threadPool = Executors.newFixedThreadPool(numThreads,
                            new ThreadFactoryBuilder().setDaemon(true)
                                .setNameFormat("ORC_GET_SPLITS #%d").build());
                    }

                    // TODO: local cache is created once, so the configs for future queries will not be honored.
                    if (cacheStripeDetails)
                    {
                        // Note that there's no FS check here; we implicitly only use metastore cache for
                        // HDFS, because only HDFS would return fileIds for us. If fileId is extended using
                        // size/mod time/etc. for other FSes, we might need to check FSes explicitly because
                        // using such an aggregate fileId cache is not bulletproof and should be disable-able.
                        bool useMetastoreCache = HiveConf.getBoolVar(
                            conf, HiveConf.ConfVars.HIVE_ORC_MS_FOOTER_CACHE_ENABLED);
                        if (localCache == null)
                        {
                            localCache = new LocalCache(numThreads, cacheStripeDetailsSize);
                        }
                        if (useMetastoreCache)
                        {
                            if (metaCache == null)
                            {
                                metaCache = new MetastoreCache(localCache);
                            }
                            Debug.Assert(conf is HiveConf);
                            metaCache.configure((HiveConf)conf);
                        }
                        // Set footer cache for current split generation. See field comment - not thread safe.
                        footerCache = useMetastoreCache ? metaCache : localCache;
                    }
                }
                String value = conf.get(ValidTxnList.VALID_TXNS_KEY,
                                        Long.MAX_VALUE + ":");
                transactionList = new ValidReadTxnList(value);
            }
        }

        /**
         * The full ACID directory information needed for splits; no more calls to HDFS needed.
         * We could just live with AcidUtils.Directory but...
         * 1) That doesn't have base files for the base-directory case.
         * 2) We save fs for convenience to avoid getting it twice.
         */
        class AcidDirInfo
        {
            public AcidDirInfo(FileSystem fs, Path splitPath, Directory acidInfo,
                List<HdfsFileStatusWithId> baseOrOriginalFiles)
            {
                this.splitPath = splitPath;
                this.acidInfo = acidInfo;
                this.baseOrOriginalFiles = baseOrOriginalFiles;
                this.fs = fs;
            }

            FileSystem fs;
            Path splitPath;
            AcidUtils.Directory acidInfo;
            List<HdfsFileStatusWithId> baseOrOriginalFiles;
        }

        interface SplitStrategy<T>
        {
            List<T> getSplits();
        }

        class SplitInfo : ACIDSplitStrategy
        {
            private Context context;
            private FileSystem fs;
            private HdfsFileStatusWithId fileWithId;
            private FileInfo fileInfo;
            private bool isOriginal;
            private List<DeltaMetaData> deltas;
            private bool hasBase;

            SplitInfo(Context context, FileSystem fs,
                HdfsFileStatusWithId fileWithId, FileInfo fileInfo,
                bool isOriginal,
                List<DeltaMetaData> deltas,
                bool hasBase, Path dir, bool[] covered) :
                base(dir, context.numBuckets, deltas, covered)
            {
                this.context = context;
                this.fs = fs;
                this.fileWithId = fileWithId;
                this.fileInfo = fileInfo;
                this.isOriginal = isOriginal;
                this.deltas = deltas;
                this.hasBase = hasBase;
            }

            public SplitInfo(Context context, FileSystem fs, FileStatus fileStatus, FileInfo fileInfo,
                bool isOriginal, List<DeltaMetaData> deltas, bool hasBase, Path dir,
                bool[] covered) :
                this(context, fs, AcidUtils.createOriginalObj(null, fileStatus),
                  fileInfo, isOriginal, deltas, hasBase, dir, covered)
            {
            }
        }

        /**
         * ETL strategy is used when spending little more time in split generation is acceptable
         * (split generation reads and caches file footers).
         */
        class ETLSplitStrategy : SplitStrategy<SplitInfo>, Callable<Void>
        {
            Context context;
            FileSystem fs;
            List<HdfsFileStatusWithId> files;
            bool isOriginal;
            List<DeltaMetaData> deltas;
            Path dir;
            bool[] covered;
            private List<Future<List<OrcSplit>>> splitFuturesRef;

            public ETLSplitStrategy(Context context, FileSystem fs, Path dir,
                List<HdfsFileStatusWithId> children, bool isOriginal, List<DeltaMetaData> deltas,
                bool[] covered)
            {
                this.context = context;
                this.dir = dir;
                this.fs = fs;
                this.files = children;
                this.isOriginal = isOriginal;
                this.deltas = deltas;
                this.covered = covered;
            }

            public List<SplitInfo> getSplits()
            {
                List<SplitInfo> result = new List<SplitInfo>(files.Count);
                // TODO: Right now, we do the metastore call here, so there will be a metastore call per
                //       partition. If we had a sync point after getting file lists, we could make just one
                //       call; this might be too much sync for many partitions and also cause issues with the
                //       huge metastore call result that cannot be handled with in-API batching. To have an
                //       optimal number of metastore calls, we should wait for batch-size number of files (a
                //       few hundreds) to become available, then call metastore.

                // Force local cache if we have deltas.
                FooterCache cache = context.cacheStripeDetails ?
                    (deltas == null ? context.footerCache : Context.localCache) : null;
                if (cache != null)
                {
                    FileInfo[] infos = cache.getAndValidate(files);
                    for (int i = 0; i < files.Count; ++i)
                    {
                        FileInfo info = infos[i];
                        if (info != null)
                        {
                            // Cached copy is valid
                            context.cacheHitCounter.incrementAndGet();
                        }
                        HdfsFileStatusWithId file = files[i];
                        // ignore files of 0 length
                        if (file.getFileStatus().getLen() > 0)
                        {
                            result.Add(new SplitInfo(
                                context, fs, file, info, isOriginal, deltas, true, dir, covered));
                        }
                    }
                }
                else
                {
                    foreach (HdfsFileStatusWithId file in files)
                    {
                        // ignore files of 0 length
                        if (file.getFileStatus().getLen() > 0)
                        {
                            result.Add(new SplitInfo(
                                context, fs, file, null, isOriginal, deltas, true, dir, covered));
                        }
                    }
                }
                return result;
            }

            public override String ToString()
            {
                return typeof(ETLSplitStrategy).Name + " strategy for " + dir;
            }

            public Future<Void> generateSplitWork(
                Context context, List<Future<List<OrcSplit>>> splitFutures)
            {
                if (context.cacheStripeDetails && context.footerCache.isBlocking())
                {
                    this.splitFuturesRef = splitFutures;
                    return Context.threadPool.submit(this);
                }
                else
                {
                    runGetSplitsSync(splitFutures);
                    return null;
                }
            }

            public Void call()
            {
                runGetSplitsSync(splitFuturesRef);
                return null;
            }

            private void runGetSplitsSync(List<Future<List<OrcSplit>>> splitFutures)
            {
                List<SplitInfo> splits = getSplits();
                List<Future<List<OrcSplit>>> localList = new List<SplitInfo>(splits.Count);
                foreach (SplitInfo splitInfo in splits)
                {
                    localList.add(Context.threadPool.submit(new SplitGenerator(splitInfo)));
                }
                lock (splitFutures)
                {
                    splitFutures.addAll(localList);
                }
            }
        }

        /**
         * BI strategy is used when the requirement is to spend less time in split generation
         * as opposed to query execution (split generation does not read or cache file footers).
         */
        class BISplitStrategy : ACIDSplitStrategy
        {
            List<HdfsFileStatusWithId> fileStatuses;
            bool isOriginal;
            List<DeltaMetaData> deltas;
            FileSystem fs;
            Context context;
            Path dir;

            public BISplitStrategy(Context context, FileSystem fs,
                Path dir, List<HdfsFileStatusWithId> fileStatuses, bool isOriginal,
                List<DeltaMetaData> deltas, bool[] covered) :
                base(dir, context.numBuckets, deltas, covered)
            {
                this.context = context;
                this.fileStatuses = fileStatuses;
                this.isOriginal = isOriginal;
                this.deltas = deltas;
                this.fs = fs;
                this.dir = dir;
            }

            public List<OrcSplit> getSplits()
            {
                List<OrcSplit> splits = Lists.newArrayList();
                foreach (HdfsFileStatusWithId file in fileStatuses)
                {
                    FileStatus fileStatus = file.getFileStatus();
                    String[] hosts = SHIMS.getLocationsWithOffset(fs, fileStatus).firstEntry().getValue()
                        .getHosts();
                    OrcSplit orcSplit = new OrcSplit(fileStatus.getPath(), file.getFileId(), 0,
                        fileStatus.getLen(), hosts, null, isOriginal, true, deltas, -1);
                    splits.add(orcSplit);
                }

                // add uncovered ACID delta splits
                splits.addAll(base.getSplits());
                return splits;
            }

            public String toString()
            {
                return typeof(BISplitStrategy).getSimpleName() + " strategy for " + dir;
            }
        }

        /**
         * ACID split strategy is used when there is no base directory (when transactions are enabled).
         */
        class ACIDSplitStrategy : SplitStrategy<OrcSplit>
        {
            Path dir;
            List<DeltaMetaData> deltas;
            bool[] covered;
            int numBuckets;

            public ACIDSplitStrategy(Path dir, int numBuckets, List<DeltaMetaData> deltas, bool[] covered)
            {
                this.dir = dir;
                this.numBuckets = numBuckets;
                this.deltas = deltas;
                this.covered = covered;
            }

            public List<OrcSplit> getSplits()
            {
                // Generate a split for any buckets that weren't covered.
                // This happens in the case where a bucket just has deltas and no
                // base.
                List<OrcSplit> splits = Lists.newArrayList();
                if (!deltas.isEmpty())
                {
                    for (int b = 0; b < numBuckets; ++b)
                    {
                        if (!covered[b])
                        {
                            splits.add(new OrcSplit(dir, null, b, 0, new String[0], null, false, false, deltas, -1));
                        }
                    }
                }
                return splits;
            }

            public String toString()
            {
                return typeof(ACIDSplitStrategy).getSimpleName() + " strategy for " + dir;
            }
        }

        /**
         * Given a directory, get the list of files and blocks in those files.
         * To parallelize file generator use "mapreduce.input.fileinputformat.list-status.num-threads"
         */
        class FileGenerator : Callable<AcidDirInfo>
        {
            private Context context;
            private FileSystem fs;
            private Path dir;
            private bool useFileIds;

            FileGenerator(Context context, FileSystem fs, Path dir, bool useFileIds)
            {
                this.context = context;
                this.fs = fs;
                this.dir = dir;
                this.useFileIds = useFileIds;
            }

            public AcidDirInfo call()
            {
                AcidUtils.Directory dirInfo = AcidUtils.getAcidState(dir,
                    context.conf, context.transactionList, useFileIds);
                Path @base = dirInfo.getBaseDirectory();
                // find the base files (original or new style)
                List<HdfsFileStatusWithId> children = (@base == null)
                    ? dirInfo.getOriginalFiles() : findBaseFiles(@base, useFileIds);
                return new AcidDirInfo(fs, dir, dirInfo, children);
            }

            private List<HdfsFileStatusWithId> findBaseFiles(
                Path @base, bool useFileIds)
            {
                if (useFileIds)
                {
                    try
                    {
                        return SHIMS.listLocatedHdfsStatus(fs, @base, AcidUtils.hiddenFileFilter);
                    }
                    catch (Throwable t)
                    {
                        LOG.error("Failed to get files with ID; using regular API", t);
                    }
                }

                // Fall back to regular API and create states without ID.
                List<FileStatus> children = SHIMS.listLocatedStatus(fs, @base, AcidUtils.hiddenFileFilter);
                List<HdfsFileStatusWithId> result = new List<HdfsFileStatusWithId>(children.Count);
                foreach (FileStatus child in children)
                {
                    result.add(AcidUtils.createOriginalObj(null, child));
                }
                return result;
            }
        }

        /**
         * Split the stripes of a given file into input splits.
         * A thread is used for each file.
         */
        class SplitGenerator : Callable<List<OrcSplit>>
        {
            private Context context;
            private FileSystem fs;
            private HdfsFileStatusWithId fileWithId;
            private FileStatus file;
            private long blockSize;
            private Dictionary<Long, BlockLocation> locations;
            private FileInfo fileInfo;
            private List<StripeInformation> stripes;
            private FileMetaInfo fileMetaInfo;
            private List<StripeStatistics> stripeStats;
            private List<OrcProto.Type> types;
            private bool[] includedCols;
            private bool isOriginal;
            private List<DeltaMetaData> deltas;
            private bool hasBase;
            private OrcFile.WriterVersion writerVersion;
            private long projColsUncompressedSize;
            private List<OrcSplit> deltaSplits;

            public SplitGenerator(SplitInfo splitInfo)
            {
                this.context = splitInfo.context;
                this.fs = splitInfo.fs;
                this.fileWithId = splitInfo.fileWithId;
                this.file = this.fileWithId.getFileStatus();
                this.blockSize = this.file.getBlockSize();
                this.fileInfo = splitInfo.fileInfo;
                // TODO: potential DFS call
                this.locations = SHIMS.getLocationsWithOffset(fs, fileWithId.getFileStatus());
                this.isOriginal = splitInfo.isOriginal;
                this.deltas = splitInfo.deltas;
                this.hasBase = splitInfo.hasBase;
                this.projColsUncompressedSize = -1;
                this.deltaSplits = splitInfo.getSplits();
            }

            Path getPath()
            {
                return fileWithId.getFileStatus().getPath();
            }

            public String toString()
            {
                return "splitter(" + fileWithId.getFileStatus().getPath() + ")";
            }

            /**
             * Compute the number of bytes that overlap between the two ranges.
             * @param offset1 start of range1
             * @param length1 length of range1
             * @param offset2 start of range2
             * @param length2 length of range2
             * @return the number of bytes in the overlap range
             */
            static long getOverlap(long offset1, long length1,
                                   long offset2, long length2)
            {
                long end1 = offset1 + length1;
                long end2 = offset2 + length2;
                if (end2 <= offset1 || end1 <= offset2)
                {
                    return 0;
                }
                else
                {
                    return Math.min(end1, end2) - Math.max(offset1, offset2);
                }
            }

            /**
             * Create an input split over the given range of bytes. The location of the
             * split is based on where the majority of the byte are coming from. ORC
             * files are unlikely to have splits that cross between blocks because they
             * are written with large block sizes.
             * @param offset the start of the split
             * @param length the length of the split
             * @param fileMetaInfo file metadata from footer and postscript
             * @
             */
            OrcSplit createSplit(long offset, long length,
                             FileMetaInfo fileMetaInfo)
            {
                String[] hosts;
                Map.Entry<Long, BlockLocation> startEntry = locations.floorEntry(offset);
                BlockLocation start = startEntry.getValue();
                if (offset + length <= start.getOffset() + start.getLength())
                {
                    // handle the single block case
                    hosts = start.getHosts();
                }
                else
                {
                    Map.Entry<Long, BlockLocation> endEntry = locations.floorEntry(offset + length);
                    BlockLocation end = endEntry.getValue();
                    //get the submap
                    NavigableMap<Long, BlockLocation> navigableMap = locations.subMap(startEntry.getKey(),
                              true, endEntry.getKey(), true);
                    // Calculate the number of bytes in the split that are local to each
                    // host.
                    Dictionary<string, long> sizes = new Dictionary<string, long>();
                    long maxSize = 0;
                    foreach (BlockLocation block in navigableMap.values())
                    {
                        long overlap = getOverlap(offset, length, block.getOffset(),
                            block.getLength());
                        if (overlap > 0)
                        {
                            foreach (string host in block.getHosts())
                            {
                                long val;
                                if (!sizes.TryGetValue(host, out val))
                                {
                                    val = 0;
                                }
                                sizes[host] = val + overlap;
                                maxSize = Math.Max(maxSize, val.get());
                            }
                        }
                        else
                        {
                            throw new IOException("File " + fileWithId.getFileStatus().getPath().toString() +
                                    " should have had overlap on block starting at " + block.getOffset());
                        }
                    }
                    // filter the list of locations to those that have at least 80% of the
                    // max
                    long threshold = (long)(maxSize * MIN_INCLUDED_LOCATION);
                    List<string> hostList = new List<string>();
                    // build the locations in a predictable order to simplify testing
                    foreach (BlockLocation block in navigableMap.values())
                    {
                        foreach (String host in block.getHosts())
                        {
                            if (sizes.containsKey(host))
                            {
                                if (sizes.get(host).get() >= threshold)
                                {
                                    hostList.add(host);
                                }
                                sizes.remove(host);
                            }
                        }
                    }
                    hosts = new String[hostList.Count];
                    hostList.toArray(hosts);
                }

                // scale the raw data size to split level based on ratio of split wrt to file length
                long fileLen = file.getLen();
                double splitRatio = (double)length / (double)fileLen;
                long scaledProjSize = projColsUncompressedSize > 0 ?
                    (long)(splitRatio * projColsUncompressedSize) : fileLen;
                return new OrcSplit(file.getPath(), fileWithId.getFileId(), offset, length, hosts,
                    fileMetaInfo, isOriginal, hasBase, deltas, scaledProjSize);
            }

            /**
             * Divide the adjacent stripes in the file into input splits based on the
             * block size and the configured minimum and maximum sizes.
             */
            public List<OrcSplit> call()
            {
                populateAndCacheStripeDetails();
                List<OrcSplit> splits = Lists.newArrayList();

                // figure out which stripes we need to read
                bool[] includeStripe = null;

                // we can't eliminate stripes if there are deltas because the
                // deltas may change the rows making them match the predicate.
                if ((deltas == null || deltas.isEmpty()) && context.sarg != null)
                {
                    SearchArgument sarg = ConvertAstToSearchArg.createFromConf(context.conf);
                    String[] colNames = extractNeededColNames(types, context.conf, includedCols, isOriginal);
                    includeStripe = pickStripes(context.sarg, colNames, writerVersion, isOriginal,
                        stripeStats, stripes.Count, file.getPath());
                }

                // if we didn't have predicate pushdown, read everything
                if (includeStripe == null)
                {
                    includeStripe = new bool[stripes.Count];
                    Arrays.fill(includeStripe, true);
                }

                long currentOffset = -1;
                long currentLength = 0;
                int idx = -1;
                foreach (StripeInformation stripe in stripes)
                {
                    idx++;

                    if (!includeStripe[idx])
                    {
                        // create split for the previous unfinished stripe
                        if (currentOffset != -1)
                        {
                            splits.add(createSplit(currentOffset, currentLength, fileMetaInfo));
                            currentOffset = -1;
                        }
                        continue;
                    }

                    // if we are working on a stripe, over the min stripe size, and
                    // crossed a block boundary, cut the input split here.
                    if (currentOffset != -1 && currentLength > context.minSize &&
                        (currentOffset / blockSize != stripe.getOffset() / blockSize))
                    {
                        splits.add(createSplit(currentOffset, currentLength, fileMetaInfo));
                        currentOffset = -1;
                    }
                    // if we aren't building a split, start a new one.
                    if (currentOffset == -1)
                    {
                        currentOffset = stripe.getOffset();
                        currentLength = stripe.getLength();
                    }
                    else
                    {
                        currentLength =
                            (stripe.getOffset() + stripe.getLength()) - currentOffset;
                    }
                    if (currentLength >= context.maxSize)
                    {
                        splits.add(createSplit(currentOffset, currentLength, fileMetaInfo));
                        currentOffset = -1;
                    }
                }
                if (currentOffset != -1)
                {
                    splits.add(createSplit(currentOffset, currentLength, fileMetaInfo));
                }

                // add uncovered ACID delta splits
                splits.addAll(deltaSplits);
                return splits;
            }

            private void populateAndCacheStripeDetails()
            {
                // Only create OrcReader if we are missing some information.
                List<OrcProto.ColumnStatistics> colStatsLocal;
                List<OrcProto.Type> typesLocal;
                if (fileInfo != null)
                {
                    stripes = fileInfo.stripeInfos;
                    stripeStats = fileInfo.stripeStats;
                    fileMetaInfo = fileInfo.fileMetaInfo;
                    typesLocal = types = fileInfo.types;
                    colStatsLocal = fileInfo.fileStats;
                    writerVersion = fileInfo.writerVersion;
                    // For multiple runs, in case sendSplitsInFooter changes
                    if (fileMetaInfo == null && context.footerInSplits)
                    {
                        Reader orcReader = createOrcReader();
                        fileInfo.fileMetaInfo = ((ReaderImpl)orcReader).getFileMetaInfo();
                        Debug.Assert(fileInfo.stripeStats != null && fileInfo.types != null
                            && fileInfo.writerVersion != null);
                        // We assume that if we needed to create a reader, we need to cache it to meta cache.
                        // TODO: This will also needlessly overwrite it in local cache for now.
                        context.footerCache.put(fileWithId.getFileId(), file, fileInfo.fileMetaInfo, orcReader);
                    }
                }
                else
                {
                    Reader orcReader = createOrcReader();
                    stripes = orcReader.getStripes();
                    typesLocal = types = orcReader.getTypes();
                    colStatsLocal = orcReader.getOrcProtoFileStatistics();
                    writerVersion = orcReader.getWriterVersion();
                    stripeStats = orcReader.getStripeStatistics();
                    fileMetaInfo = context.footerInSplits ?
                        ((ReaderImpl)orcReader).getFileMetaInfo() : null;
                    if (context.cacheStripeDetails)
                    {
                        Long fileId = fileWithId.getFileId();
                        context.footerCache.put(fileId, file, fileMetaInfo, orcReader);
                    }
                }
                includedCols = genIncludedColumns(types, context.conf, isOriginal);
                projColsUncompressedSize = computeProjectionSize(typesLocal, colStatsLocal, includedCols, isOriginal);
            }

            private Reader createOrcReader()
            {
                return OrcFile.createReader(file.getPath(),
                    OrcFile.readerOptions(context.conf).filesystem(fs));
            }

            private long computeProjectionSize(List<OrcProto.Type> types,
                List<OrcProto.ColumnStatistics> stats, bool[] includedCols, bool isOriginal)
            {
                int rootIdx = getRootColumn(isOriginal);
                List<int> internalColIds = Lists.newArrayList();
                if (includedCols != null)
                {
                    for (int i = 0; i < includedCols.length; i++)
                    {
                        if (includedCols[i])
                        {
                            internalColIds.add(rootIdx + i);
                        }
                    }
                }
                return ReaderImpl.getRawDataSizeFromColIndices(internalColIds, types, stats);
            }
        }

        public static List<OrcSplit> generateSplitsInfo(Configuration conf, int numSplits = -1)
        {
            // Use threads to resolve directories into splits.
            if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ORC_MS_FOOTER_CACHE_ENABLED))
            {
                // Create HiveConf once, since this is expensive.
                conf = new HiveConf(conf, typeof(OrcInputFormat));
            }
            Context context = new Context(conf, numSplits);
            if (LOG.isInfoEnabled())
            {
                LOG.info("ORC pushdown predicate: " + context.sarg);
            }
            bool useFileIds = HiveConf.getBoolVar(conf, ConfVars.HIVE_ORC_INCLUDE_FILE_ID_IN_SPLITS);
            List<OrcSplit> splits = Lists.newArrayList();
            List<Future<AcidDirInfo>> pathFutures = Lists.newArrayList();
            List<Future<Void>> strategyFutures = Lists.newArrayList();
            List<Future<List<OrcSplit>>> splitFutures = Lists.newArrayList();

            // multi-threaded file statuses and split strategy
            Path[] paths = getInputPaths(conf);
            CompletionService<AcidDirInfo> ecs = new ExecutorCompletionService<AcidDirInfo>(Context.threadPool);
            foreach (Path dir in paths)
            {
                FileSystem fs = dir.getFileSystem(conf);
                FileGenerator fileGenerator = new FileGenerator(context, fs, dir, useFileIds);
                pathFutures.add(ecs.submit(fileGenerator));
            }

            // complete path futures and schedule split generation
            try
            {
                for (int notIndex = 0; notIndex < paths.length; ++notIndex)
                {
                    AcidDirInfo adi = ecs.take().get();
                    SplitStrategy splitStrategy = determineSplitStrategy(
                        context, adi.fs, adi.splitPath, adi.acidInfo, adi.baseOrOriginalFiles);

                    if (isDebugEnabled)
                    {
                        LOG.debug(splitStrategy);
                    }

                    // Hack note - different split strategies return differently typed lists, yay Java.
                    // This works purely by magic, because we know which strategy produces which type.
                    if (splitStrategy is ETLSplitStrategy)
                    {
                        Future<Void> ssFuture = ((ETLSplitStrategy)splitStrategy).generateSplitWork(
                            context, splitFutures);
                        if (ssFuture != null)
                        {
                            strategyFutures.add(ssFuture);
                        }
                    }
                    else
                    {
                        List<OrcSplit> readySplits = (List<OrcSplit>)splitStrategy.getSplits();
                        splits.addAll(readySplits);
                    }
                }

                // complete split futures
                foreach (Future<Void> ssFuture in strategyFutures)
                {
                    ssFuture.get(); // Make sure we get exceptions strategies might have thrown.
                }
                // All the split strategies are done, so it must be safe to access splitFutures.
                foreach (Future<List<OrcSplit>> splitFuture in splitFutures)
                {
                    splits.addAll(splitFuture.get());
                }
            }
            catch (Exception e)
            {
                cancelFutures(pathFutures);
                cancelFutures(strategyFutures);
                cancelFutures(splitFutures);
                throw new RuntimeException("ORC split generation failed with exception: " + e.getMessage(), e);
            }

            if (context.cacheStripeDetails)
            {
                LOG.info("FooterCacheHitRatio: " + context.cacheHitCounter.get() + "/"
                    + context.numFilesCounter.get());
            }

            if (isDebugEnabled)
            {
                foreach (OrcSplit split in splits)
                {
                    LOG.debug(split + " projected_columns_uncompressed_size: "
                        + split.getColumnarProjectionSize());
                }
            }
            return splits;
        }

        private static void cancelFutures<T>(List<Future<T>> futures)
        {
            foreach (Future<T> future in futures)
            {
                future.cancel(true);
            }
        }

        public InputSplit[] getSplits(JobConf job,
                                      int numSplits)
        {
            if (isDebugEnabled)
            {
                LOG.debug("getSplits started");
            }
            List<OrcSplit> result = generateSplitsInfo(job, numSplits);
            if (isDebugEnabled)
            {
                LOG.debug("getSplits finished");
            }
            return result.toArray(new InputSplit[result.Count]);
        }

        /**
         * FileInfo.
         *
         * Stores information relevant to split generation for an ORC File.
         *
         */
        private class FileInfo
        {
            private long modificationTime;
            private long size;
            private Long fileId;
            private List<StripeInformation> stripeInfos;
            private FileMetaInfo fileMetaInfo;
            private List<StripeStatistics> stripeStats;
            private List<OrcProto.ColumnStatistics> fileStats;
            private List<OrcProto.Type> types;
            private OrcFile.WriterVersion writerVersion;


            FileInfo(long modificationTime, long size, List<StripeInformation> stripeInfos,
                     List<StripeStatistics> stripeStats, List<OrcProto.Type> types,
                     List<OrcProto.ColumnStatistics> fileStats, FileMetaInfo fileMetaInfo,
                     OrcFile.WriterVersion writerVersion, Long fileId)
            {
                this.modificationTime = modificationTime;
                this.size = size;
                this.fileId = fileId;
                this.stripeInfos = stripeInfos;
                this.fileMetaInfo = fileMetaInfo;
                this.stripeStats = stripeStats;
                this.types = types;
                this.fileStats = fileStats;
                this.writerVersion = writerVersion;
            }
        }

        private org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct>
          createVectorizedReader(InputSplit split, JobConf conf, Reporter reporter
                                 )
        {
            return (org.apache.hadoop.mapred.RecordReader)
              new VectorizedOrcInputFormat().getRecordReader(split, conf, reporter);
        }

        public org.apache.hadoop.mapred.RecordReader<NullWritable, OrcStruct>
        getRecordReader(InputSplit inputSplit, JobConf conf,
                        Reporter reporter)
        {
            bool vectorMode = Utilities.isVectorMode(conf);

            // if HiveCombineInputFormat gives us FileSplits instead of OrcSplits,
            // we know it is not ACID. (see a check in CombineHiveInputFormat.getSplits() that assures this)
            if (inputSplit.getClass() == typeof(FileSplit))
            {
                if (vectorMode)
                {
                    return createVectorizedReader(inputSplit, conf, reporter);
                }
                return new OrcRecordReader(OrcFile.createReader(
                    ((FileSplit)inputSplit).getPath(),
                    OrcFile.readerOptions(conf)), conf, (FileSplit)inputSplit);
            }

            OrcSplit split = (OrcSplit)inputSplit;
            reporter.setStatus(inputSplit.toString());

            Options options = new Options(conf).reporter(reporter);
            RowReader<OrcStruct> inner = getReader(inputSplit, options);


            /*Even though there are no delta files, we still need to produce row ids so that an
            * UPDATE or DELETE statement would work on a table which didn't have any previous updates*/
            if (split.isOriginal() && split.getDeltas().isEmpty())
            {
                if (vectorMode)
                {
                    return createVectorizedReader(inputSplit, conf, reporter);
                }
                else
                {
                    return new NullKeyRecordReader(inner, conf);
                }
            }

            if (vectorMode)
            {
                return (org.apache.hadoop.mapred.RecordReader)
                    new VectorizedOrcAcidRowReader(inner, conf, (FileSplit)inputSplit);
            }
            return new NullKeyRecordReader(inner, conf);
        }
        /**
         * Return a RecordReader that is compatible with the Hive 0.12 reader
         * with NullWritable for the key instead of RecordIdentifier.
         */
        public class NullKeyRecordReader : AcidRecordReader<NullWritable, OrcStruct>
        {
            private RecordIdentifier id;
            private RowReader<OrcStruct> inner;

            public RecordIdentifier getRecordIdentifier()
            {
                return id;
            }
            private NullKeyRecordReader(RowReader<OrcStruct> inner, Configuration conf)
            {
                this.inner = inner;
                id = inner.createKey();
            }
            public bool next(NullWritable nullWritable,
                                OrcStruct orcStruct)
            {
                return inner.next(id, orcStruct);
            }

            public NullWritable createKey()
            {
                return NullWritable.get();
            }

            public OrcStruct createValue()
            {
                return inner.createValue();
            }

            public long getPos()
            {
                return inner.getPos();
            }

            public void close()
            {
                inner.close();
            }

            public float getProgress()
            {
                return inner.getProgress();
            }
        }


        public RowReader<OrcStruct> getReader(InputSplit inputSplit,
                                              Options options)
        {
            OrcSplit split = (OrcSplit)inputSplit;
            Path path = split.getPath();
            Path root;
            if (split.hasBase())
            {
                if (split.isOriginal())
                {
                    root = path.getParent();
                }
                else
                {
                    root = path.getParent().getParent();
                }
            }
            else
            {
                root = path;
            }
            Path[] deltas = AcidUtils.deserializeDeltas(root, split.getDeltas());
            Configuration conf = options.getConfiguration();
            Reader reader;
            int bucket;
            Reader.Options readOptions = new Reader.Options();
            readOptions.range(split.getStart(), split.getLength());
            if (split.hasBase())
            {
                bucket = AcidUtils.parseBaseBucketFilename(split.getPath(), conf)
                    .getBucket();
                reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
                List<OrcProto.Type> types = reader.getTypes();
                readOptions.include(genIncludedColumns(types, conf, split.isOriginal()));
                setSearchArgument(readOptions, types, conf, split.isOriginal());
            }
            else
            {
                bucket = (int)split.getStart();
                reader = null;
                if (deltas != null && deltas.length > 0)
                {
                    Path bucketPath = AcidUtils.createBucketFile(deltas[0], bucket);
                    OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf);
                    FileSystem fs = readerOptions.getFilesystem();
                    if (fs == null)
                    {
                        fs = path.getFileSystem(options.getConfiguration());
                    }
                    if (fs.exists(bucketPath))
                    {
                        /* w/o schema evolution (which ACID doesn't support yet) all delta
                        files have the same schema, so choosing the 1st one*/
                        List<OrcProto.Type> types =
                          OrcFile.createReader(bucketPath, readerOptions).getTypes();
                        readOptions.include(genIncludedColumns(types, conf, split.isOriginal()));
                        setSearchArgument(readOptions, types, conf, split.isOriginal());
                    }
                }
            }
            String txnString = conf.get(ValidTxnList.VALID_TXNS_KEY,
                                        Long.MAX_VALUE + ":");
            ValidTxnList validTxnList = new ValidReadTxnList(txnString);
            OrcRawRecordMerger records =
                new OrcRawRecordMerger(conf, true, reader, split.isOriginal(), bucket,
                    validTxnList, readOptions, deltas);
            return new OrcStructRowReader();
        }

        class OrcStructRowReader : RowReader<OrcStruct>
        {

            OrcStruct innerRecord = records.createValue();

            public ObjectInspector getObjectInspector()
            {
                return ((StructObjectInspector)records.getObjectInspector())
                    .getAllStructFieldRefs().get(OrcRecordUpdater.ROW)
                    .getFieldObjectInspector();
            }

            public bool next(RecordIdentifier recordIdentifier,
                                OrcStruct orcStruct)
            {
                bool result;
                // filter out the deleted records
                do
                {
                    result = records.next(recordIdentifier, innerRecord);
                } while (result &&
                    OrcRecordUpdater.getOperation(innerRecord) ==
                        OrcRecordUpdater.DELETE_OPERATION);
                if (result)
                {
                    // swap the fields with the passed in orcStruct
                    orcStruct.linkFields(OrcRecordUpdater.getRow(innerRecord));
                }
                return result;
            }

            public RecordIdentifier createKey()
            {
                return records.createKey();
            }

            public OrcStruct createValue()
            {
                return new OrcStruct(records.getColumns());
            }

            public long getPos()
            {
                return records.getPos();
            }

            public void close()
            {
                records.close();
            }

            public float getProgress()
            {
                return records.getProgress();
            }
        }

        static Path findOriginalBucket(FileSystem fs,
                                       Path directory,
                                       int bucket)
        {
            foreach (FileStatus stat in fs.listStatus(directory))
            {
                String name = stat.getPath().getName();
                String numberPart = name.substring(0, name.indexOf('_'));
                if (org.apache.commons.lang3.StringUtils.isNumeric(numberPart) &&
                    Integer.parseInt(numberPart) == bucket)
                {
                    return stat.getPath();
                }
            }
            throw new ArgumentException("Can't find bucket " + bucket + " in " +
                directory);
        }

        public static bool[] pickStripesViaTranslatedSarg(SearchArgument sarg,
            WriterVersion writerVersion, List<OrcProto.Type> types,
            List<StripeStatistics> stripeStats, int stripeCount)
        {
            LOG.info("Translated ORC pushdown predicate: " + sarg);
            Debug.Assert(sarg != null);
            if (stripeStats == null || writerVersion == OrcFile.WriterVersion.ORIGINAL)
            {
                return null; // only do split pruning if HIVE-8732 has been fixed in the writer
            }
            // eliminate stripes that doesn't satisfy the predicate condition
            List<PredicateLeaf> sargLeaves = sarg.getLeaves();
            int[] filterColumns = RecordReaderImpl.mapTranslatedSargColumns(types, sargLeaves);
            return pickStripesInternal(sarg, filterColumns, stripeStats, stripeCount, null);
        }

        private static bool[] pickStripes(SearchArgument sarg, String[] sargColNames,
            WriterVersion writerVersion, bool isOriginal, List<StripeStatistics> stripeStats,
            int stripeCount, Path filePath)
        {
            if (sarg == null || stripeStats == null || writerVersion == OrcFile.WriterVersion.ORIGINAL)
            {
                return null; // only do split pruning if HIVE-8732 has been fixed in the writer
            }
            // eliminate stripes that doesn't satisfy the predicate condition
            List<PredicateLeaf> sargLeaves = sarg.getLeaves();
            int[] filterColumns = RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(sargLeaves,
                sargColNames, getRootColumn(isOriginal));
            return pickStripesInternal(sarg, filterColumns, stripeStats, stripeCount, filePath);
        }

        private static bool[] pickStripesInternal(SearchArgument sarg, int[] filterColumns,
            List<StripeStatistics> stripeStats, int stripeCount, Path filePath)
        {
            bool[] includeStripe = new bool[stripeCount];
            for (int i = 0; i < includeStripe.length; ++i)
            {
                includeStripe[i] = (i >= stripeStats.Count) ||
                    isStripeSatisfyPredicate(stripeStats.get(i), sarg, filterColumns);
                if (isDebugEnabled && !includeStripe[i])
                {
                    LOG.debug("Eliminating ORC stripe-" + i + " of file '" + filePath
                        + "'  as it did not satisfy predicate condition.");
                }
            }
            return includeStripe;
        }

        private static bool isStripeSatisfyPredicate(
            StripeStatistics stripeStatistics, SearchArgument sarg, int[] filterColumns)
        {
            List<PredicateLeaf> predLeaves = sarg.getLeaves();
            TruthValue[] truthValues = new TruthValue[predLeaves.Count];
            for (int pred = 0; pred < truthValues.length; pred++)
            {
                if (filterColumns[pred] != -1)
                {

                    // column statistics at index 0 contains only the number of rows
                    ColumnStatistics stats = stripeStatistics.getColumnStatistics()[filterColumns[pred]];
                    truthValues[pred] = RecordReaderImpl.evaluatePredicate(stats, predLeaves.get(pred), null);
                }
                else
                {

                    // parition column case.
                    // partition filter will be evaluated by partition pruner so
                    // we will not evaluate partition filter here.
                    truthValues[pred] = TruthValue.YES_NO_NULL;
                }
            }
            return sarg.evaluate(truthValues).isNeeded();
        }

        internal static SplitStrategy determineSplitStrategy(Context context, FileSystem fs, Path dir,
            AcidUtils.Directory dirInfo, List<HdfsFileStatusWithId> baseOrOriginalFiles)
        {
            Path @base = dirInfo.getBaseDirectory();
            List<HdfsFileStatusWithId> original = dirInfo.getOriginalFiles();
            List<DeltaMetaData> deltas = AcidUtils.serializeDeltas(dirInfo.getCurrentDirectories());
            bool[] covered = new bool[context.numBuckets];
            bool isOriginal = @base == null;

            // if we have a base to work from
            if (@base != null || !original.isEmpty())
            {
                long totalFileSize = 0;
                foreach (HdfsFileStatusWithId child in baseOrOriginalFiles)
                {
                    totalFileSize += child.getFileStatus().getLen();
                    AcidOutputFormat.Options opts = AcidUtils.parseBaseBucketFilename
                        (child.getFileStatus().getPath(), context.conf);
                    int b = opts.getBucket();
                    // If the bucket is in the valid range, mark it as covered.
                    // I wish Hive actually enforced bucketing all of the time.
                    if (b >= 0 && b < covered.length)
                    {
                        covered[b] = true;
                    }
                }

                int numFiles = baseOrOriginalFiles.Count;
                long avgFileSize = totalFileSize / numFiles;
                int totalFiles = context.numFilesCounter.addAndGet(numFiles);
                switch (context.splitStrategyKind)
                {
                    case BI:
                        // BI strategy requested through config
                        return new BISplitStrategy(context, fs, dir, baseOrOriginalFiles, isOriginal, deltas, covered);
                    case ETL:
                        // ETL strategy requested through config
                        return new ETLSplitStrategy(context, fs, dir, baseOrOriginalFiles, isOriginal, deltas, covered);
                    default:
                        // HYBRID strategy
                        if (avgFileSize > context.maxSize || totalFiles <= context.minSplits)
                        {
                            return new ETLSplitStrategy(context, fs, dir, baseOrOriginalFiles, isOriginal, deltas, covered);
                        }
                        else
                        {
                            return new BISplitStrategy(context, fs, dir, baseOrOriginalFiles, isOriginal, deltas, covered);
                        }
                }
            }
            else
            {
                // no base, only deltas
                return new ACIDSplitStrategy(dir, context.numBuckets, deltas, covered);
            }
        }

        public RawReader<OrcStruct> getRawReader(Configuration conf,
                                                 bool collapseEvents,
                                                 int bucket,
                                                 ValidTxnList validTxnList,
                                                 Path baseDirectory,
                                                 Path[] deltaDirectory
                                                 )
        {
            Reader reader = null;
            bool isOriginal = false;
            if (baseDirectory != null)
            {
                Path bucketFile;
                if (baseDirectory.getName().startsWith(AcidUtils.BASE_PREFIX))
                {
                    bucketFile = AcidUtils.createBucketFile(baseDirectory, bucket);
                }
                else
                {
                    isOriginal = true;
                    bucketFile = findOriginalBucket(baseDirectory.getFileSystem(conf),
                        baseDirectory, bucket);
                }
                reader = OrcFile.createReader(bucketFile, OrcFile.readerOptions(conf));
            }
            return new OrcRawRecordMerger(conf, collapseEvents, reader, isOriginal,
                bucket, validTxnList, new Reader.Options(), deltaDirectory);
        }

        /**
         * Represents footer cache.
         */
        public interface FooterCache
        {
            FileInfo[] getAndValidate(List<HdfsFileStatusWithId> files);
            bool isBlocking();
            void put(long fileId, FileStatus file, FileMetaInfo fileMetaInfo, Reader orcReader);
        }

        /** Local footer cache using Guava. Stores convoluted Java objects. */
        private class LocalCache : FooterCache
        {
            private Cache<Path, FileInfo> cache;

            public LocalCache(int numThreads, int cacheStripeDetailsSize)
            {
                cache = CacheBuilder.newBuilder()
                  .concurrencyLevel(numThreads)
                  .initialCapacity(cacheStripeDetailsSize)
                  .maximumSize(cacheStripeDetailsSize)
                  .softValues()
                  .build();
            }

            public FileInfo[] getAndValidate(List<HdfsFileStatusWithId> files)
            {
                // TODO: should local cache also be by fileId? Preserve the original logic for now.
                FileInfo[] result = new FileInfo[files.Count];
                int i = -1;
                foreach (HdfsFileStatusWithId fileWithId in files)
                {
                    ++i;
                    FileStatus file = fileWithId.getFileStatus();
                    Path path = file.getPath();
                    Long fileId = fileWithId.getFileId();
                    FileInfo fileInfo = cache.getIfPresent(path);
                    if (isDebugEnabled)
                    {
                        LOG.debug("Info " + (fileInfo == null ? "not " : "") + "cached for path: " + path);
                    }
                    if (fileInfo == null) continue;
                    if ((fileId != null && fileInfo.fileId != null && fileId == fileInfo.fileId)
                        || (fileInfo.modificationTime == file.getModificationTime() &&
                        fileInfo.size == file.getLen()))
                    {
                        result[i] = fileInfo;
                        continue;
                    }
                    // Invalidate
                    cache.invalidate(path);
                    if (isDebugEnabled)
                    {
                        LOG.debug("Meta-Info for : " + path + " changed. CachedModificationTime: "
                            + fileInfo.modificationTime + ", CurrentModificationTime: "
                            + file.getModificationTime() + ", CachedLength: " + fileInfo.size
                            + ", CurrentLength: " + file.getLen());
                    }
                }
                return result;
            }

            public void put(Path path, FileInfo fileInfo)
            {
                cache.put(path, fileInfo);
            }

            public void put(Long fileId, FileStatus file, FileMetaInfo fileMetaInfo, Reader orcReader)
            {
                cache.put(file.getPath(), new FileInfo(file.getModificationTime(), file.getLen(),
                    orcReader.getStripes(), orcReader.getStripeStatistics(), orcReader.getTypes(),
                    orcReader.getOrcProtoFileStatistics(), fileMetaInfo, orcReader.getWriterVersion(),
                    fileId));
            }

            public bool isBlocking()
            {
                return false;
            }
        }

        /** Metastore-based footer cache storing serialized footers. Also has a local cache. */
        public class MetastoreCache : FooterCache
        {
            private LocalCache localCache;
            private bool isWarnLogged = false;
            private HiveConf conf;

            public MetastoreCache(LocalCache lc)
            {
                localCache = lc;
            }

            public FileInfo[] getAndValidate(List<HdfsFileStatusWithId> files)
            {
                // First, check the local cache.
                FileInfo[] result = localCache.getAndValidate(files);
                Debug.Assert(result.length == files.Count);
                // This is an unfortunate consequence of batching/iterating thru MS results.
                // TODO: maybe have a direct map call for small lists if this becomes a perf issue.
                Dictionary<long, int> posMap = new Dictionary<long, int>(files.Count);
                for (int i = 0; i < result.length; ++i)
                {
                    if (result[i] != null) continue;
                    HdfsFileStatusWithId file = files.get(i);
                    Long fileId = file.getFileId();
                    if (fileId == null)
                    {
                        if (!isWarnLogged || isDebugEnabled)
                        {
                            LOG.warn("Not using metastore cache because fileId is missing: "
                                + file.getFileStatus().getPath());
                            isWarnLogged = true;
                        }
                        continue;
                    }
                    posMap.put(fileId, i);
                }
                Iterator<Entry<Long, ByteBuffer>> iter = null;
                Hive hive;
                try
                {
                    hive = getHive();
                    iter = hive.getFileMetadata(Lists.newArrayList(posMap.keySet()), conf).iterator();
                }
                catch (HiveException ex)
                {
                    throw new IOException(ex);
                }
                List<long> corruptIds = null;
                while (iter.hasNext())
                {
                    Entry<Long, ByteBuffer> e = iter.next();
                    int ix = posMap.get(e.getKey());
                    Debug.Assert(result[ix] == null);
                    HdfsFileStatusWithId file = files.get(ix);
                    Debug.Assert(file.getFileId() == e.getKey());
                    result[ix] = createFileInfoFromMs(file, e.getValue());
                    if (result[ix] == null)
                    {
                        if (corruptIds == null)
                        {
                            corruptIds = new List<long>();
                        }
                        corruptIds.add(file.getFileId());
                    }
                    else
                    {
                        localCache.put(file.getFileStatus().getPath(), result[ix]);
                    }
                }
                if (corruptIds != null)
                {
                    try
                    {
                        hive.clearFileMetadata(corruptIds);
                    }
                    catch (HiveException ex)
                    {
                        LOG.error("Failed to clear corrupt cache data", ex);
                    }
                }
                return result;
            }

            private Hive getHive()
            {
                // TODO: we wish we could cache the Hive object, but it's not thread safe, and each
                //       threadlocal we "cache" would need to be reinitialized for every query. This is
                //       a huge PITA. Hive object will be cached internally, but the compat check will be
                //       done every time inside get().
                return Hive.getWithFastCheck(conf);
            }

            private static FileInfo createFileInfoFromMs(
                HdfsFileStatusWithId file, ByteBuffer bb)
            {
                FileStatus fs = file.getFileStatus();
                ReaderImpl.FooterInfo fi = null;
                ByteBuffer original = bb.duplicate();
                try
                {
                    fi = ReaderImpl.extractMetaInfoFromFooter(bb, fs.getPath());
                }
                catch (Exception ex)
                {
                    byte[] data = new byte[original.remaining()];
                    Array.Copy(original.array(), original.arrayOffset() + original.position(),
                        data, 0, data.length);
                    String msg = "Failed to parse the footer stored in cache for file ID "
                        + file.getFileId() + " " + original + " [ " + Hex.encodeHexString(data) + " ]";
                    LOG.error(msg, ex);
                    return null;
                }
                return new FileInfo(fs.getModificationTime(), fs.getLen(), fi.getStripes(), fi.getMetadata(),
                    fi.getFooter().getTypesList(), fi.getFooter().getStatisticsList(), fi.getFileMetaInfo(),
                    fi.getFileMetaInfo().writerVersion, file.getFileId());
            }

            public void put(Long fileId, FileStatus file, FileMetaInfo fileMetaInfo, Reader orcReader)
            {
                localCache.put(fileId, file, fileMetaInfo, orcReader);
                if (fileId != null)
                {
                    try
                    {
                        getHive().putFileMetadata(Lists.newArrayList(fileId),
                            Lists.newArrayList(((ReaderImpl)orcReader).getSerializedFileFooter()));
                    }
                    catch (HiveException e)
                    {
                        throw new IOException(e);
                    }
                }
            }

            public void configure(HiveConf queryConfig)
            {
                this.conf = queryConfig;
            }

            public bool isBlocking()
            {
                return true;
            }
        }
    }
}
