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
    using System.Globalization;
    using System.IO;
    using OrcSharp.External;

    /**
     * Utilities that are shared by all of the ACID input and output formats. They
     * are used by the compactor and cleaner and thus must be format agnostic.
     */
    public static class AcidUtils
    {
        // This key will be put in the conf file when planning an acid operation
        public const string CONF_ACID_KEY = "hive.doing.acid";
        public const string BASE_PREFIX = "base_";
#if false
  public static PathFilter baseFileFilter = new PathFilter() {
    public bool accept(Path path) {
      return path.getName().startsWith(BASE_PREFIX);
    }
  };
#endif
        public const string DELTA_PREFIX = "delta_";
        public const string DELTA_SIDE_FILE_SUFFIX = "_flush_length";
#if false
  public static PathFilter deltaFileFilter = new PathFilter() {
    public bool accept(Path path) {
      return path.getName().startsWith(DELTA_PREFIX);
    }
  };
#endif
        public const string BUCKET_PREFIX = "bucket_";
#if false
  public static PathFilter bucketFileFilter = new PathFilter() {
    public bool accept(Path path) {
      return path.getName().startsWith(BUCKET_PREFIX) &&
          !path.getName().endsWith(DELTA_SIDE_FILE_SUFFIX);
    }
  };
#endif
        public const string BUCKET_DIGITS = "%05d";
        public const string DELTA_DIGITS = "%07d";
        /**
         * 10K statements per tx.  Probably overkill ... since that many delta files
         * would not be good for performance
         */
        public const string STATEMENT_DIGITS = "%04d";
        /**
         * This must be in sync with {@link #STATEMENT_DIGITS}
         */
        public const int MAX_STATEMENTS_PER_TXN = 10000;
        public static Pattern BUCKET_DIGIT_PATTERN = Pattern.compile("[0-9]{5}$");
        public static Pattern LEGACY_BUCKET_DIGIT_PATTERN = Pattern.compile("^[0-9]{5}");
#if false
  public static PathFilter originalBucketFilter = new PathFilter() {
    public bool accept(Path path) {
      return ORIGINAL_PATTERN.matcher(path.getName()).matches();
    }
  };
#endif

        private static Logger LOG = LoggerFactory.getLog(typeof(AcidUtils));

        private static Pattern ORIGINAL_PATTERN =
            Pattern.compile("[0-9]+_[0-9]+");

#if false
  public static PathFilter hiddenFileFilter = new PathFilter(){
    public bool accept(Path p){
      string name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };
#endif

        /**
   * Create the bucket filename.
   * @param subdir the subdirectory for the bucket.
   * @param bucket the bucket number
   * @return the filename
   */
        public static string createBucketFile(string subdir, int bucket)
        {
            return Path.Combine(subdir,
                BUCKET_PREFIX + string.Format(CultureInfo.InvariantCulture, BUCKET_DIGITS, bucket));
        }

        /**
         * This is format of delta dir name prior to Hive 1.3.x
         */
        public static string deltaSubdir(long min, long max)
        {
            return DELTA_PREFIX + string.Format(CultureInfo.InvariantCulture, DELTA_DIGITS, min) + "_" +
                string.Format(CultureInfo.InvariantCulture, DELTA_DIGITS, max);
        }

        /**
         * Each write statement in a transaction creates its own delta dir.
         * @since 1.3.x
         */
        public static string deltaSubdir(long min, long max, int statementId)
        {
            return deltaSubdir(min, max) + "_" + string.Format(CultureInfo.InvariantCulture, STATEMENT_DIGITS, statementId);
        }

        public static string baseDir(long txnId)
        {
            return BASE_PREFIX + string.Format(CultureInfo.InvariantCulture, DELTA_DIGITS, txnId);
        }
        /**
         * Create a filename for a bucket file.
         * @param directory the partition directory
         * @param options the options for writing the bucket
         * @return the filename that should store the bucket
         */
        public static string createFilename(string directory,
                                          AcidOutputFormat.Options options)
        {
            string subdir;
            if (options.getOldStyle())
            {
                return Path.Combine(directory, string.Format(CultureInfo.InvariantCulture, BUCKET_DIGITS,
                    options.getBucket()) + "_0");
            }
            else if (options.isWritingBase())
            {
                subdir = BASE_PREFIX + string.Format(CultureInfo.InvariantCulture, DELTA_DIGITS,
                    options.getMaximumTransactionId());
            }
            else if (options.getStatementId() == -1)
            {
                //when minor compaction runs, we collapse per statement delta files inside a single
                //transaction so we no longer need a statementId in the file name
                subdir = deltaSubdir(options.getMinimumTransactionId(),
                  options.getMaximumTransactionId());
            }
            else
            {
                subdir = deltaSubdir(options.getMinimumTransactionId(),
                  options.getMaximumTransactionId(),
                  options.getStatementId());
            }
            return createBucketFile(Path.Combine(directory, subdir), options.getBucket());
        }

        /**
         * Get the transaction id from a base directory name.
         * @param path the base directory name
         * @return the maximum transaction id that is included
         */
        static long parseBase(string path)
        {
            string filename = Path.GetFileName(path);
            if (filename.StartsWith(BASE_PREFIX, StringComparison.Ordinal))
            {
                return long.Parse(filename.Substring(BASE_PREFIX.Length));
            }
            throw new ArgumentException(filename + " does not start with " +
                BASE_PREFIX);
        }

        /**
         * Parse a bucket filename back into the options that would have created
         * the file.
         * @param bucketFile the path to a bucket file
         * @param conf the configuration
         * @return the options used to create that filename
         */
        public static AcidOutputFormat.Options
                          parseBaseBucketFilename(string bucketFile,
                                                  Configuration conf)
        {
            AcidOutputFormat.Options result = new AcidOutputFormat.Options(conf);
            string filename = Path.GetFileName(bucketFile);
            result.writingBase(true);
            if (ORIGINAL_PATTERN.matcher(filename).matches())
            {
                int bucket =
                    int.Parse(filename.Substring(0, filename.IndexOf('_')));
                result
                    .setOldStyle(true)
                    .minimumTransactionId(0)
                    .maximumTransactionId(0)
                    .bucket(bucket);
            }
            else if (filename.StartsWith(BUCKET_PREFIX, StringComparison.Ordinal))
            {
                int bucket =
                    int.Parse(filename.Substring(filename.IndexOf('_') + 1));
                result
                    .setOldStyle(false)
                    .minimumTransactionId(0)
                    .maximumTransactionId(parseBase(bucketFile.getParent()))
                    .bucket(bucket);
            }
            else
            {
                result.setOldStyle(true).bucket(-1).minimumTransactionId(0)
                    .maximumTransactionId(0);
            }
            return result;
        }

        public enum Operation { NOT_ACID, INSERT, UPDATE, DELETE }

        public static interface Directory
        {

            /**
             * Get the base directory.
             * @return the base directory to read
             */
            string getBaseDirectory();

            /**
             * Get the list of original files.  Not {@code null}.
             * @return the list of original files (eg. 000000_0)
             */
            List<HdfsFileStatusWithId> getOriginalFiles();

            /**
             * Get the list of base and delta directories that are valid and not
             * obsolete.  Not {@code null}.  List must be sorted in a specific way.
             * See {@link org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDelta#compareTo(org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDelta)}
             * for details.
             * @return the minimal list of current directories
             */
            List<ParsedDelta> getCurrentDirectories();

            /**
             * Get the list of obsolete directories. After filtering out bases and
             * deltas that are not selected by the valid transaction list, return the
             * list of original files, bases, and deltas that have been replaced by
             * more up to date ones.  Not {@code null}.
             */
            List<FileStatus> getObsolete();
        }

        public class ParsedDelta : IComparable<ParsedDelta>
        {
            internal long minTransaction;
            internal long maxTransaction;
            internal FileStatus path;
            //-1 is for internal (getAcidState()) purposes and means the delta dir
            //had no statement ID
            internal int statementId;

            public ParsedDelta(long min, long max, FileStatus path, int statementId = -1)
            {
                this.minTransaction = min;
                this.maxTransaction = max;
                this.path = path;
                this.statementId = statementId;
            }

            public long getMinTransaction()
            {
                return minTransaction;
            }

            public long getMaxTransaction()
            {
                return maxTransaction;
            }

            public string getPath()
            {
                return path.getPath();
            }

            public int getStatementId()
            {
                return statementId == -1 ? 0 : statementId;
            }

            /**
             * Compactions (Major/Minor) merge deltas/bases but delete of old files
             * happens in a different process; thus it's possible to have bases/deltas with
             * overlapping txnId boundaries.  The sort order helps figure out the "best" set of files
             * to use to get data.
             * This sorts "wider" delta before "narrower" i.e. delta_5_20 sorts before delta_5_10 (and delta_11_20)
             */
            public int CompareTo(ParsedDelta parsedDelta)
            {
                if (minTransaction != parsedDelta.minTransaction)
                {
                    if (minTransaction < parsedDelta.minTransaction)
                    {
                        return -1;
                    }
                    else
                    {
                        return 1;
                    }
                }
                else if (maxTransaction != parsedDelta.maxTransaction)
                {
                    if (maxTransaction < parsedDelta.maxTransaction)
                    {
                        return 1;
                    }
                    else
                    {
                        return -1;
                    }
                }
                else if (statementId != parsedDelta.statementId)
                {
                    /**
                     * We want deltas after minor compaction (w/o statementId) to sort
                     * earlier so that getAcidState() considers compacted files (into larger ones) obsolete
                     * Before compaction, include deltas with all statementIds for a given txnId
                     * in a {@link org.apache.hadoop.hive.ql.io.AcidUtils.Directory}
                     */
                    if (statementId < parsedDelta.statementId)
                    {
                        return -1;
                    }
                    else
                    {
                        return 1;
                    }
                }
                else
                {
                    return path.CompareTo(parsedDelta.path);
                }
            }
        }

        /**
         * Convert a list of deltas to a list of delta directories.
         * @param deltas the list of deltas out of a Directory object.
         * @return a list of delta directory paths that need to be read
         */
        public static string[] getPaths(List<ParsedDelta> deltas)
        {
            string[] result = new string[deltas.Count];
            for (int i = 0; i < result.Length; ++i)
            {
                result[i] = deltas[i].getPath();
            }
            return result;
        }

        /**
         * Convert the list of deltas into an equivalent list of begin/end
         * transaction id pairs.  Assumes {@code deltas} is sorted.
         * @param deltas
         * @return the list of transaction ids to serialize
         */
        public static List<AcidInputFormat.DeltaMetaData> serializeDeltas(List<ParsedDelta> deltas)
        {
            List<AcidInputFormat.DeltaMetaData> result = new List<AcidInputFormat.DeltaMetaData>(deltas.Count);
            AcidInputFormat.DeltaMetaData last = null;
            foreach (ParsedDelta parsedDelta in deltas)
            {
                if (last != null && last.getMinTxnId() == parsedDelta.getMinTransaction() && last.getMaxTxnId() == parsedDelta.getMaxTransaction())
                {
                    last.getStmtIds().add(parsedDelta.getStatementId());
                    continue;
                }
                last = new AcidInputFormat.DeltaMetaData(parsedDelta.getMinTransaction(), parsedDelta.getMaxTransaction(), new List<int>());
                result.Add(last);
                if (parsedDelta.statementId >= 0)
                {
                    last.getStmtIds().add(parsedDelta.getStatementId());
                }
            }
            return result;
        }

        /**
         * Convert the list of begin/end transaction id pairs to a list of delta
         * directories.  Note that there may be multiple delta files for the exact same txn range starting
         * with 1.3.x;
         * see {@link org.apache.hadoop.hive.ql.io.AcidUtils#deltaSubdir(long, long, int)}
         * @param root the root directory
         * @param deltas list of begin/end transaction id pairs
         * @return the list of delta paths
         */
        public static string[] deserializeDeltas(string root, List<AcidInputFormat.DeltaMetaData> deltas)
        {
            List<string> results = new List<string>(deltas.Count);
            foreach (AcidInputFormat.DeltaMetaData dmd in deltas)
            {
                if (dmd.getStmtIds().isEmpty())
                {
                    results.Add(new string(root, deltaSubdir(dmd.getMinTxnId(), dmd.getMaxTxnId())));
                    continue;
                }
                foreach (int stmtId in dmd.getStmtIds())
                {
                    results.Add(new string(root, deltaSubdir(dmd.getMinTxnId(), dmd.getMaxTxnId(), stmtId)));
                }
            }
            return results.ToArray();
        }

        private static ParsedDelta parseDelta(FileStatus path)
        {
            ParsedDelta p = parsedDelta(path.getPath());
            return new ParsedDelta(p.getMinTransaction(),
              p.getMaxTransaction(), path, p.statementId);
        }
        public static ParsedDelta parsedDelta(string deltaDir)
        {
            string filename = Path.GetFileName(deltaDir);
            if (filename.StartsWith(DELTA_PREFIX, StringComparison.Ordinal))
            {
                string rest = filename.Substring(DELTA_PREFIX.Length);
                int split = rest.IndexOf('_');
                int split2 = rest.IndexOf('_', split + 1);//may be -1 if no statementId
                long min = long.Parse(rest.Substring(0, split));
                long max = split2 == -1 ?
                  long.Parse(rest.Substring(split + 1)) :
                  long.Parse(rest.Substring(split + 1, split2));
                if (split2 == -1)
                {
                    return new ParsedDelta(min, max, null);
                }
                int statementId = int.Parse(rest.Substring(split2 + 1));
                return new ParsedDelta(min, max, null, statementId);
            }
            throw new ArgumentException(deltaDir + " does not start with " +
                                               DELTA_PREFIX);
        }

        /**
         * Is the given directory in ACID format?
         * @param directory the partition directory to check
         * @param conf the query configuration
         * @return true, if it is an ACID directory
         */
        public static bool isAcid(string directory,
                                     Configuration conf)
        {
            FileSystem fs = directory.getFileSystem(conf);
            foreach (FileStatus file in fs.listStatus(directory))
            {
                string filename = file.getPath().getName();
                if (filename.StartsWith(BASE_PREFIX, StringComparison.Ordinal) ||
                    filename.StartsWith(DELTA_PREFIX, StringComparison.Ordinal))
                {
                    if (file.isDir())
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        public static Directory getAcidState(string directory,
            Configuration conf,
            ValidTxnList txnList
            )
        {
            return getAcidState(directory, conf, txnList, false);
        }

        /** State class for getChildState; cannot modify 2 things in a method. */
        private class TxnBase
        {
            internal FileStatus status;
            internal long txn;
        }

        /**
         * Get the ACID state of the given directory. It finds the minimal set of
         * base and diff directories. Note that because major compactions don't
         * preserve the history, we can't use a base directory that includes a
         * transaction id that we must exclude.
         * @param directory the partition directory to analyze
         * @param conf the configuration
         * @param txnList the list of transactions that we are reading
         * @return the state of the directory
         */
        public static Directory getAcidState(string directory,
                                             Configuration conf,
                                             ValidTxnList txnList,
                                             bool useFileIds
                                             )
        {
            FileSystem fs = directory.getFileSystem(conf);
            List<ParsedDelta> deltas = new List<ParsedDelta>();
            List<ParsedDelta> working = new List<ParsedDelta>();
            List<FileStatus> originalDirectories = new List<FileStatus>();
            List<FileStatus> obsolete = new List<FileStatus>();
            List<HdfsFileStatusWithId> childrenWithId = null;
            if (useFileIds)
            {
                try
                {
                    childrenWithId = SHIMS.listLocatedHdfsStatus(fs, directory, hiddenFileFilter);
                }
                catch (Exception t)
                {
                    LOG.error("Failed to get files with ID; using regular API", t);
                    useFileIds = false;
                }
            }
            TxnBase bestBase = new TxnBase();
            List<HdfsFileStatusWithId> original = new List<HdfsFileStatusWithId>();
            if (childrenWithId != null)
            {
                foreach (HdfsFileStatusWithId child in childrenWithId)
                {
                    getChildState(child.getFileStatus(), child, txnList, working,
                        originalDirectories, original, obsolete, bestBase);
                }
            }
            else
            {
                List<FileStatus> children = SHIMS.listLocatedStatus(fs, directory, hiddenFileFilter);
                foreach (FileStatus child in children)
                {
                    getChildState(
                        child, null, txnList, working, originalDirectories, original, obsolete, bestBase);
                }
            }

            // If we have a base, the original files are obsolete.
            if (bestBase.status != null)
            {
                // remove the entries so we don't get confused later and think we should
                // use them.
                original.Clear();
            }
            else
            {
                // Okay, we're going to need these originals.  Recurse through them and figure out what we
                // really need.
                foreach (FileStatus origDir in originalDirectories)
                {
                    findOriginals(fs, origDir, original, useFileIds);
                }
            }

            working.Sort();
            //so now, 'working' should be sorted like delta_5_20 delta_5_10 delta_11_20 delta_51_60 for example
            //and we want to end up with the best set containing all relevant data: delta_5_20 delta_51_60,
            //subject to list of 'exceptions' in 'txnList' (not show in above example).
            long current = bestBase.txn;
            int lastStmtId = -1;
            foreach (ParsedDelta next in working)
            {
                if (next.maxTransaction > current)
                {
                    // are any of the new transactions ones that we care about?
                    if (txnList.isTxnRangeValid(current + 1, next.maxTransaction) !=
                      ValidTxnList.RangeResponse.NONE)
                    {
                        deltas.Add(next);
                        current = next.maxTransaction;
                        lastStmtId = next.statementId;
                    }
                }
                else if (next.maxTransaction == current && lastStmtId >= 0)
                {
                    //make sure to get all deltas within a single transaction;  multi-statement txn
                    //generate multiple delta files with the same txnId range
                    //of course, if maxTransaction has already been minor compacted, all per statement deltas are obsolete
                    deltas.Add(next);
                }
                else
                {
                    obsolete.Add(next.path);
                }
            }

            string @base = bestBase.status == null ? null : bestBase.status.getPath();
            LOG.debug("in directory " + directory.toUri().ToString() + " base = " + @base + " deltas = " +
                deltas.Count);

#if false
    return new Directory(){

      public string getBaseDirectory() {
        return @base;
      }

      public List<HdfsFileStatusWithId> getOriginalFiles() {
        return original;
      }

      public List<ParsedDelta> getCurrentDirectories() {
        return deltas;
      }

      public List<FileStatus> getObsolete() {
        return obsolete;
      }
    };
#endif
            return null;
        }

        private static void getChildState(FileStatus child, HdfsFileStatusWithId childWithId,
            ValidTxnList txnList, List<ParsedDelta> working, List<FileStatus> originalDirectories,
            List<HdfsFileStatusWithId> original, List<FileStatus> obsolete, TxnBase bestBase)
        {
            string p = child.getPath();
            string fn = Path.GetFileName(p);
            if (fn.StartsWith(BASE_PREFIX, StringComparison.Ordinal) && child.isDir())
            {
                long txn = parseBase(p);
                if (bestBase.status == null)
                {
                    bestBase.status = child;
                    bestBase.txn = txn;
                }
                else if (bestBase.txn < txn)
                {
                    obsolete.Add(bestBase.status);
                    bestBase.status = child;
                    bestBase.txn = txn;
                }
                else
                {
                    obsolete.Add(child);
                }
            }
            else if (fn.StartsWith(DELTA_PREFIX, StringComparison.Ordinal) && child.isDir())
            {
                ParsedDelta delta = parseDelta(child);
                if (txnList.isTxnRangeValid(delta.minTransaction,
                    delta.maxTransaction) !=
                    ValidTxnList.RangeResponse.NONE)
                {
                    working.Add(delta);
                }
            }
            else if (child.isDir())
            {
                // This is just the directory.  We need to recurse and find the actual files.  But don't
                // do this until we have determined there is no base.  This saves time.  Plus,
                // it is possible that the cleaner is running and removing these original files,
                // in which case recursing through them could cause us to get an error.
                originalDirectories.Add(child);
            }
            else
            {
                original.Add(createOriginalObj(childWithId, child));
            }
        }

        public static HdfsFileStatusWithId createOriginalObj(
            HdfsFileStatusWithId childWithId, FileStatus child)
        {
            return childWithId != null ? childWithId : new HdfsFileStatusWithoutId(child);
        }

        private class HdfsFileStatusWithoutId : HdfsFileStatusWithId
        {
            private FileStatus fs;

            public HdfsFileStatusWithoutId(FileStatus fs)
            {
                this.fs = fs;
            }

            public FileStatus getFileStatus()
            {
                return fs;
            }

            public Long getFileId()
            {
                return null;
            }
        }

        /**
         * Find the original files (non-ACID layout) recursively under the partition directory.
         * @param fs the file system
         * @param stat the directory to add
         * @param original the list of original files
         */
        private static void findOriginals(FileSystem fs, FileStatus stat,
            List<HdfsFileStatusWithId> original, bool useFileIds)
        {
            Debug.Assert(stat.isDir());
            List<HdfsFileStatusWithId> childrenWithId = null;
            if (useFileIds)
            {
                try
                {
                    childrenWithId = SHIMS.listLocatedHdfsStatus(fs, stat.getPath(), hiddenFileFilter);
                }
                catch (Exception t)
                {
                    LOG.error("Failed to get files with ID; using regular API", t);
                    useFileIds = false;
                }
            }
            if (childrenWithId != null)
            {
                foreach (HdfsFileStatusWithId child in childrenWithId)
                {
                    if (child.getFileStatus().isDir())
                    {
                        findOriginals(fs, child.getFileStatus(), original, useFileIds);
                    }
                    else
                    {
                        original.Add(child);
                    }
                }
            }
            else
            {
                List<FileStatus> children = SHIMS.listLocatedStatus(fs, stat.getPath(), hiddenFileFilter);
                foreach (FileStatus child in children)
                {
                    if (child.isDir())
                    {
                        findOriginals(fs, child, original, useFileIds);
                    }
                    else
                    {
                        original.Add(createOriginalObj(null, child));
                    }
                }
            }
        }
    }
}
