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
    using System.Globalization;
    using System.IO;
    using org.apache.hadoop.hive.ql.io.orc.external;
    using org.apache.hadoop.hive.ql.io.orc.query;
    using OrcProto = global::orc.proto;

    public class RecordReaderImpl : RecordReader
    {
        internal static Log LOG = LogFactory.getLog(typeof(RecordReaderImpl));
        private static bool isLogDebugEnabled = LOG.isDebugEnabled();
        private string path;
        private long firstRow;
        private List<StripeInformation> stripes = new List<StripeInformation>();
        private OrcProto.StripeFooter stripeFooter;
        private long totalRowCount;
        private CompressionCodec codec;
        private IList<OrcProto.Type> types;
        private int bufferSize;
        private bool[] included;
        private long rowIndexStride;
        private long rowInStripe = 0;
        private int currentStripe = -1;
        private long rowBaseInStripe = 0;
        private long rowCountInStripe = 0;
        private Dictionary<StreamName, InStream> streams =
            new Dictionary<StreamName, InStream>();
        DiskRangeList bufferChunks = null;
        private TreeReaderFactory.TreeReader reader;
        private OrcProto.RowIndex[] indexes;
        private OrcProto.BloomFilterIndex[] bloomFilterIndices;
        private SargApplier sargApp;
        // an array about which row groups aren't skipped
        private bool[] includedRowGroups = null;
        private Configuration conf;
        private MetadataReader metadata;
        private DataReader dataReader;

        public class Index
        {
            OrcProto.RowIndex[] rowGroupIndex;
            OrcProto.BloomFilterIndex[] bloomFilterIndex;

            public Index(OrcProto.RowIndex[] rgIndex, OrcProto.BloomFilterIndex[] bfIndex)
            {
                this.rowGroupIndex = rgIndex;
                this.bloomFilterIndex = bfIndex;
            }

            public OrcProto.RowIndex[] getRowGroupIndex()
            {
                return rowGroupIndex;
            }

            public OrcProto.BloomFilterIndex[] getBloomFilterIndex()
            {
                return bloomFilterIndex;
            }

            public void setRowGroupIndex(OrcProto.RowIndex[] rowGroupIndex)
            {
                this.rowGroupIndex = rowGroupIndex;
            }
        }

        /**
         * Given a list of column names, find the given column and return the index.
         *
         * @param columnNames the list of potential column names
         * @param columnName  the column name to look for
         * @param rootColumn  offset the result with the rootColumn
         * @return the column number or -1 if the column wasn't found
         */
        static int findColumns(string[] columnNames,
                               string columnName,
                               int rootColumn)
        {
            for (int i = 0; i < columnNames.Length; ++i)
            {
                if (columnName.Equals(columnNames[i]))
                {
                    return i + rootColumn;
                }
            }
            return -1;
        }

        /**
         * Find the mapping from predicate leaves to columns.
         * @param sargLeaves the search argument that we need to map
         * @param columnNames the names of the columns
         * @param rootColumn the offset of the top level row, which offsets the
         *                   result
         * @return an array mapping the sarg leaves to concrete column numbers
         */
        public static int[] mapSargColumnsToOrcInternalColIdx(List<PredicateLeaf> sargLeaves,
                                   string[] columnNames,
                                   int rootColumn)
        {
            int[] result = new int[sargLeaves.Count];
            Arrays.fill(result, -1);
            for (int i = 0; i < result.Length; ++i)
            {
                string colName = sargLeaves[i].getColumnName();
                result[i] = findColumns(columnNames, colName, rootColumn);
            }
            return result;
        }

        public RecordReaderImpl(
            IList<StripeInformation> stripes,
            Stream file,
            string path,
            RecordReaderOptions options,
            IList<OrcProto.Type> types,
            CompressionCodec codec,
            int bufferSize,
            long strideRate,
            Configuration conf)
        {
            this.path = path;
            this.codec = codec;
            this.types = types;
            this.bufferSize = bufferSize;
            this.included = options.getInclude();
            this.conf = conf;
            this.rowIndexStride = strideRate;
            this.metadata = new MetadataReaderImpl(file, codec, bufferSize, types.Count);
            SearchArgument sarg = options.getSearchArgument();
            if (sarg != null && strideRate != 0)
            {
                sargApp = new SargApplier(
                    sarg, options.getColumnNames(), strideRate, types, included.Length);
            }
            else
            {
                sargApp = null;
            }
            long rows = 0;
            long skippedRows = 0;
            long offset = options.getOffset();
            long maxOffset = options.getMaxOffset();
            foreach (StripeInformation stripe in stripes)
            {
                long stripeStart = stripe.getOffset();
                if (offset > stripeStart)
                {
                    skippedRows += stripe.getNumberOfRows();
                }
                else if (stripeStart < maxOffset)
                {
                    this.stripes.Add(stripe);
                    rows += stripe.getNumberOfRows();
                }
            }

            bool? zeroCopy = options.getUseZeroCopy();
            if (zeroCopy == null)
            {
                zeroCopy = OrcConf.USE_ZEROCOPY.getBoolean(conf);
            }
            // TODO: we could change the ctor to pass this externally
            this.dataReader = RecordReaderUtils.createDefaultDataReader(file, path, zeroCopy.Value, codec);
            this.dataReader.open();

            firstRow = skippedRows;
            totalRowCount = rows;
            bool? skipCorrupt = options.getSkipCorruptRecords();
            if (skipCorrupt == null)
            {
                skipCorrupt = OrcConf.SKIP_CORRUPT_DATA.getBoolean(conf);
            }

            reader = RecordReaderFactory.createTreeReader(0, conf, types, included, skipCorrupt.Value);
            indexes = new OrcProto.RowIndex[types.Count];
            bloomFilterIndices = new OrcProto.BloomFilterIndex[types.Count];
            advanceToNextRow(reader, 0L, true);
        }

        public class PositionProviderImpl : PositionProvider
        {
            private OrcProto.RowIndexEntry entry;
            private int index;

            public PositionProviderImpl(OrcProto.RowIndexEntry entry, int startPos = 0)
            {
                this.entry = entry;
                this.index = startPos;
            }

            public long getNext()
            {
                return (long)entry.PositionsList[index++];
            }
        }

        internal OrcProto.StripeFooter readStripeFooter(StripeInformation stripe)
        {
            return metadata.readStripeFooter(stripe);
        }

        internal enum Location
        {
            BEFORE, MIN, MIDDLE, MAX, AFTER
        }

        /**
         * Given a point and min and max, determine if the point is before, at the
         * min, in the middle, at the max, or after the range.
         * @param point the point to test
         * @param min the minimum point
         * @param max the maximum point
         * @param <T> the type of the comparision
         * @return the location of the point
         */
        internal static Location compareToRange(IComparable point, object min, object max)
        {
            int minCompare = point.CompareTo(min);
            if (minCompare < 0)
            {
                return Location.BEFORE;
            }
            else if (minCompare == 0)
            {
                return Location.MIN;
            }
            int maxCompare = point.CompareTo(max);
            if (maxCompare > 0)
            {
                return Location.AFTER;
            }
            else if (maxCompare == 0)
            {
                return Location.MAX;
            }
            return Location.MIDDLE;
        }

        /**
         * Get the maximum value out of an index entry.
         * @param index
         *          the index entry
         * @return the object for the maximum value or null if there isn't one
         */
        internal static object getMax(ColumnStatistics index)
        {
            if (index is IntegerColumnStatistics)
            {
                return ((IntegerColumnStatistics)index).getMaximum();
            }
            else if (index is DoubleColumnStatistics)
            {
                return ((DoubleColumnStatistics)index).getMaximum();
            }
            else if (index is StringColumnStatistics)
            {
                return ((StringColumnStatistics)index).getMaximum();
            }
            else if (index is DateColumnStatistics)
            {
                return ((DateColumnStatistics)index).getMaximum();
            }
            else if (index is DecimalColumnStatistics)
            {
                return ((DecimalColumnStatistics)index).getMaximum();
            }
            else if (index is TimestampColumnStatistics)
            {
                return ((TimestampColumnStatistics)index).getMaximum();
            }
            else if (index is BooleanColumnStatistics)
            {
                if (((BooleanColumnStatistics)index).getTrueCount() != 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return null;
            }
        }

        /**
         * Get the minimum value out of an index entry.
         * @param index
         *          the index entry
         * @return the object for the minimum value or null if there isn't one
         */
        internal static object getMin(ColumnStatistics index)
        {
            if (index is IntegerColumnStatistics)
            {
                return ((IntegerColumnStatistics)index).getMinimum();
            }
            else if (index is DoubleColumnStatistics)
            {
                return ((DoubleColumnStatistics)index).getMinimum();
            }
            else if (index is StringColumnStatistics)
            {
                return ((StringColumnStatistics)index).getMinimum();
            }
            else if (index is DateColumnStatistics)
            {
                return ((DateColumnStatistics)index).getMinimum();
            }
            else if (index is DecimalColumnStatistics)
            {
                return ((DecimalColumnStatistics)index).getMinimum();
            }
            else if (index is TimestampColumnStatistics)
            {
                return ((TimestampColumnStatistics)index).getMinimum();
            }
            else if (index is BooleanColumnStatistics)
            {
                if (((BooleanColumnStatistics)index).getFalseCount() != 0)
                {
                    return false;
                }
                else
                {
                    return true;
                }
            }
            else
            {
                return null;
            }
        }

        /**
         * Evaluate a predicate with respect to the statistics from the column
         * that is referenced in the predicate.
         * @param statsProto the statistics for the column mentioned in the predicate
         * @param predicate the leaf predicate we need to evaluation
         * @param bloomFilter
         * @return the set of truth values that may be returned for the given
         *   predicate.
         */
        internal static TruthValue evaluatePredicateProto(OrcProto.ColumnStatistics statsProto,
            PredicateLeaf predicate, OrcProto.BloomFilter bloomFilter)
        {
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(statsProto);
            object minValue = getMin(cs);
            object maxValue = getMax(cs);
            BloomFilter bf = null;
            if (bloomFilter != null)
            {
                bf = BloomFilterIO.Create(bloomFilter);
            }
            return evaluatePredicateRange(predicate, minValue, maxValue, cs.hasNull(), bf);
        }

        /**
         * Evaluate a predicate with respect to the statistics from the column
         * that is referenced in the predicate.
         * @param stats the statistics for the column mentioned in the predicate
         * @param predicate the leaf predicate we need to evaluation
         * @return the set of truth values that may be returned for the given
         *   predicate.
         */
        internal static TruthValue evaluatePredicate(ColumnStatistics stats,
            PredicateLeaf predicate, BloomFilter bloomFilter)
        {
            object minValue = getMin(stats);
            object maxValue = getMax(stats);
            return evaluatePredicateRange(predicate, minValue, maxValue, stats.hasNull(), bloomFilter);
        }

        static TruthValue evaluatePredicateRange(PredicateLeaf predicate, object min,
            object max, bool hasNull, BloomFilter bloomFilter)
        {
            // if we didn't have any values, everything must have been null
            if (min == null)
            {
                if (predicate.getOperator() == PredicateLeaf.Operator.IS_NULL)
                {
                    return TruthValue.YES;
                }
                else
                {
                    return TruthValue.NULL;
                }
            }

            TruthValue result;
            try
            {
                // Predicate object and stats objects are converted to the type of the predicate object.
                object baseObj = predicate.getLiteral();
                object minValue = getBaseObjectForComparison(predicate.getType(), min);
                object maxValue = getBaseObjectForComparison(predicate.getType(), max);
                object predObj = getBaseObjectForComparison(predicate.getType(), baseObj);

                result = evaluatePredicateMinMax(predicate, predObj, minValue, maxValue, hasNull);
                if (shouldEvaluateBloomFilter(predicate, result, bloomFilter))
                {
                    result = evaluatePredicateBloomFilter(predicate, predObj, bloomFilter, hasNull);
                }
                // in case failed conversion, return the default YES_NO_NULL truth value
            }
            catch (Exception e)
            {
                if (LOG.isWarnEnabled())
                {
                    LOG.warn("Exception when evaluating predicate. Skipping ORC PPD." +
                        " Exception: " + e.ToString());
                }
                if (predicate.getOperator().Equals(PredicateLeaf.Operator.NULL_SAFE_EQUALS) || !hasNull)
                {
                    result = TruthValue.YES_NO;
                }
                else
                {
                    result = TruthValue.YES_NO_NULL;
                }
            }
            return result;
        }

        private static bool shouldEvaluateBloomFilter(PredicateLeaf predicate,
            TruthValue result, BloomFilter bloomFilter)
        {
            // evaluate bloom filter only when
            // 1) Bloom filter is available
            // 2) Min/Max evaluation yield YES or MAYBE
            // 3) Predicate is EQUALS or IN list
            if (bloomFilter != null
                && result != TruthValue.NO_NULL && result != TruthValue.NO
                && (predicate.getOperator().Equals(PredicateLeaf.Operator.EQUALS)
                    || predicate.getOperator().Equals(PredicateLeaf.Operator.NULL_SAFE_EQUALS)
                    || predicate.getOperator().Equals(PredicateLeaf.Operator.IN)))
            {
                return true;
            }
            return false;
        }

        private static TruthValue evaluatePredicateMinMax(PredicateLeaf predicate, object predObj,
            object minValue,
            object maxValue,
            bool hasNull)
        {
            Location loc;

            switch (predicate.getOperator())
            {
                case PredicateLeaf.Operator.NULL_SAFE_EQUALS:
                    loc = compareToRange((IComparable)predObj, minValue, maxValue);
                    if (loc == Location.BEFORE || loc == Location.AFTER)
                    {
                        return TruthValue.NO;
                    }
                    else
                    {
                        return TruthValue.YES_NO;
                    }
                case PredicateLeaf.Operator.EQUALS:
                    loc = compareToRange((IComparable)predObj, minValue, maxValue);
                    if (minValue.Equals(maxValue) && loc == Location.MIN)
                    {
                        return hasNull ? TruthValue.YES_NULL : TruthValue.YES;
                    }
                    else if (loc == Location.BEFORE || loc == Location.AFTER)
                    {
                        return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
                    }
                    else
                    {
                        return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
                    }
                case PredicateLeaf.Operator.LESS_THAN:
                    loc = compareToRange((IComparable)predObj, minValue, maxValue);
                    if (loc == Location.AFTER)
                    {
                        return hasNull ? TruthValue.YES_NULL : TruthValue.YES;
                    }
                    else if (loc == Location.BEFORE || loc == Location.MIN)
                    {
                        return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
                    }
                    else
                    {
                        return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
                    }
                case PredicateLeaf.Operator.LESS_THAN_EQUALS:
                    loc = compareToRange((IComparable)predObj, minValue, maxValue);
                    if (loc == Location.AFTER || loc == Location.MAX)
                    {
                        return hasNull ? TruthValue.YES_NULL : TruthValue.YES;
                    }
                    else if (loc == Location.BEFORE)
                    {
                        return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
                    }
                    else
                    {
                        return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
                    }
                case PredicateLeaf.Operator.IN:
                    if (minValue.Equals(maxValue))
                    {
                        // for a single value, look through to see if that value is in the
                        // set
                        foreach (object arg in predicate.getLiteralList())
                        {
                            predObj = getBaseObjectForComparison(predicate.getType(), arg);
                            loc = compareToRange((IComparable)predObj, minValue, maxValue);
                            if (loc == Location.MIN)
                            {
                                return hasNull ? TruthValue.YES_NULL : TruthValue.YES;
                            }
                        }
                        return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
                    }
                    else
                    {
                        // are all of the values outside of the range?
                        foreach (object arg in predicate.getLiteralList())
                        {
                            predObj = getBaseObjectForComparison(predicate.getType(), arg);
                            loc = compareToRange((IComparable)predObj, minValue, maxValue);
                            if (loc == Location.MIN || loc == Location.MIDDLE ||
                                loc == Location.MAX)
                            {
                                return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
                            }
                        }
                        return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
                    }
                case PredicateLeaf.Operator.BETWEEN:
                    List<object> args = predicate.getLiteralList();
                    object predObj1 = getBaseObjectForComparison(predicate.getType(), args[0]);

                    loc = compareToRange((IComparable)predObj1, minValue, maxValue);
                    if (loc == Location.BEFORE || loc == Location.MIN)
                    {
                        object predObj2 = getBaseObjectForComparison(predicate.getType(), args[1]);

                        Location loc2 = compareToRange((IComparable)predObj2, minValue, maxValue);
                        if (loc2 == Location.AFTER || loc2 == Location.MAX)
                        {
                            return hasNull ? TruthValue.YES_NULL : TruthValue.YES;
                        }
                        else if (loc2 == Location.BEFORE)
                        {
                            return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
                        }
                        else
                        {
                            return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
                        }
                    }
                    else if (loc == Location.AFTER)
                    {
                        return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
                    }
                    else
                    {
                        return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
                    }
                case PredicateLeaf.Operator.IS_NULL:
                    // min = null condition above handles the all-nulls YES case
                    return hasNull ? TruthValue.YES_NO : TruthValue.NO;
                default:
                    return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
            }
        }

        private static TruthValue evaluatePredicateBloomFilter(PredicateLeaf predicate,
            object predObj, BloomFilter bloomFilter, bool hasNull)
        {
            switch (predicate.getOperator())
            {
                case PredicateLeaf.Operator.NULL_SAFE_EQUALS:
                    // null safe equals does not return *_NULL variant. So set hasNull to false
                    return checkInBloomFilter(bloomFilter, predObj, false);
                case PredicateLeaf.Operator.EQUALS:
                    return checkInBloomFilter(bloomFilter, predObj, hasNull);
                case PredicateLeaf.Operator.IN:
                    foreach (object arg in predicate.getLiteralList())
                    {
                        // if atleast one value in IN list exist in bloom filter, qualify the row group/stripe
                        object predObjItem = getBaseObjectForComparison(predicate.getType(), arg);
                        TruthValue result = checkInBloomFilter(bloomFilter, predObjItem, hasNull);
                        if (result == TruthValue.YES_NO_NULL || result == TruthValue.YES_NO)
                        {
                            return result;
                        }
                    }
                    return hasNull ? TruthValue.NO_NULL : TruthValue.NO;
                default:
                    return hasNull ? TruthValue.YES_NO_NULL : TruthValue.YES_NO;
            }
        }

        private static TruthValue checkInBloomFilter(BloomFilter bf, object predObj, bool hasNull)
        {
            TruthValue result = hasNull ? TruthValue.NO_NULL : TruthValue.NO;

            if (predObj is long)
            {
                if (bf.testLong((long)predObj))
                {
                    result = TruthValue.YES_NO_NULL;
                }
            }
            else if (predObj is double)
            {
                if (bf.testDouble((double)predObj))
                {
                    result = TruthValue.YES_NO_NULL;
                }
            }
            else if (predObj is string || predObj is HiveDecimal)
            {
                if (bf.testString(predObj.ToString()))
                {
                    result = TruthValue.YES_NO_NULL;
                }
            }
            else if (predObj is DateTime)
            {
                // Could be date or timestamp
                DateTime dateTime = (DateTime)predObj;
                if (bf.testLong(dateTime.getTimestamp()) || bf.testLong(dateTime.getDays()))
                {
                    result = TruthValue.YES_NO_NULL;
                }
            }
            else
            {
                // if the predicate object is null and if hasNull says there are no nulls then return NO
                if (predObj == null && !hasNull)
                {
                    result = TruthValue.NO;
                }
                else
                {
                    result = TruthValue.YES_NO_NULL;
                }
            }

            if (result == TruthValue.YES_NO_NULL && !hasNull)
            {
                result = TruthValue.YES_NO;
            }

            if (LOG.isDebugEnabled())
            {
                LOG.debug("Bloom filter evaluation: " + result.ToString());
            }

            return result;
        }

        private static object getBaseObjectForComparison(PredicateLeaf.Type type, object obj)
        {
            if (obj == null)
            {
                return null;
            }
            switch (type)
            {
                case PredicateLeaf.Type.BOOLEAN:
                    if (obj is bool)
                    {
                        return obj;
                    }
                    else
                    {
                        // will only be true if the string conversion yields "true", all other values are
                        // considered false
                        return Boolean.Parse(obj.ToString());
                    }
                case PredicateLeaf.Type.DATE:
                    if (obj is DateTime)
                    {
                        return obj;
                    }
                    else if (obj is String)
                    {
                        return DateTime.Parse((String)obj);
                    }
#if false
                    else if (obj is Timestamp)
                    {
                        return DateWritable.timeToDate(((Timestamp)obj).getTime() / 1000L);
                    }
#endif
                    // always string, but prevent the comparison to numbers (are they days/seconds/milliseconds?)
                    break;
                case PredicateLeaf.Type.DECIMAL:
                    if (obj is bool)
                    {
                        return (bool)obj ? HiveDecimal.One : HiveDecimal.Zero;
                    }
                    else if (obj is int)
                    {
                        return new HiveDecimal((int)obj);
                    }
                    else if (obj is long)
                    {
                        return new HiveDecimal(((long)obj));
                    }
                    else if (obj is float || obj is double || obj is string)
                    {
                        return HiveDecimal.Parse(obj.ToString());
                    }
                    else if (obj is HiveDecimal)
                    {
                        return obj;
                    }
                    else if (obj is DateTime)
                    {
#if TODO
                        return new HiveDecimalWritable(
                            new Double(new TimestampWritable((Timestamp)obj).getDouble()).ToString());
#endif
                    }
                    break;
                case PredicateLeaf.Type.FLOAT:
                    if (obj is IConvertible)
                    {
                        // widening conversion
                        return ((IConvertible)obj).ToDouble(CultureInfo.InvariantCulture);
                    }
                    else if (obj is HiveDecimal)
                    {
                        return ((HiveDecimal)obj).doubleValue();
                    }
                    else if (obj is DateTime)
                    {
#if TODO
                        return new TimestampWritable((Timestamp)obj).getDouble();
#endif
                    }
                    else if (obj is HiveDecimal)
                    {
                        return ((HiveDecimal)obj).doubleValue();
                    }
                    break;
                case PredicateLeaf.Type.LONG:
                    if (obj is IConvertible)
                    {
                        // widening conversion
                        return ((IConvertible)obj).ToInt64(CultureInfo.InvariantCulture);
                    }
                    else if (obj is HiveDecimal)
                    {
                        return ((HiveDecimal)obj).longValue();
                    }
                    break;
                case PredicateLeaf.Type.STRING:
                    if (obj != null)
                    {
                        return (obj.ToString());
                    }
                    break;
                case PredicateLeaf.Type.TIMESTAMP:
                    if (obj is DateTime)
                    {
                        return obj;
                    }
#if TODO
                    else if (obj is int)
                    {
                        return TimestampWritable.longToTimestamp((int)obj, false);
                    }
                    else if (obj is float)
                    {
                        return TimestampWritable.doubleToTimestamp((float)obj);
                    }
                    else if (obj is double)
                    {
                        return TimestampWritable.doubleToTimestamp((double)obj);
                    }
                    else if (obj is HiveDecimal)
                    {
                        return TimestampWritable.decimalToTimestamp((HiveDecimal)obj);
                    }
                    else if (obj is HiveDecimalWritable)
                    {
                        return TimestampWritable.decimalToTimestamp(((HiveDecimalWritable)obj).getHiveDecimal());
                    }
                    else if (obj is Date)
                    {
                        return new Timestamp(((Date)obj).getTime());
                    }
#endif
                    // float/double conversion to timestamp is interpreted as seconds whereas integer conversion
                    // to timestamp is interpreted as milliseconds by default. The integer to timestamp casting
                    // is also config driven. The filter operator changes its promotion based on config:
                    // "int.timestamp.conversion.in.seconds". Disable PPD for integer cases.
                    break;
                default:
                    break;
            }

            throw new ArgumentException(String.Format(
                "ORC SARGS could not convert from {0} to {1}", obj == null ? "(null)" : obj.GetType().Name, type));
        }

        public class SargApplier
        {
            public static readonly bool[] READ_ALL_RGS = null;
            public static readonly bool[] READ_NO_RGS = new bool[0];

            private SearchArgument sarg;
            private List<PredicateLeaf> sargLeaves;
            private int[] filterColumns;
            private long rowIndexStride;
            // same as the above array, but indices are set to true
            internal bool[] sargColumns;

            public SargApplier(SearchArgument sarg, string[] columnNames, long rowIndexStride,
                IList<OrcProto.Type> types, int includedCount)
            {
                this.sarg = sarg;
                sargLeaves = sarg.getLeaves();
                filterColumns = mapSargColumnsToOrcInternalColIdx(sargLeaves, columnNames, 0);
                this.rowIndexStride = rowIndexStride;
                // included will not be null, row options will fill the array with trues if null
                sargColumns = new bool[includedCount];
                foreach (int i in filterColumns)
                {
                    // filter columns may have -1 as index which could be partition column in SARG.
                    if (i > 0)
                    {
                        sargColumns[i] = true;
                    }
                }
            }

            /**
             * Pick the row groups that we need to load from the current stripe.
             *
             * @return an array with a bool for each row group or null if all of the
             * row groups must be read.
             * @
             */
            public bool[] pickRowGroups(StripeInformation stripe, OrcProto.RowIndex[] indexes,
                OrcProto.BloomFilterIndex[] bloomFilterIndices, bool returnNone)
            {
                long rowsInStripe = stripe.getNumberOfRows();
                int groupsInStripe = (int)((rowsInStripe + rowIndexStride - 1) / rowIndexStride);
                bool[] result = new bool[groupsInStripe]; // TODO: avoid alloc?
                TruthValue[] leafValues = new TruthValue[sargLeaves.Count];
                bool hasSelected = false, hasSkipped = false;
                for (int rowGroup = 0; rowGroup < result.Length; ++rowGroup)
                {
                    for (int pred = 0; pred < leafValues.Length; ++pred)
                    {
                        int columnIx = filterColumns[pred];
                        if (columnIx != -1)
                        {
                            if (indexes[columnIx] == null)
                            {
                                throw new InvalidDataException("Index is not populated for " + columnIx);
                            }
                            OrcProto.RowIndexEntry entry = indexes[columnIx].EntryList[rowGroup];
                            if (entry == null)
                            {
                                throw new InvalidDataException("RG is not populated for " + columnIx + " rg " + rowGroup);
                            }
                            OrcProto.ColumnStatistics stats = entry.Statistics;
                            OrcProto.BloomFilter bf = null;
                            if (bloomFilterIndices != null && bloomFilterIndices[filterColumns[pred]] != null)
                            {
                                bf = bloomFilterIndices[filterColumns[pred]].BloomFilterList[rowGroup];
                            }
                            leafValues[pred] = evaluatePredicateProto(stats, sargLeaves[pred], bf);
                            if (LOG.isTraceEnabled())
                            {
                                LOG.trace("Stats = " + stats);
                                LOG.trace("Setting " + sargLeaves[pred] + " to " + leafValues[pred]);
                            }
                        }
                        else
                        {
                            // the column is a virtual column
                            leafValues[pred] = TruthValue.YES_NO_NULL;
                        }
                    }
                    result[rowGroup] = sarg.evaluate(leafValues).isNeeded();
                    hasSelected = hasSelected || result[rowGroup];
                    hasSkipped = hasSkipped || (!result[rowGroup]);
                    if (LOG.isDebugEnabled())
                    {
                        LOG.debug("Row group " + (rowIndexStride * rowGroup) + " to " +
                            (rowIndexStride * (rowGroup + 1) - 1) + " is " +
                            (result[rowGroup] ? "" : "not ") + "included.");
                    }
                }

                return hasSkipped ? ((hasSelected || !returnNone) ? result : READ_NO_RGS) : READ_ALL_RGS;
            }
        }

        /**
         * Pick the row groups that we need to load from the current stripe.
         *
         * @return an array with a bool for each row group or null if all of the
         * row groups must be read.
         * @
         */
        protected bool[] pickRowGroups()
        {
            // if we don't have a sarg or indexes, we read everything
            if (sargApp == null)
            {
                return null;
            }
            readRowIndex(currentStripe, included, sargApp.sargColumns);
            return sargApp.pickRowGroups(stripes[currentStripe], indexes, bloomFilterIndices, false);
        }

        private void clearStreams()
        {
            // explicit close of all streams to de-ref ByteBuffers
            foreach (InStream @is in streams.Values)
            {
                @is.Close();
            }
            if (bufferChunks != null)
            {
                if (dataReader.isTrackingDiskRanges())
                {
                    for (DiskRangeList range = bufferChunks; range != null; range = range.next)
                    {
                        if (!(range is BufferChunk))
                        {
                            continue;
                        }
                        dataReader.releaseBuffer(((BufferChunk)range).chunk);
                    }
                }
            }
            bufferChunks = null;
            streams.Clear();
        }

        /**
         * Read the current stripe into memory.
         *
         * @
         */
        private void readStripe()
        {
            StripeInformation stripe = beginReadStripe();
            includedRowGroups = pickRowGroups();

            // move forward to the first unskipped row
            if (includedRowGroups != null)
            {
                while (rowInStripe < rowCountInStripe &&
                    !includedRowGroups[(int)(rowInStripe / rowIndexStride)])
                {
                    rowInStripe = Math.Min(rowCountInStripe, rowInStripe + rowIndexStride);
                }
            }

            // if we haven't skipped the whole stripe, read the data
            if (rowInStripe < rowCountInStripe)
            {
                // if we aren't projecting columns or filtering rows, just read it all
                if (included == null && includedRowGroups == null)
                {
                    readAllDataStreams(stripe);
                }
                else
                {
                    readPartialDataStreams(stripe);
                }
                reader.startStripe(streams, stripeFooter);
                // if we skipped the first row group, move the pointers forward
                if (rowInStripe != 0)
                {
                    seekToRowEntry(reader, (int)(rowInStripe / rowIndexStride));
                }
            }
        }

        private StripeInformation beginReadStripe()
        {
            StripeInformation stripe = stripes[currentStripe];
            stripeFooter = readStripeFooter(stripe);
            clearStreams();
            // setup the position in the stripe
            rowCountInStripe = stripe.getNumberOfRows();
            rowInStripe = 0;
            rowBaseInStripe = 0;
            for (int i = 0; i < currentStripe; ++i)
            {
                rowBaseInStripe += stripes[i].getNumberOfRows();
            }
            // reset all of the indexes
            for (int i = 0; i < indexes.Length; ++i)
            {
                indexes[i] = null;
            }
            return stripe;
        }

        private void readAllDataStreams(StripeInformation stripe)
        {
            long start = stripe.getIndexLength();
            long end = start + stripe.getDataLength();
            // explicitly trigger 1 big read
            DiskRangeList toRead = new DiskRangeList(start, end);
            bufferChunks = dataReader.readFileData(toRead, stripe.getOffset(), false);
            createStreams(stripeFooter.StreamsList, bufferChunks, null, codec, bufferSize, streams);
        }

        /**
         * The sections of stripe that we have read.
         * This might not match diskRange - 1 disk range can be multiple buffer chunks, depending on DFS block boundaries.
         */
        public class BufferChunk : DiskRangeList
        {
            internal ByteBuffer chunk;

            public BufferChunk(ByteBuffer chunk, long offset)
                : base(offset, offset + chunk.remaining())
            {
                this.chunk = chunk;
            }

            public ByteBuffer getChunk()
            {
                return chunk;
            }

            public override bool hasData()
            {
                return chunk != null;
            }

            public override string ToString()
            {
                bool makesSense = chunk.remaining() == (end - offset);
                return "data range [" + offset + ", " + end + "), size: " + chunk.remaining()
                    + (makesSense ? "" : "(!)") + " type: " + (chunk.isDirect() ? "direct" : "array-backed");
            }

            public override DiskRange sliceAndShift(long offset, long end, long shiftBy)
            {
                Debug.Assert(offset <= end && offset >= this.offset && end <= this.end);
                Debug.Assert(offset + shiftBy >= 0);
                ByteBuffer sliceBuf = chunk.slice();
                int newPos = (int)(offset - this.offset);
                int newLimit = newPos + (int)(end - offset);
                try
                {
                    sliceBuf.position(newPos);
                    sliceBuf.limit(newLimit);
                }
                catch (Exception t)
                {
                    LOG.error("Failed to slice buffer chunk with range" + " [" + this.offset + ", " + this.end
                        + "), position: " + chunk.position() + " limit: " + chunk.limit() + ", "
                        + (chunk.isDirect() ? "direct" : "array") + "; to [" + offset + ", " + end + ") "
                        + t.GetType());
                    throw;
                }
                return new BufferChunk(sliceBuf, offset + shiftBy);
            }

            public override ByteBuffer getData()
            {
                return chunk;
            }
        }

        /**
         * Plan the ranges of the file that we need to read given the list of
         * columns and row groups.
         *
         * @param streamList        the list of streams available
         * @param indexes           the indexes that have been loaded
         * @param includedColumns   which columns are needed
         * @param includedRowGroups which row groups are needed
         * @param isCompressed      does the file have generic compression
         * @param encodings         the encodings for each column
         * @param types             the types of the columns
         * @param compressionSize   the compression block size
         * @return the list of disk ranges that will be loaded
         */
        internal static DiskRangeList planReadPartialDataStreams(
            IList<OrcProto.Stream> streamList,
            OrcProto.RowIndex[] indexes,
            bool[] includedColumns,
            bool[] includedRowGroups,
            bool isCompressed,
            IList<OrcProto.ColumnEncoding> encodings,
            IList<OrcProto.Type> types,
            int compressionSize,
            bool doMergeBuffers)
        {
            long offset = 0;
            // figure out which columns have a present stream
            bool[] hasNull = RecordReaderUtils.findPresentStreamsByColumn(streamList, types);
            DiskRangeList.CreateHelper list = new DiskRangeList.CreateHelper();
            foreach (OrcProto.Stream stream in streamList)
            {
                long length = (long)stream.Length;
                int column = (int)stream.Column;
                OrcProto.Stream.Types.Kind streamKind = stream.Kind;
                // since stream kind is optional, first check if it exists
                if (stream.HasKind &&
                    (StreamName.getArea(streamKind) == StreamName.Area.DATA) &&
                    includedColumns[column])
                {
                    // if we aren't filtering or it is a dictionary, load it.
                    if (includedRowGroups == null
                        || RecordReaderUtils.isDictionary(streamKind, encodings[column]))
                    {
                        RecordReaderUtils.addEntireStreamToRanges(offset, length, list, doMergeBuffers);
                    }
                    else
                    {
                        RecordReaderUtils.addRgFilteredStreamToRanges(stream, includedRowGroups,
                            isCompressed, indexes[column], encodings[column], types[column],
                            compressionSize, hasNull[column], offset, length, list, doMergeBuffers);
                    }
                }
                offset += length;
            }
            return list.extract();
        }

        void createStreams(IList<OrcProto.Stream> streamDescriptions,
            DiskRangeList ranges,
            bool[] includeColumn,
            CompressionCodec codec,
            int bufferSize,
            Dictionary<StreamName, InStream> streams)
        {
            long streamOffset = 0;
            foreach (OrcProto.Stream streamDesc in streamDescriptions)
            {
                int column = (int)streamDesc.Column;
                long length = (long)streamDesc.Length;
                if ((includeColumn != null && !includeColumn[column]) ||
                    streamDesc.HasKind &&
                        (StreamName.getArea(streamDesc.Kind) != StreamName.Area.DATA))
                {
                    streamOffset += length;
                    continue;
                }
                List<DiskRange> buffers = RecordReaderUtils.getStreamBuffers(
                    ranges, streamOffset, length);
                StreamName name = new StreamName(column, streamDesc.Kind);
                streams[name] = InStream.create(null, name.ToString(), buffers,
                    length, codec, bufferSize);
                streamOffset += length;
            }
        }

        private void readPartialDataStreams(StripeInformation stripe)
        {
            IList<OrcProto.Stream> streamList = stripeFooter.StreamsList;
            DiskRangeList toRead = planReadPartialDataStreams(streamList,
                indexes, included, includedRowGroups, codec != null,
                stripeFooter.ColumnsList, types, bufferSize, true);
            if (LOG.isDebugEnabled())
            {
                LOG.debug("chunks = " + RecordReaderUtils.stringifyDiskRanges(toRead));
            }
            bufferChunks = dataReader.readFileData(toRead, stripe.getOffset(), false);
            if (LOG.isDebugEnabled())
            {
                LOG.debug("merge = " + RecordReaderUtils.stringifyDiskRanges(bufferChunks));
            }

            createStreams(streamList, bufferChunks, included, codec, bufferSize, streams);
        }

        public bool hasNext()
        {
            return rowInStripe < rowCountInStripe;
        }

        /**
         * Read the next stripe until we find a row that we don't skip.
         *
         * @
         */
        private void advanceStripe()
        {
            rowInStripe = rowCountInStripe;
            while (rowInStripe >= rowCountInStripe &&
                currentStripe < stripes.Count - 1)
            {
                currentStripe += 1;
                readStripe();
            }
        }

        /**
         * Skip over rows that we aren't selecting, so that the next row is
         * one that we will read.
         *
         * @param nextRow the row we want to go to
         * @
         */
        private bool advanceToNextRow(
            TreeReaderFactory.TreeReader reader, long nextRow, bool canAdvanceStripe)
        {
            long nextRowInStripe = nextRow - rowBaseInStripe;
            // check for row skipping
            if (rowIndexStride != 0 &&
                includedRowGroups != null &&
                nextRowInStripe < rowCountInStripe)
            {
                int rowGroup = (int)(nextRowInStripe / rowIndexStride);
                if (!includedRowGroups[rowGroup])
                {
                    while (rowGroup < includedRowGroups.Length && !includedRowGroups[rowGroup])
                    {
                        rowGroup += 1;
                    }
                    if (rowGroup >= includedRowGroups.Length)
                    {
                        if (canAdvanceStripe)
                        {
                            advanceStripe();
                        }
                        return canAdvanceStripe;
                    }
                    nextRowInStripe = Math.Min(rowCountInStripe, rowGroup * rowIndexStride);
                }
            }
            if (nextRowInStripe >= rowCountInStripe)
            {
                if (canAdvanceStripe)
                {
                    advanceStripe();
                }
                return canAdvanceStripe;
            }
            if (nextRowInStripe != rowInStripe)
            {
                if (rowIndexStride != 0)
                {
                    int rowGroup = (int)(nextRowInStripe / rowIndexStride);
                    seekToRowEntry(reader, rowGroup);
                    reader.skipRows(nextRowInStripe - rowGroup * rowIndexStride);
                }
                else
                {
                    reader.skipRows(nextRowInStripe - rowInStripe);
                }
                rowInStripe = nextRowInStripe;
            }
            return true;
        }

        public object next(object previous)
        {
            try
            {
                object result = reader.next(previous);
                // find the next row
                rowInStripe++;
                advanceToNextRow(reader, rowInStripe + rowBaseInStripe, true);
                return result;
            }
            catch (IOException e)
            {
                // Rethrow exception with file name in log message
                throw new IOException("Error reading file: " + path, e);
            }
        }

        public VectorizedRowBatch nextBatch(VectorizedRowBatch previous)
        {
            try
            {
                VectorizedRowBatch result;
                if (rowInStripe >= rowCountInStripe)
                {
                    currentStripe += 1;
                    readStripe();
                }

                long batchSize = computeBatchSize(VectorizedRowBatch.DEFAULT_SIZE);

                rowInStripe += batchSize;
                if (previous == null)
                {
                    ColumnVector[] cols = (ColumnVector[])reader.nextVector(null, (int)batchSize);
                    result = new VectorizedRowBatch(cols.Length);
                    result.cols = cols;
                }
                else
                {
                    result = (VectorizedRowBatch)previous;
                    result.selectedInUse = false;
                    reader.nextVector(result.cols, (int)batchSize);
                }

                result.size = (int)batchSize;
                advanceToNextRow(reader, rowInStripe + rowBaseInStripe, true);
                return result;
            }
            catch (IOException e)
            {
                // Rethrow exception with file name in log message
                throw new IOException("Error reading file: " + path, e);
            }
        }

        private long computeBatchSize(long targetBatchSize)
        {
            long batchSize = 0;
            // In case of PPD, batch size should be aware of row group boundaries. If only a subset of row
            // groups are selected then marker position is set to the end of range (subset of row groups
            // within strip). Batch size computed out of marker position makes sure that batch size is
            // aware of row group boundary and will not cause overflow when reading rows
            // illustration of this case is here https://issues.apache.org/jira/browse/HIVE-6287
            if (rowIndexStride != 0 && includedRowGroups != null && rowInStripe < rowCountInStripe)
            {
                int startRowGroup = (int)(rowInStripe / rowIndexStride);
                if (!includedRowGroups[startRowGroup])
                {
                    while (startRowGroup < includedRowGroups.Length && !includedRowGroups[startRowGroup])
                    {
                        startRowGroup += 1;
                    }
                }

                int endRowGroup = startRowGroup;
                while (endRowGroup < includedRowGroups.Length && includedRowGroups[endRowGroup])
                {
                    endRowGroup += 1;
                }

                long markerPosition =
                    (endRowGroup * rowIndexStride) < rowCountInStripe ? (endRowGroup * rowIndexStride)
                        : rowCountInStripe;
                batchSize = Math.Min(targetBatchSize, (markerPosition - rowInStripe));

                if (isLogDebugEnabled && batchSize < targetBatchSize)
                {
                    LOG.debug("markerPosition: " + markerPosition + " batchSize: " + batchSize);
                }
            }
            else
            {
                batchSize = Math.Min(targetBatchSize, (rowCountInStripe - rowInStripe));
            }
            return batchSize;
        }

        public void close()
        {
            clearStreams();
            dataReader.close();
        }

        public long getRowNumber()
        {
            return rowInStripe + rowBaseInStripe + firstRow;
        }

        /**
         * Return the fraction of rows that have been read from the selected.
         * section of the file
         *
         * @return fraction between 0.0 and 1.0 of rows consumed
         */
        public float getProgress()
        {
            return ((float)rowBaseInStripe + rowInStripe) / totalRowCount;
        }

        private int findStripe(long rowNumber)
        {
            for (int i = 0; i < stripes.Count; i++)
            {
                StripeInformation stripe = stripes[i];
                if (stripe.getNumberOfRows() > rowNumber)
                {
                    return i;
                }
                rowNumber -= stripe.getNumberOfRows();
            }
            throw new ArgumentException("Seek after the end of reader range");
        }

        internal Index readRowIndex(
            int stripeIndex, bool[] included, bool[] sargColumns)
        {
            return readRowIndex(stripeIndex, included, null, null, sargColumns);
        }

        internal Index readRowIndex(int stripeIndex, bool[] included, OrcProto.RowIndex[] indexes,
            OrcProto.BloomFilterIndex[] bloomFilterIndex, bool[] sargColumns)
        {
            StripeInformation stripe = stripes[stripeIndex];
            OrcProto.StripeFooter stripeFooter = null;
            // if this is the current stripe, use the cached objects.
            if (stripeIndex == currentStripe)
            {
                stripeFooter = this.stripeFooter;
                indexes = indexes == null ? this.indexes : indexes;
                bloomFilterIndex = bloomFilterIndex == null ? this.bloomFilterIndices : bloomFilterIndex;
                sargColumns = sargColumns == null ?
                    (sargApp == null ? null : sargApp.sargColumns) : sargColumns;
            }
            return metadata.readRowIndex(stripe, stripeFooter, included, indexes, sargColumns,
                bloomFilterIndex);
        }

        private void seekToRowEntry(TreeReaderFactory.TreeReader reader, int rowEntry)
        {
            PositionProvider[] index = new PositionProvider[indexes.Length];
            for (int i = 0; i < indexes.Length; ++i)
            {
                if (indexes[i] != null)
                {
                    index[i] = new PositionProviderImpl(indexes[i].EntryList[rowEntry]);
                }
            }
            reader.seek(index);
        }

        public void seekToRow(long rowNumber)
        {
            if (rowNumber < 0)
            {
                throw new ArgumentException("Seek to a negative row number " +
                    rowNumber);
            }
            else if (rowNumber < firstRow)
            {
                throw new ArgumentException("Seek before reader range " +
                    rowNumber);
            }
            // convert to our internal form (rows from the beginning of slice)
            rowNumber -= firstRow;

            // move to the right stripe
            int rightStripe = findStripe(rowNumber);
            if (rightStripe != currentStripe)
            {
                currentStripe = rightStripe;
                readStripe();
            }
            readRowIndex(currentStripe, included, sargApp == null ? null : sargApp.sargColumns);

            // if we aren't to the right row yet, advance in the stripe.
            advanceToNextRow(reader, rowNumber, true);
        }

        private const char TRANSLATED_SARG_SEPARATOR = '_';

        public static string encodeTranslatedSargColumn(int rootColumn, int? indexInSourceTable)
        {
            return rootColumn + TRANSLATED_SARG_SEPARATOR
                + ((indexInSourceTable == null) ? -1 : indexInSourceTable.Value).ToString();
        }

        public static int[] mapTranslatedSargColumns(List<OrcProto.Type> types, List<PredicateLeaf> sargLeaves)
        {
            int[] result = new int[sargLeaves.Count];
            OrcProto.Type lastRoot = null; // Root will be the same for everyone as of now.
            string lastRootStr = null;
            for (int i = 0; i < result.Length; ++i)
            {
                string[] rootAndIndex = sargLeaves[i].getColumnName().Split(TRANSLATED_SARG_SEPARATOR);
                Debug.Assert(rootAndIndex.Length == 2);
                string rootStr = rootAndIndex[0], indexStr = rootAndIndex[1];
                int index = Int32.Parse(indexStr);
                // First, check if the column even maps to anything.
                if (index == -1)
                {
                    result[i] = -1;
                    continue;
                }
                Debug.Assert(index >= 0);
                // Then, find the root type if needed.
                if (!rootStr.Equals(lastRootStr))
                {
                    lastRoot = types[Int32.Parse(rootStr)];
                    lastRootStr = rootStr;
                }
                // Subtypes of the root types correspond, in order, to the columns in the table schema
                // (disregarding schema evolution that doesn't presently work). Get the index for the
                // corresponding subtype.
                result[i] = (int)lastRoot.SubtypesList[index];
            }
            return result;
        }
    }
}
