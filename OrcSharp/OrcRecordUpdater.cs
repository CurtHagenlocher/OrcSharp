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
    using System.Text;
    using OrcSharp.External;
    using OrcSharp.Serialization;

    /// <summary>
    /// A RecordUpdater where the files are stored as ORC.
    /// </summary>
    public class OrcRecordUpdater
    {
        private static Log LOG = LogFactory.getLog(typeof(OrcRecordUpdater));

        public const string ACID_KEY_INDEX_NAME = "hive.acid.key.index";
        public const string ACID_FORMAT = "_orc_acid_version";
        public const string ACID_STATS = "hive.acid.stats";
        public const int ORC_ACID_VERSION = 0;

        const int INSERT_OPERATION = 0;
        const int UPDATE_OPERATION = 1;
        const int DELETE_OPERATION = 2;

        internal const int OPERATION = 0;
        internal const int ORIGINAL_TRANSACTION = 1;
        internal const int BUCKET = 2;
        internal const int ROW_ID = 3;
        internal const int CURRENT_TRANSACTION = 4;
        internal const int ROW = 5;
        internal const int FIELDS = 6;

        const int DELTA_BUFFER_SIZE = 16 * 1024;
        const long DELTA_STRIPE_SIZE = 16 * 1024 * 1024;

        private AcidOutputFormat.Options options;
        private Path path;
        private FileSystem fs;
        private Writer writer;
        private FSDataOutputStream flushLengths;
        private OrcStruct item;
        private IntWritable operation = new IntWritable();
        private LongWritable currentTransaction = new LongWritable(-1);
        private LongWritable originalTransaction = new LongWritable(-1);
        private IntWritable bucket = new IntWritable();
        private LongWritable rowId = new LongWritable();
        private long insertedRows = 0;
        private long rowIdOffset = 0;
        // This records how many rows have been inserted or deleted.  It is separate from insertedRows
        // because that is monotonically increasing to give new unique row ids.
        private long rowCountDelta = 0;
        private KeyIndexBuilder indexBuilder = new KeyIndexBuilder();
        private StructField recIdField = null; // field to look for the record identifier in
        private StructField rowIdField = null; // field inside recId to look for row id in
        private StructField originalTxnField = null;  // field inside recId to look for original txn in
        private StructObjectInspector rowInspector; // OI for the original row
        private StructObjectInspector recIdInspector; // OI for the record identifier struct
        private LongObjectInspector rowIdInspector; // OI for the long row id inside the recordIdentifier
        private LongObjectInspector origTxnInspector; // OI for the original txn inside the record
        // identifer

        class AcidStats
        {
            internal long inserts;
            internal long updates;
            internal long deletes;

            public AcidStats()
            {
                // nothing
            }

            AcidStats(string serialized)
            {
                string[] parts = serialized.Split(',');
                inserts = Int64.Parse(parts[0]);
                updates = Int64.Parse(parts[1]);
                deletes = Int64.Parse(parts[2]);
            }

            internal string serialize()
            {
                StringBuilder builder = new StringBuilder();
                builder.Append(inserts);
                builder.Append(",");
                builder.Append(updates);
                builder.Append(",");
                builder.Append(deletes);
                return builder.ToString();
            }

            public override string ToString()
            {
                StringBuilder builder = new StringBuilder();
                builder.Append(" inserts: ").Append(inserts);
                builder.Append(" updates: ").Append(updates);
                builder.Append(" deletes: ").Append(deletes);
                return builder.ToString();
            }
        }

        static Path getSideFile(Path main)
        {
            return new Path(main + AcidUtils.DELTA_SIDE_FILE_SUFFIX);
        }

        internal static int getOperation(OrcStruct @struct)
        {
            return ((IntWritable)@struct.getFieldValue(OPERATION)).get();
        }

        internal static long getCurrentTransaction(OrcStruct @struct)
        {
            return ((LongWritable)@struct.getFieldValue(CURRENT_TRANSACTION)).get();
        }

        internal static long getOriginalTransaction(OrcStruct @struct)
        {
            return ((LongWritable)@struct.getFieldValue(ORIGINAL_TRANSACTION)).get();
        }

        internal static int getBucket(OrcStruct @struct)
        {
            return ((IntWritable)@struct.getFieldValue(BUCKET)).get();
        }

        internal static long getRowId(OrcStruct @struct)
        {
            return ((LongWritable)@struct.getFieldValue(ROW_ID)).get();
        }

        static OrcStruct getRow(OrcStruct @struct)
        {
            if (@struct == null)
            {
                return null;
            }
            else
            {
                return (OrcStruct)@struct.getFieldValue(ROW);
            }
        }

        /**
         * An extension to AcidOutputFormat that allows users to add additional
         * options.
         */
        public class OrcOptions : AcidOutputFormat.Options
        {
            OrcFile.WriterOptions orcOptions = null;

            public OrcOptions(Configuration conf)
                : base(conf)
            {
            }

            public OrcOptions orcOptions(OrcFile.WriterOptions opts)
            {
                this.orcOptions = opts;
                return this;
            }

            public OrcFile.WriterOptions getOrcOptions()
            {
                return orcOptions;
            }
        }

        /**
         * Create an object inspector for the ACID event based on the object inspector
         * for the underlying row.
         * @param rowInspector the row's object inspector
         * @return an object inspector for the event stream
         */
        static StructObjectInspector createEventSchema(ObjectInspector rowInspector)
        {
            List<StructField> fields = new List<StructField>();
            fields.Add(new OrcStruct.Field("operation",
                PrimitiveObjectInspectorFactory.writableIntObjectInspector, OPERATION));
            fields.Add(new OrcStruct.Field("originalTransaction",
                PrimitiveObjectInspectorFactory.writableLongObjectInspector,
                ORIGINAL_TRANSACTION));
            fields.Add(new OrcStruct.Field("bucket",
                PrimitiveObjectInspectorFactory.writableIntObjectInspector, BUCKET));
            fields.Add(new OrcStruct.Field("rowId",
                PrimitiveObjectInspectorFactory.writableLongObjectInspector, ROW_ID));
            fields.Add(new OrcStruct.Field("currentTransaction",
                PrimitiveObjectInspectorFactory.writableLongObjectInspector,
                CURRENT_TRANSACTION));
            fields.Add(new OrcStruct.Field("row", rowInspector, ROW));
            return new OrcStruct.OrcStructInspector(fields);
        }

        OrcRecordUpdater(Path path,
                         AcidOutputFormat.Options options)
        {
            this.options = options;
            this.bucket.set(options.getBucket());
            this.path = AcidUtils.createFilename(path, options);
            FileSystem fs = options.getFilesystem();
            if (fs == null)
            {
                fs = path.getFileSystem(options.getConfiguration());
            }
            this.fs = fs;
            try
            {
                FSDataOutputStream strm = fs.create(new Path(path, ACID_FORMAT), false);
                strm.writeInt(ORC_ACID_VERSION);
                strm.close();
            }
            catch (IOException ioe)
            {
                if (LOG.isDebugEnabled())
                {
                    LOG.debug("Failed to create " + path + "/" + ACID_FORMAT + " with " +
                        ioe);
                }
            }
            if (options.getMinimumTransactionId() != options.getMaximumTransactionId()
                && !options.isWritingBase())
            {
                flushLengths = fs.create(getSideFile(this.path), true, 8,
                    options.getReporter());
            }
            else
            {
                flushLengths = null;
            }
            OrcFile.WriterOptions writerOptions = null;
            if (options is OrcOptions)
            {
                writerOptions = ((OrcOptions)options).getOrcOptions();
            }
            if (writerOptions == null)
            {
                writerOptions = OrcFile.writerOptions( /* options.getTableProperties(), */
                    options.getConfiguration());
            }
            writerOptions.fileSystem(fs).callback(indexBuilder);
            if (!options.isWritingBase())
            {
                writerOptions.blockPadding(false);
                writerOptions.bufferSize(DELTA_BUFFER_SIZE);
                writerOptions.stripeSize(DELTA_STRIPE_SIZE);
            }
            rowInspector = (StructObjectInspector)options.getInspector();
            writerOptions.inspector(createEventSchema(findRecId(options.getInspector(),
                options.getRecordIdColumn())));
            this.writer = OrcFile.createWriter(this.path, writerOptions);
            item = new OrcStruct(FIELDS);
            item.setFieldValue(OPERATION, operation);
            item.setFieldValue(CURRENT_TRANSACTION, currentTransaction);
            item.setFieldValue(ORIGINAL_TRANSACTION, originalTransaction);
            item.setFieldValue(BUCKET, bucket);
            item.setFieldValue(ROW_ID, rowId);
        }

        public override String ToString()
        {
            return GetType().Name + "[" + path + "]";
        }

        /**
         * To handle multiple INSERT... statements in a single transaction, we want to make sure
         * to generate unique {@code rowId} for all inserted rows of the transaction.
         * @return largest rowId created by previous statements (maybe 0)
         * @
         */
        private long findRowIdOffsetForInsert()
        {
            /*
            * 1. need to know bucket we are writing to
            * 2. need to know which delta dir it's in
            * Then,
            * 1. find the same bucket file in previous delta dir for this txn
            * 2. read the footer and get AcidStats which has insert count
             * 2.1 if AcidStats.inserts>0 done
             *  else go to previous delta file
             *  For example, consider insert/update/insert case...*/
            if (options.getStatementId() <= 0)
            {
                return 0;//there is only 1 statement in this transaction (so far)
            }
            for (int pastStmt = options.getStatementId() - 1; pastStmt >= 0; pastStmt--)
            {
                Path matchingBucket = AcidUtils.createFilename(options.getFinalDestination(), options.clone().statementId(pastStmt));
                if (!fs.exists(matchingBucket))
                {
                    continue;
                }
                Reader reader = OrcFile.createReader(matchingBucket, OrcFile.readerOptions(options.getConfiguration()));
                //no close() on Reader?!
                AcidStats acidStats = parseAcidStats(reader);
                if (acidStats.inserts > 0)
                {
                    return acidStats.inserts;
                }
            }
            //if we got here, we looked at all delta files in this txn, prior to current statement and didn't 
            //find any inserts...
            return 0;
        }
        // Find the record identifier column (if there) and return a possibly new ObjectInspector that
        // will strain out the record id for the underlying writer.
        private ObjectInspector findRecId(ObjectInspector inspector, int rowIdColNum)
        {
            if (!(inspector is StructObjectInspector))
            {
                throw new InvalidOperationException("Serious problem, expected a StructObjectInspector, but got a " +
                    inspector.GetType().FullName);
            }
            if (rowIdColNum < 0)
            {
                return inspector;
            }
            else
            {
                RecIdStrippingObjectInspector newInspector =
                    new RecIdStrippingObjectInspector(inspector, rowIdColNum);
                recIdField = newInspector.getRecId();
                List<StructField> fields =
                    ((StructObjectInspector)recIdField.getFieldObjectInspector()).getAllStructFieldRefs();
                // Go by position, not field name, as field names aren't guaranteed.  The order of fields
                // in RecordIdentifier is transactionId, bucketId, rowId
                originalTxnField = fields[0];
                origTxnInspector = (LongObjectInspector)originalTxnField.getFieldObjectInspector();
                rowIdField = fields[2];
                rowIdInspector = (LongObjectInspector)rowIdField.getFieldObjectInspector();


                recIdInspector = (StructObjectInspector)recIdField.getFieldObjectInspector();
                return newInspector;
            }
        }

        private void addEvent(int operation, long currentTransaction, long rowId, Object row)
        {
            this.operation.set(operation);
            this.currentTransaction.set(currentTransaction);
            // If this is an insert, originalTransaction should be set to this transaction.  If not,
            // it will be reset by the following if anyway.
            long originalTransaction = currentTransaction;
            if (operation == DELETE_OPERATION || operation == UPDATE_OPERATION)
            {
                Object rowIdValue = rowInspector.getStructFieldData(row, recIdField);
                originalTransaction = origTxnInspector.get(
                    recIdInspector.getStructFieldData(rowIdValue, originalTxnField));
                rowId = rowIdInspector.get(recIdInspector.getStructFieldData(rowIdValue, rowIdField));
            }
            else if (operation == INSERT_OPERATION)
            {
                rowId += rowIdOffset;
            }
            this.rowId.set(rowId);
            this.originalTransaction.set(originalTransaction);
            item.setFieldValue(OrcRecordUpdater.ROW, (operation == DELETE_OPERATION ? null : row));
            indexBuilder.addKey(operation, originalTransaction, bucket.get(), rowId);
            writer.addRow(item);
        }

        public void insert(long currentTransaction, Object row)
        {
            if (this.currentTransaction.get() != currentTransaction)
            {
                insertedRows = 0;
                //this method is almost no-op in hcatalog.streaming case since statementId == 0 is
                //always true in that case
                rowIdOffset = findRowIdOffsetForInsert();
            }
            addEvent(INSERT_OPERATION, currentTransaction, insertedRows++, row);
            rowCountDelta++;
        }

        public void update(long currentTransaction, Object row)
        {
            if (this.currentTransaction.get() != currentTransaction)
            {
                insertedRows = 0;
            }
            addEvent(UPDATE_OPERATION, currentTransaction, -1L, row);
        }

        public void delete(long currentTransaction, Object row)
        {
            if (this.currentTransaction.get() != currentTransaction)
            {
                insertedRows = 0;
            }
            addEvent(DELETE_OPERATION, currentTransaction, -1, row);
            rowCountDelta--;

        }

        public void flush()
        {
            // We only support flushes on files with multiple transactions, because
            // flushes create significant overhead in HDFS. Record updaters with a
            // single transaction should be closed rather than flushed.
            if (flushLengths == null)
            {
                throw new InvalidOperationException("Attempting to flush a RecordUpdater on "
                   + path + " with a single transaction.");
            }
            long len = writer.writeIntermediateFooter();
            flushLengths.writeLong(len);
            OrcInputFormat.SHIMS.hflush(flushLengths);
        }

        public void close(bool abort)
        {
            if (abort)
            {
                if (flushLengths == null)
                {
                    fs.delete(path, false);
                }
            }
            else
            {
                if (writer != null) writer.close();
            }
            if (flushLengths != null)
            {
                flushLengths.close();
                fs.delete(getSideFile(path), false);
            }
            writer = null;
        }

        public SerDeStats getStats()
        {
            SerDeStats stats = new SerDeStats();
            stats.setRowCount(rowCountDelta);
            // Don't worry about setting raw data size diff.  I have no idea how to calculate that
            // without finding the row we are updating or deleting, which would be a mess.
            return stats;
        }

        Writer getWriter()
        {
            return writer;
        }

        static RecordIdentifier[] parseKeyIndex(Reader reader)
        {
            String[] stripes;
            try
            {
                ByteBuffer val =
                    reader.getMetadataValue(OrcRecordUpdater.ACID_KEY_INDEX_NAME)
                        .duplicate();
                stripes = utf8Decoder.decode(val).toString().split(";");
            }
            catch (CharacterCodingException e)
            {
                throw new ArgumentException("Bad string encoding for " +
                    OrcRecordUpdater.ACID_KEY_INDEX_NAME, e);
            }
            RecordIdentifier[] result = new RecordIdentifier[stripes.length];
            for (int i = 0; i < stripes.length; ++i)
            {
                if (stripes[i].length() != 0)
                {
                    String[] parts = stripes[i].split(",");
                    result[i] = new RecordIdentifier();
                    result[i].setValues(Long.parseLong(parts[0]),
                        Integer.parseInt(parts[1]), Long.parseLong(parts[2]));
                }
            }
            return result;
        }
        /**
         * {@link KeyIndexBuilder} creates these
         */
        static AcidStats parseAcidStats(Reader reader)
        {
            if (reader.hasMetadataValue(OrcRecordUpdater.ACID_STATS))
            {
                String statsSerialized;
                try
                {
                    ByteBuffer val =
                      reader.getMetadataValue(OrcRecordUpdater.ACID_STATS)
                        .duplicate();
                    statsSerialized = utf8Decoder.decode(val).toString();
                }
                catch (CharacterCodingException e)
                {
                    throw new ArgumentException("Bad string encoding for " +
                      OrcRecordUpdater.ACID_STATS, e);
                }
                return new AcidStats(statsSerialized);
            }
            else
            {
                return null;
            }
        }

        class KeyIndexBuilder : OrcFile.WriterCallback
        {
            StringBuilder lastKey = new StringBuilder();
            long lastTransaction;
            int lastBucket;
            long lastRowId;
            AcidStats acidStats = new AcidStats();

            public void preStripeWrite(Writer writer)
            {
                lastKey.Append(lastTransaction);
                lastKey.Append(',');
                lastKey.Append(lastBucket);
                lastKey.Append(',');
                lastKey.Append(lastRowId);
                lastKey.Append(';');
            }

            public void preFooterWrite(Writer writer)
            {
                writer.addUserMetadata(ACID_KEY_INDEX_NAME,
                    Encoding.UTF8.GetBytes(lastKey.ToString()));
                writer.addUserMetadata(ACID_STATS,
                    Encoding.UTF8.GetBytes(acidStats.serialize()));
            }

            void addKey(int op, long transaction, int bucket, long rowId)
            {
                switch (op)
                {
                    case INSERT_OPERATION:
                        acidStats.inserts += 1;
                        break;
                    case UPDATE_OPERATION:
                        acidStats.updates += 1;
                        break;
                    case DELETE_OPERATION:
                        acidStats.deletes += 1;
                        break;
                    default:
                        throw new ArgumentException("Unknown operation " + op);
                }
                lastTransaction = transaction;
                lastBucket = bucket;
                lastRowId = rowId;
            }
        }

        /**
         * An ObjectInspector that will strip out the record identifier so that the underlying writer
         * doesn't see it.
         */
        private class RecIdStrippingObjectInspector : StructObjectInspector
        {
            private StructObjectInspector wrapped;
            List<StructField> fields;
            StructField recId;

            public RecIdStrippingObjectInspector(ObjectInspector oi, int rowIdColNum)
            {
                if (!(oi is StructObjectInspector))
                {
                    throw new InvalidOperationException("Serious problem, expected a StructObjectInspector, " +
                        "but got a " + oi.GetType().Name);
                }
                wrapped = (StructObjectInspector)oi;
                IList<StructField> wrappedFields = wrapped.getAllStructFieldRefs();
                fields = new List<StructField>(wrapped.getAllStructFieldRefs().Count);
                for (int i = 0; i < wrappedFields.Count; i++)
                {
                    if (i == rowIdColNum)
                    {
                        recId = wrappedFields[i];
                    }
                    else
                    {
                        fields.Add(wrappedFields[i]);
                    }
                }
            }

            public override IList<StructField> getAllStructFieldRefs()
            {
                return fields;
            }

            public override StructField getStructFieldRef(string fieldName)
            {
                return wrapped.getStructFieldRef(fieldName);
            }

            public override object getStructFieldData(object data, StructField fieldRef)
            {
                // For performance don't check that that the fieldRef isn't recId everytime,
                // just assume that the caller used getAllStructFieldRefs and thus doesn't have that fieldRef
                return wrapped.getStructFieldData(data, fieldRef);
            }

            public override List<object> getStructFieldsDataAsList(object data)
            {
                return wrapped.getStructFieldsDataAsList(data);
            }

            public override string getTypeName()
            {
                return wrapped.getTypeName();
            }

            public override ObjectInspectorCategory getCategory()
            {
                return wrapped.getCategory();
            }

            StructField getRecId()
            {
                return recId;
            }
        }
    }
}
