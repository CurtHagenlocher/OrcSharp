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
    using System.IO;
    using System.Numerics;
    using System.Text;
    using OrcSharp.External;
    using OrcSharp.Types;
    using OrcProto = global::orc.proto;

    /**
     * Factory for creating ORC tree readers.
     */
    public class TreeReaderFactory
    {
        internal static Func<TimeZoneInfo> CreateTimeZone;
        private static readonly Logger LOG = LoggerFactory.getLog(typeof(TreeReaderFactory));

        public class TreeReaderSchema
        {
            /**
             * The types in the ORC file.
             */
            IList<OrcProto.Type> _fileTypes;

            /**
             * The treeReaderSchema that the reader should read as.
             */
            IList<OrcProto.Type> _schemaTypes;

            /**
             * The subtype of the row STRUCT.  Different than 0 for ACID.
             */
            int _innerStructSubtype;

            public TreeReaderSchema()
            {
                _fileTypes = null;
                _schemaTypes = null;
                _innerStructSubtype = -1;
            }

            public TreeReaderSchema fileTypes(IList<OrcProto.Type> fileTypes)
            {
                this._fileTypes = fileTypes;
                return this;
            }

            public TreeReaderSchema schemaTypes(IList<OrcProto.Type> schemaTypes)
            {
                this._schemaTypes = schemaTypes;
                return this;
            }

            public TreeReaderSchema innerStructSubtype(int innerStructSubtype)
            {
                this._innerStructSubtype = innerStructSubtype;
                return this;
            }

            public IList<OrcProto.Type> getFileTypes()
            {
                return _fileTypes;
            }

            public IList<OrcProto.Type> getSchemaTypes()
            {
                return _schemaTypes;
            }

            public int getInnerStructSubtype()
            {
                return _innerStructSubtype;
            }
        }

        public interface TreeReader
        {
            void startStripe(Dictionary<StreamName, InStream> streams, OrcProto.StripeFooter stripeFooter);
            void seek(PositionProvider[] index);
            object next();
            object nextVector(object previousVector, long batchSize);
            void skipRows(long rows);
            void setVectorColumnCount(int vectorColumnCount);
        }

        public abstract class TreeReader<T> : TreeReader
        {
            protected int columnId;
            protected BitFieldReader present = null;
            private bool valuePresent = false;
            protected int vectorColumnCount;

            protected TreeReader(int columnId, InStream @in = null)
            {
                this.columnId = columnId;
                if (@in == null)
                {
                    present = null;
                    valuePresent = true;
                }
                else
                {
                    present = new BitFieldReader(@in, 1);
                }
                vectorColumnCount = -1;
            }

            public void setVectorColumnCount(int vectorColumnCount)
            {
                this.vectorColumnCount = vectorColumnCount;
            }

            public virtual void checkEncoding(OrcProto.ColumnEncoding encoding)
            {
                if (encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT)
                {
                    throw new IOException("Unknown encoding " + encoding + " in column " +
                        columnId);
                }
            }

            public static IntegerReader createIntegerReader(
                OrcProto.ColumnEncoding.Types.Kind kind,
                InStream @in,
                bool signed,
                bool skipCorrupt)
            {
                switch (kind)
                {
                    case OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2:
                    case OrcProto.ColumnEncoding.Types.Kind.DICTIONARY_V2:
                        return new RunLengthIntegerReaderV2(@in, signed, skipCorrupt);
                    case OrcProto.ColumnEncoding.Types.Kind.DIRECT:
                    case OrcProto.ColumnEncoding.Types.Kind.DICTIONARY:
                        return new RunLengthIntegerReader(@in, signed);
                    default:
                        throw new ArgumentException("Unknown encoding " + kind);
                }
            }

            public virtual void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter)
            {
                checkEncoding(stripeFooter.ColumnsList[columnId]);
                InStream @in = streams.get(new StreamName(columnId,
                    OrcProto.Stream.Types.Kind.PRESENT));
                if (@in == null)
                {
                    present = null;
                    valuePresent = true;
                }
                else
                {
                    present = new BitFieldReader(@in, 1);
                }
            }

            /**
             * Seek to the given position.
             *
             * @param index the indexes loaded from the file
             * @
             */
            public virtual void seek(PositionProvider[] index)
            {
                seek(index[columnId]);
            }

            public virtual void seek(PositionProvider index)
            {
                if (present != null)
                {
                    present.seek(index);
                }
            }

            protected long countNonNulls(long rows)
            {
                if (present != null)
                {
                    long result = 0;
                    for (long c = 0; c < rows; ++c)
                    {
                        if (present.next() == 1)
                        {
                            result += 1;
                        }
                    }
                    return result;
                }
                else
                {
                    return rows;
                }
            }

            public abstract void skipRows(long rows);

            public abstract T getNext();

            protected bool hasValue()
            {
                return present == null ? valuePresent : present.next() == 1;
            }

            public object next()
            {
                return getNext();
            }

            /**
             * Populates the isNull vector array in the previousVector object based on
             * the present stream values. This function is called from all the child
             * readers, and they all set the values based on isNull field value.
             *
             * @param previousVector The columnVector object whose isNull value is populated
             * @param batchSize      Size of the column vector
             * @return next column vector
             * @
             */
            public virtual object nextVector(object previousVector, long batchSize)
            {
                ColumnVector result = (ColumnVector)previousVector;
                if (present != null)
                {
                    // Set noNulls and isNull vector of the ColumnVector based on
                    // present stream
                    result.noNulls = true;
                    for (int i = 0; i < batchSize; i++)
                    {
                        result.isNull[i] = (present.next() != 1);
                        if (result.noNulls && result.isNull[i])
                        {
                            result.noNulls = false;
                        }
                    }
                }
                else
                {
                    // There is not present stream, this means that all the values are
                    // present.
                    result.noNulls = true;
                    for (int i = 0; i < batchSize; i++)
                    {
                        result.isNull[i] = false;
                    }
                }
                return previousVector;
            }

            public BitFieldReader getPresent()
            {
                return present;
            }
        }

        sealed public class BooleanTreeReader : TreeReader<bool?>
        {
            private BitFieldReader reader = null;

            public BooleanTreeReader(int columnId, InStream present = null, InStream data = null)
                : base(columnId, present)
            {
                if (data != null)
                {
                    reader = new BitFieldReader(data, 1);
                }
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter)
            {
                base.startStripe(streams, stripeFooter);
                reader = new BitFieldReader(streams.get(new StreamName(columnId,
                    OrcProto.Stream.Types.Kind.DATA)), 1);
            }

            public override void seek(PositionProvider[] index)
            {
                seek(index[columnId]);
            }

            public override void seek(PositionProvider index)
            {
                base.seek(index);
                reader.seek(index);
            }

            public override void skipRows(long items)
            {
                reader.skip(countNonNulls(items));
            }

            public override bool? getNext()
            {
                if (!hasValue())
                {
                    return null;
                }
                return (reader.next() == 1);
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                LongColumnVector result;
                if (previousVector == null)
                {
                    result = new LongColumnVector();
                }
                else
                {
                    result = (LongColumnVector)previousVector;
                }

                // Read present/isNull stream
                base.nextVector(result, batchSize);

                // Read value entries based on isNull entries
                reader.nextVector(result, batchSize);
                return result;
            }
        }

        sealed public class ByteTreeReader : TreeReader<sbyte?>
        {
            private RunLengthByteReader reader = null;

            public ByteTreeReader(int columnId, InStream present = null, InStream data = null)
                : base(columnId, present)
            {
                this.reader = new RunLengthByteReader(data);
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter)
            {
                base.startStripe(streams, stripeFooter);
                reader = new RunLengthByteReader(streams.get(new StreamName(columnId,
                    OrcProto.Stream.Types.Kind.DATA)));
            }

            public override void seek(PositionProvider[] index)
            {
                seek(index[columnId]);
            }

            public override void seek(PositionProvider index)
            {
                base.seek(index);
                reader.seek(index);
            }

            public override sbyte? getNext()
            {
                if (!hasValue())
                {
                    return null;
                }

                return (sbyte)reader.next();
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                LongColumnVector result;
                if (previousVector == null)
                {
                    result = new LongColumnVector();
                }
                else
                {
                    result = (LongColumnVector)previousVector;
                }

                // Read present/isNull stream
                base.nextVector(result, batchSize);

                // Read value entries based on isNull entries
                reader.nextVector(result, batchSize);
                return result;
            }

            public override void skipRows(long items)
            {
                reader.skip(countNonNulls(items));
            }
        }

        sealed public class ShortTreeReader : TreeReader<short?>
        {
            private IntegerReader reader = null;

            public ShortTreeReader(int columnId, InStream present = null, InStream data = null,
                OrcProto.ColumnEncoding encoding = null)
                : base(columnId, present)
            {
                if (data != null && encoding != null)
                {
                    checkEncoding(encoding);
                    this.reader = createIntegerReader(encoding.Kind, data, true, false);
                }
            }

            public override void checkEncoding(OrcProto.ColumnEncoding encoding)
            {
                if ((encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT) &&
                    (encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2))
                {
                    throw new IOException("Unknown encoding " + encoding + " in column " +
                        columnId);
                }
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter)
            {
                base.startStripe(streams, stripeFooter);
                StreamName name = new StreamName(columnId,
                    OrcProto.Stream.Types.Kind.DATA);
                reader = createIntegerReader(stripeFooter.ColumnsList[columnId].Kind,
                    streams.get(name), true, false);
            }

            public override void seek(PositionProvider[] index)
            {
                seek(index[columnId]);
            }

            public override void seek(PositionProvider index)
            {
                base.seek(index);
                reader.seek(index);
            }

            public override short? getNext()
            {
                if (!hasValue())
                {
                    return null;
                }

                return (short)reader.next();
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                LongColumnVector result;
                if (previousVector == null)
                {
                    result = new LongColumnVector();
                }
                else
                {
                    result = (LongColumnVector)previousVector;
                }

                // Read present/isNull stream
                base.nextVector(result, batchSize);

                // Read value entries based on isNull entries
                reader.nextVector(result, batchSize);
                return result;
            }

            public override void skipRows(long items)
            {
                reader.skip(countNonNulls(items));
            }
        }

        sealed public class IntTreeReader : TreeReader<int?>
        {
            private IntegerReader reader = null;

            public IntTreeReader(int columnId, InStream present = null, InStream data = null,
                OrcProto.ColumnEncoding encoding = null)
                : base(columnId, present)
            {
                if (data != null && encoding != null)
                {
                    checkEncoding(encoding);
                    this.reader = createIntegerReader(encoding.Kind, data, true, false);
                }
            }

            public override void checkEncoding(OrcProto.ColumnEncoding encoding)
            {
                if ((encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT) &&
                    (encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2))
                {
                    throw new IOException("Unknown encoding " + encoding + " in column " +
                        columnId);
                }
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter)
            {
                base.startStripe(streams, stripeFooter);
                StreamName name = new StreamName(columnId,
                    OrcProto.Stream.Types.Kind.DATA);
                reader = createIntegerReader(stripeFooter.ColumnsList[columnId].Kind,
                    streams.get(name), true, false);
            }

            public override void seek(PositionProvider[] index)
            {
                seek(index[columnId]);
            }

            public override void seek(PositionProvider index)
            {
                base.seek(index);
                reader.seek(index);
            }

            public override int? getNext()
            {
                if (!hasValue())
                {
                    return null;
                }
                return (int)reader.next();
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                LongColumnVector result;
                if (previousVector == null)
                {
                    result = new LongColumnVector();
                }
                else
                {
                    result = (LongColumnVector)previousVector;
                }

                // Read present/isNull stream
                base.nextVector(result, batchSize);

                // Read value entries based on isNull entries
                reader.nextVector(result, batchSize);
                return result;
            }

            public override void skipRows(long items)
            {
                reader.skip(countNonNulls(items));
            }
        }

        sealed public class LongTreeReader : TreeReader<long?>
        {
            private IntegerReader reader = null;

            public LongTreeReader(int columnId, bool skipCorrupt)
                : this(columnId, null, null, null, skipCorrupt)
            {
            }

            public LongTreeReader(int columnId, InStream present, InStream data,
                OrcProto.ColumnEncoding encoding,
                bool skipCorrupt)
                : base(columnId, present)
            {
                if (data != null && encoding != null)
                {
                    checkEncoding(encoding);
                    this.reader = createIntegerReader(encoding.Kind, data, true, skipCorrupt);
                }
            }

            public override void checkEncoding(OrcProto.ColumnEncoding encoding)
            {
                if ((encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT) &&
                    (encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2))
                {
                    throw new IOException("Unknown encoding " + encoding + " in column " +
                        columnId);
                }
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter)
            {
                base.startStripe(streams, stripeFooter);
                StreamName name = new StreamName(columnId,
                    OrcProto.Stream.Types.Kind.DATA);
                reader = createIntegerReader(stripeFooter.ColumnsList[columnId].Kind,
                    streams.get(name), true, false);
            }

            public override void seek(PositionProvider[] index)
            {
                seek(index[columnId]);
            }

            public override void seek(PositionProvider index)
            {
                base.seek(index);
                reader.seek(index);
            }

            public override long? getNext()
            {
                if (!hasValue())
                {
                    return null;
                }
                return reader.next();
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                LongColumnVector result;
                if (previousVector == null)
                {
                    result = new LongColumnVector();
                }
                else
                {
                    result = (LongColumnVector)previousVector;
                }

                // Read present/isNull stream
                base.nextVector(result, batchSize);

                // Read value entries based on isNull entries
                reader.nextVector(result, batchSize);
                return result;
            }

            public override void skipRows(long items)
            {
                reader.skip(countNonNulls(items));
            }
        }

        sealed public class FloatTreeReader : TreeReader<float?>
        {
            private InStream stream;
            private SerializationUtils utils;

            public FloatTreeReader(int columnId, InStream present = null, InStream data = null)
                : base(columnId, present)
            {
                this.utils = new SerializationUtils();
                this.stream = data;
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter)
            {
                base.startStripe(streams, stripeFooter);
                StreamName name = new StreamName(columnId,
                    OrcProto.Stream.Types.Kind.DATA);
                stream = streams.get(name);
            }

            public override void seek(PositionProvider[] index)
            {
                seek(index[columnId]);
            }

            public override void seek(PositionProvider index)
            {
                base.seek(index);
                stream.seek(index);
            }

            public override float? getNext()
            {
                if (!hasValue())
                {
                    return null;
                }
                return utils.readFloat(stream);
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                DoubleColumnVector result;
                if (previousVector == null)
                {
                    result = new DoubleColumnVector();
                }
                else
                {
                    result = (DoubleColumnVector)previousVector;
                }

                // Read present/isNull stream
                base.nextVector(result, batchSize);

                bool hasNulls = !result.noNulls;
                bool allNulls = hasNulls;

                if (hasNulls)
                {
                    // conditions to ensure bounds checks skips
                    for (int i = 0; batchSize <= result.isNull.Length && i < batchSize; i++)
                    {
                        allNulls = allNulls & result.isNull[i];
                    }
                    if (allNulls)
                    {
                        result.vector[0] = Double.NaN;
                        result.isRepeating = true;
                    }
                    else
                    {
                        // some nulls
                        result.isRepeating = false;
                        // conditions to ensure bounds checks skips
                        for (int i = 0; batchSize <= result.isNull.Length
                            && batchSize <= result.vector.Length && i < batchSize; i++)
                        {
                            if (!result.isNull[i])
                            {
                                result.vector[i] = utils.readFloat(stream);
                            }
                            else
                            {
                                // If the value is not present then set NaN
                                result.vector[i] = Double.NaN;
                            }
                        }
                    }
                }
                else
                {
                    // no nulls & > 1 row (check repeating)
                    bool repeating = (batchSize > 1);
                    float f1 = utils.readFloat(stream);
                    result.vector[0] = f1;
                    // conditions to ensure bounds checks skips
                    for (int i = 1; i < batchSize && batchSize <= result.vector.Length; i++)
                    {
                        float f2 = utils.readFloat(stream);
                        repeating = repeating && (f1 == f2);
                        result.vector[i] = f2;
                    }
                    result.isRepeating = repeating;
                }
                return result;
            }

            public override void skipRows(long items)
            {
                items = countNonNulls(items);
                for (int i = 0; i < items; ++i)
                {
                    utils.readFloat(stream);
                }
            }
        }

        sealed public class DoubleTreeReader : TreeReader<double?>
        {
            private InStream stream;
            private SerializationUtils utils;

            public DoubleTreeReader(int columnId, InStream present = null, InStream data = null)
                : base(columnId, present)
            {
                this.utils = new SerializationUtils();
                this.stream = data;
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter
            )
            {
                base.startStripe(streams, stripeFooter);
                StreamName name =
                    new StreamName(columnId,
                        OrcProto.Stream.Types.Kind.DATA);
                stream = streams.get(name);
            }

            public override void seek(PositionProvider[] index)
            {
                seek(index[columnId]);
            }

            public override void seek(PositionProvider index)
            {
                base.seek(index);
                stream.seek(index);
            }

            public override double? getNext()
            {
                if (!hasValue())
                {
                    return null;
                }
                return utils.readDouble(stream);
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                DoubleColumnVector result;
                if (previousVector == null)
                {
                    result = new DoubleColumnVector();
                }
                else
                {
                    result = (DoubleColumnVector)previousVector;
                }

                // Read present/isNull stream
                base.nextVector(result, batchSize);

                bool hasNulls = !result.noNulls;
                bool allNulls = hasNulls;

                if (hasNulls)
                {
                    // conditions to ensure bounds checks skips
                    for (int i = 0; i < batchSize && batchSize <= result.isNull.Length; i++)
                    {
                        allNulls = allNulls & result.isNull[i];
                    }
                    if (allNulls)
                    {
                        result.vector[0] = Double.NaN;
                        result.isRepeating = true;
                    }
                    else
                    {
                        // some nulls
                        result.isRepeating = false;
                        // conditions to ensure bounds checks skips
                        for (int i = 0; batchSize <= result.isNull.Length
                            && batchSize <= result.vector.Length && i < batchSize; i++)
                        {
                            if (!result.isNull[i])
                            {
                                result.vector[i] = utils.readDouble(stream);
                            }
                            else
                            {
                                // If the value is not present then set NaN
                                result.vector[i] = Double.NaN;
                            }
                        }
                    }
                }
                else
                {
                    // no nulls
                    bool repeating = (batchSize > 1);
                    double d1 = utils.readDouble(stream);
                    result.vector[0] = d1;
                    // conditions to ensure bounds checks skips
                    for (int i = 1; i < batchSize && batchSize <= result.vector.Length; i++)
                    {
                        double d2 = utils.readDouble(stream);
                        repeating = repeating && (d1 == d2);
                        result.vector[i] = d2;
                    }
                    result.isRepeating = repeating;
                }

                return result;
            }

            public override void skipRows(long items)
            {
                items = countNonNulls(items);
                long len = items * 8;
                while (len > 0)
                {
                    len -= stream.skip(len);
                }
            }
        }

        sealed public class BinaryTreeReader : TreeReader<byte[]>
        {
            private InStream stream;
            private IntegerReader lengths = null;
            private LongColumnVector scratchlcv;

            public BinaryTreeReader(int columnId, InStream present = null, InStream data = null, InStream length = null,
                OrcProto.ColumnEncoding encoding = null)
                : base(columnId, present)
            {
                scratchlcv = new LongColumnVector();
                this.stream = data;
                if (length != null && encoding != null)
                {
                    checkEncoding(encoding);
                    this.lengths = createIntegerReader(encoding.Kind, length, false, false);
                }
            }

            public override void checkEncoding(OrcProto.ColumnEncoding encoding)
            {
                if ((encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT) &&
                    (encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2))
                {
                    throw new IOException("Unknown encoding " + encoding + " in column " +
                        columnId);
                }
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter)
            {
                base.startStripe(streams, stripeFooter);
                StreamName name = new StreamName(columnId,
                    OrcProto.Stream.Types.Kind.DATA);
                stream = streams.get(name);
                lengths = createIntegerReader(stripeFooter.ColumnsList[columnId].Kind,
                    streams.get(new StreamName(columnId, OrcProto.Stream.Types.Kind.LENGTH)), false, false);
            }

            public override void seek(PositionProvider[] index)
            {
                seek(index[columnId]);
            }

            public override void seek(PositionProvider index)
            {
                base.seek(index);
                stream.seek(index);
                lengths.seek(index);
            }

            public override byte[] getNext()
            {
                if (!hasValue())
                {
                    return null;
                }
                int len = (int)lengths.next();
                byte[] result = new byte[len];
                stream.readFully(result, 0, len);
                return result;
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                BytesColumnVector result;
                if (previousVector == null)
                {
                    result = new BytesColumnVector();
                }
                else
                {
                    result = (BytesColumnVector)previousVector;
                }

                // Read present/isNull stream
                base.nextVector(result, batchSize);

                BytesColumnVectorUtil.readOrcByteArrays(stream, lengths, scratchlcv, result, batchSize);
                return result;
            }

            public override void skipRows(long items)
            {
                items = countNonNulls(items);
                long lengthToSkip = 0;
                for (int i = 0; i < items; ++i)
                {
                    lengthToSkip += lengths.next();
                }
                while (lengthToSkip > 0)
                {
                    lengthToSkip -= stream.skip(lengthToSkip);
                }
            }
        }

        sealed public class TimestampTreeReader : TreeReader<Timestamp?>
        {
            private IntegerReader data = null;
            private IntegerReader nanos = null;
            private bool _skipCorrupt;
            private long base_timestamp;
            private TimeZoneInfo readerTimeZone;
            private TimeZoneInfo writerTimeZone;
            private bool hasSameTZRules;

            public TimestampTreeReader(int columnId, bool skipCorrupt)
                : this(columnId, null, null, null, null, skipCorrupt)
            {
            }

            public TimestampTreeReader(int columnId, InStream presentStream, InStream dataStream,
                InStream nanosStream, OrcProto.ColumnEncoding encoding, bool skipCorrupt)
                : base(columnId, presentStream)
            {
                this._skipCorrupt = skipCorrupt;
                this.readerTimeZone = (CreateTimeZone != null) ? CreateTimeZone() : TimeZoneInfo.Local;
                this.base_timestamp = TimeZones.GetBaseTimestamp(readerTimeZone.Id, out writerTimeZone);
                this.hasSameTZRules = writerTimeZone.HasSameRules(readerTimeZone);
                if (encoding != null)
                {
                    checkEncoding(encoding);

                    if (dataStream != null)
                    {
                        this.data = createIntegerReader(encoding.Kind, dataStream, true, _skipCorrupt);
                    }

                    if (nanosStream != null)
                    {
                        this.nanos = createIntegerReader(encoding.Kind, nanosStream, false, _skipCorrupt);
                    }
                }
            }

            public override void checkEncoding(OrcProto.ColumnEncoding encoding)
            {
                if ((encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT) &&
                    (encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2))
                {
                    throw new IOException("Unknown encoding " + encoding + " in column " +
                        columnId);
                }
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter)
            {
                base.startStripe(streams, stripeFooter);
                data = createIntegerReader(stripeFooter.ColumnsList[columnId].Kind,
                    streams.get(new StreamName(columnId,
                        OrcProto.Stream.Types.Kind.DATA)), true, _skipCorrupt);
                nanos = createIntegerReader(stripeFooter.ColumnsList[columnId].Kind,
                    streams.get(new StreamName(columnId,
                        OrcProto.Stream.Types.Kind.SECONDARY)), false, _skipCorrupt);

                string timezone = stripeFooter.HasWriterTimezone ? stripeFooter.WriterTimezone : "UTC";
                this.base_timestamp = TimeZones.GetBaseTimestamp(timezone, out writerTimeZone);
                this.hasSameTZRules = writerTimeZone.HasSameRules(readerTimeZone);
            }

            public override void seek(PositionProvider[] index)
            {
                seek(index[columnId]);
            }

            public override void seek(PositionProvider index)
            {
                base.seek(index);
                data.seek(index);
                nanos.seek(index);
            }

            public override Timestamp? getNext()
            {
                if (!hasValue())
                {
                    return null;
                }

                long millis = (data.next() + base_timestamp) * WriterImpl.MILLIS_PER_SECOND;
                int newNanos = parseNanos(nanos.next());
                // fix the rounding when we divided by 1000.
                if (millis >= 0)
                {
                    millis += newNanos / 1000000;
                }
                else
                {
                    millis -= newNanos / 1000000;
                }
                DateTime timestamp = Epoch.getTimestamp(millis);

                long offset = 0;
                // If reader and writer time zones have different rules, adjust the timezone difference
                // between reader and writer taking day light savings into account.
                if (!hasSameTZRules)
                {
                    offset = (long)((writerTimeZone.GetUtcOffset(timestamp) - readerTimeZone.GetUtcOffset(timestamp)).TotalMilliseconds);
                }
                long adjustedMillis = millis + offset;
                DateTime ts = Epoch.getTimestamp(adjustedMillis);
                // Sometimes the reader timezone might have changed after adding the adjustedMillis.
                // To account for that change, check for any difference in reader timezone after
                // adding adjustedMillis. If so use the new offset (offset at adjustedMillis point of time).
                if (!hasSameTZRules &&
                    (readerTimeZone.GetUtcOffset(timestamp) != readerTimeZone.GetUtcOffset(ts)))
                {
                    long newOffset =
                        (long)((writerTimeZone.GetUtcOffset(timestamp) - readerTimeZone.GetUtcOffset(ts)).TotalMilliseconds);
                    adjustedMillis = millis + newOffset;
                }

                return new Timestamp(adjustedMillis, newNanos);
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                LongColumnVector result;
                if (previousVector == null)
                {
                    result = new LongColumnVector();
                }
                else
                {
                    result = (LongColumnVector)previousVector;
                }

                result.reset();
                for (int i = 0; i < batchSize; i++)
                {
                    Timestamp? obj = getNext();
                    if (obj == null)
                    {
                        result.noNulls = false;
                        result.isNull[i] = true;
                    }
                    else
                    {
                        result.vector[i] = obj.Value.Nanoseconds;
                    }
                }

                return result;
            }

            private static int parseNanos(long serialized)
            {
                int zeros = 7 & (int)serialized;
                int result = (int)((ulong)serialized >> 3);
                if (zeros != 0)
                {
                    for (int i = 0; i <= zeros; ++i)
                    {
                        result *= 10;
                    }
                }
                return result;
            }

            public override void skipRows(long items)
            {
                items = countNonNulls(items);
                data.skip(items);
                nanos.skip(items);
            }
        }

        sealed public class DateTreeReader : TreeReader<Date?>
        {
            private IntegerReader reader = null;

            public DateTreeReader(int columnId, InStream present = null, InStream data = null,
                OrcProto.ColumnEncoding encoding = null)
                : base(columnId, present)
            {
                if (data != null && encoding != null)
                {
                    checkEncoding(encoding);
                    reader = createIntegerReader(encoding.Kind, data, true, false);
                }
            }

            public override void checkEncoding(OrcProto.ColumnEncoding encoding)
            {
                if ((encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT) &&
                    (encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2))
                {
                    throw new IOException("Unknown encoding " + encoding + " in column " +
                        columnId);
                }
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter)
            {
                base.startStripe(streams, stripeFooter);
                StreamName name = new StreamName(columnId,
                    OrcProto.Stream.Types.Kind.DATA);
                reader = createIntegerReader(stripeFooter.ColumnsList[columnId].Kind,
                    streams.get(name), true, false);
            }

            public override void seek(PositionProvider[] index)
            {
                seek(index[columnId]);
            }

            public override void seek(PositionProvider index)
            {
                base.seek(index);
                reader.seek(index);
            }

            public override Date? getNext()
            {
                if (!hasValue())
                {
                    return null;
                }
                return new Date((int)reader.next());
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                LongColumnVector result;
                if (previousVector == null)
                {
                    result = new LongColumnVector();
                }
                else
                {
                    result = (LongColumnVector)previousVector;
                }

                // Read present/isNull stream
                base.nextVector(result, batchSize);

                // Read value entries based on isNull entries
                reader.nextVector(result, batchSize);
                return result;
            }

            public override void skipRows(long items)
            {
                reader.skip(countNonNulls(items));
            }
        }

        sealed public class DecimalTreeReader : TreeReader<HiveDecimal>
        {
            private InStream valueStream;
            private IntegerReader scaleReader = null;
            private LongColumnVector scratchScaleVector;

            private int precision;
            private int scale;

            public DecimalTreeReader(int columnId, int precision, int scale, InStream present = null,
                InStream valueStream = null, InStream scaleStream = null, OrcProto.ColumnEncoding encoding = null)
                : base(columnId, present)
            {
                this.precision = precision;
                this.scale = scale;
                this.scratchScaleVector = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
                this.valueStream = valueStream;
                if (scaleStream != null && encoding != null)
                {
                    checkEncoding(encoding);
                    this.scaleReader = createIntegerReader(encoding.Kind, scaleStream, true, false);
                }
            }

            public override void checkEncoding(OrcProto.ColumnEncoding encoding)
            {
                if ((encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT) &&
                    (encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2))
                {
                    throw new IOException("Unknown encoding " + encoding + " in column " +
                        columnId);
                }
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter)
            {
                base.startStripe(streams, stripeFooter);
                valueStream = streams.get(new StreamName(columnId,
                    OrcProto.Stream.Types.Kind.DATA));
                scaleReader = createIntegerReader(stripeFooter.ColumnsList[columnId].Kind,
                    streams.get(new StreamName(columnId, OrcProto.Stream.Types.Kind.SECONDARY)), true, false);
            }

            public override void seek(PositionProvider[] index)
            {
                seek(index[columnId]);
            }

            public override void seek(PositionProvider index)
            {
                base.seek(index);
                valueStream.seek(index);
                scaleReader.seek(index);
            }

            public override HiveDecimal getNext()
            {
                if (!hasValue())
                {
                    return null;
                }

                HiveDecimal result = HiveDecimal.create(
                    SerializationUtils.readBigInteger(valueStream),
                    (int)scaleReader.next());
                return HiveDecimal.enforcePrecisionScale(result, precision, scale);
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                DecimalColumnVector result;
                if (previousVector == null)
                {
                    result = new DecimalColumnVector(precision, scale);
                }
                else
                {
                    result = (DecimalColumnVector)previousVector;
                }

                // Save the reference for isNull in the scratch vector
                bool[] scratchIsNull = scratchScaleVector.isNull;

                // Read present/isNull stream
                base.nextVector(result, batchSize);

                // Read value entries based on isNull entries
                if (result.isRepeating)
                {
                    if (!result.isNull[0])
                    {
                        BigInteger bInt = SerializationUtils.readBigInteger(valueStream);
                        short scaleInData = (short)scaleReader.next();
                        HiveDecimal dec = HiveDecimal.create(bInt, scaleInData);
                        dec = HiveDecimal.enforcePrecisionScale(dec, precision, scale);
                        result.set(0, dec);
                    }
                }
                else
                {
                    // result vector has isNull values set, use the same to read scale vector.
                    scratchScaleVector.isNull = result.isNull;
                    scaleReader.nextVector(scratchScaleVector, batchSize);
                    for (int i = 0; i < batchSize; i++)
                    {
                        if (!result.isNull[i])
                        {
                            BigInteger bInt = SerializationUtils.readBigInteger(valueStream);
                            short scaleInData = (short)scratchScaleVector.vector[i];
                            HiveDecimal dec = HiveDecimal.create(bInt, scaleInData);
                            dec = HiveDecimal.enforcePrecisionScale(dec, precision, scale);
                            result.set(i, dec);
                        }
                    }
                }
                // Switch back the null vector.
                scratchScaleVector.isNull = scratchIsNull;
                return result;
            }

            public override void skipRows(long items)
            {
                items = countNonNulls(items);
                for (int i = 0; i < items; i++)
                {
                    SerializationUtils.readBigInteger(valueStream);
                }
                scaleReader.skip(items);
            }
        }

        /**
         * A tree reader that will read string columns. At the start of the
         * stripe, it creates an internal reader based on whether a direct or
         * dictionary encoding was used.
         */
        public class StringTreeReader : TreeReader<string>
        {
            private TreeReader<string> reader;

            public StringTreeReader(int columnId)
                : base(columnId)
            {
            }

            public StringTreeReader(int columnId, InStream present, InStream data, InStream length,
                InStream dictionary, OrcProto.ColumnEncoding encoding)
                : base(columnId, present)
            {
                if (encoding != null)
                {
                    switch (encoding.Kind)
                    {
                        case OrcProto.ColumnEncoding.Types.Kind.DIRECT:
                        case OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2:
                            reader = new StringDirectTreeReader(columnId, present, data, length,
                                encoding.Kind);
                            break;
                        case OrcProto.ColumnEncoding.Types.Kind.DICTIONARY:
                        case OrcProto.ColumnEncoding.Types.Kind.DICTIONARY_V2:
                            reader = new StringDictionaryTreeReader(columnId, present, data, length, dictionary,
                                encoding);
                            break;
                        default:
                            throw new ArgumentException("Unsupported encoding " +
                                encoding.Kind);
                    }
                }
            }

            public override void checkEncoding(OrcProto.ColumnEncoding encoding)
            {
                reader.checkEncoding(encoding);
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter)
            {
                // For each stripe, checks the encoding and initializes the appropriate
                // reader
                switch (stripeFooter.ColumnsList[columnId].Kind)
                {
                    case OrcProto.ColumnEncoding.Types.Kind.DIRECT:
                    case OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2:
                        reader = new StringDirectTreeReader(columnId);
                        break;
                    case OrcProto.ColumnEncoding.Types.Kind.DICTIONARY:
                    case OrcProto.ColumnEncoding.Types.Kind.DICTIONARY_V2:
                        reader = new StringDictionaryTreeReader(columnId);
                        break;
                    default:
                        throw new ArgumentException("Unsupported encoding " +
                            stripeFooter.ColumnsList[columnId].Kind);
                }
                reader.startStripe(streams, stripeFooter);
            }

            public override void seek(PositionProvider[] index)
            {
                reader.seek(index);
            }

            public override void seek(PositionProvider index)
            {
                reader.seek(index);
            }

            public override string getNext()
            {
                return reader.getNext();
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                return reader.nextVector(previousVector, batchSize);
            }

            public override void skipRows(long items)
            {
                reader.skipRows(items);
            }
        }

        // This class collects together very similar methods for reading an ORC vector of byte arrays and
        // creating the BytesColumnVector.
        //
        public class BytesColumnVectorUtil
        {
            private static byte[] commonReadByteArrays(InStream stream, IntegerReader lengths,
                LongColumnVector scratchlcv,
                BytesColumnVector result, long batchSize)
            {
                // Read lengths
                scratchlcv.isNull = result.isNull;  // Notice we are replacing the isNull vector here...
                lengths.nextVector(scratchlcv, batchSize);
                int totalLength = 0;
                if (!scratchlcv.isRepeating)
                {
                    for (int i = 0; i < batchSize; i++)
                    {
                        if (!scratchlcv.isNull[i])
                        {
                            totalLength += (int)scratchlcv.vector[i];
                        }
                    }
                }
                else
                {
                    if (!scratchlcv.isNull[0])
                    {
                        totalLength = (int)(batchSize * scratchlcv.vector[0]);
                    }
                }

                // Read all the strings for this batch
                byte[] allBytes = new byte[totalLength];
                int offset = 0;
                int len = totalLength;
                while (len > 0)
                {
                    int bytesRead = stream.Read(allBytes, offset, len);
                    if (bytesRead < 0)
                    {
                        throw new EndOfStreamException("Can't finish byte read from " + stream);
                    }
                    len -= bytesRead;
                    offset += bytesRead;
                }

                return allBytes;
            }

            // This method has the common code for reading in bytes into a BytesColumnVector.
            public static void readOrcByteArrays(InStream stream, IntegerReader lengths,
                LongColumnVector scratchlcv,
                BytesColumnVector result, long batchSize)
            {

                byte[] allBytes = commonReadByteArrays(stream, lengths, scratchlcv, result, batchSize);

                // Too expensive to figure out 'repeating' by comparisons.
                result.isRepeating = false;
                int offset = 0;
                if (!scratchlcv.isRepeating)
                {
                    for (int i = 0; i < batchSize; i++)
                    {
                        if (!scratchlcv.isNull[i])
                        {
                            result.setRef(i, allBytes, offset, (int)scratchlcv.vector[i]);
                            offset += (int)scratchlcv.vector[i];
                        }
                        else
                        {
                            result.setRef(i, allBytes, 0, 0);
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < batchSize; i++)
                    {
                        if (!scratchlcv.isNull[i])
                        {
                            result.setRef(i, allBytes, offset, (int)scratchlcv.vector[0]);
                            offset += (int)scratchlcv.vector[0];
                        }
                        else
                        {
                            result.setRef(i, allBytes, 0, 0);
                        }
                    }
                }
            }
        }

        /**
         * A reader for string columns that are direct encoded in the current
         * stripe.
         */
        sealed public class StringDirectTreeReader : TreeReader<string>
        {
            private InStream stream;
            private IntegerReader lengths;
            private LongColumnVector scratchlcv;

            public StringDirectTreeReader(int columnId, InStream present = null, InStream data = null,
                InStream length = null, OrcProto.ColumnEncoding.Types.Kind? encoding = null)
                : base(columnId, present)
            {
                this.scratchlcv = new LongColumnVector();
                this.stream = data;
                if (length != null && encoding != null)
                {
                    this.lengths = createIntegerReader(encoding.Value, length, false, false);
                }
            }

            public override void checkEncoding(OrcProto.ColumnEncoding encoding)
            {
                if (encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT &&
                    encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2)
                {
                    throw new IOException("Unknown encoding " + encoding + " in column " +
                        columnId);
                }
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter
            )
            {
                base.startStripe(streams, stripeFooter);
                StreamName name = new StreamName(columnId,
                    OrcProto.Stream.Types.Kind.DATA);
                stream = streams.get(name);
                lengths = createIntegerReader(stripeFooter.ColumnsList[columnId].Kind,
                    streams.get(new StreamName(columnId, OrcProto.Stream.Types.Kind.LENGTH)),
                    false, false);
            }

            public override void seek(PositionProvider[] index)
            {
                seek(index[columnId]);
            }

            public override void seek(PositionProvider index)
            {
                base.seek(index);
                stream.seek(index);
                // don't seek data stream
                lengths.seek(index);
            }

            public override string getNext()
            {
                if (!hasValue())
                {
                    return null;
                }
                int len = (int)lengths.next();
                byte[] tmp = new byte[len];
                stream.readFully(tmp, 0, len);
                return Encoding.UTF8.GetString(tmp);
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                BytesColumnVector result;
                if (previousVector == null)
                {
                    result = new BytesColumnVector();
                }
                else
                {
                    result = (BytesColumnVector)previousVector;
                }

                // Read present/isNull stream
                base.nextVector(result, batchSize);

                BytesColumnVectorUtil.readOrcByteArrays(stream, lengths, scratchlcv, result, batchSize);
                return result;
            }

            public override void skipRows(long items)
            {
                items = countNonNulls(items);
                long lengthToSkip = 0;
                for (int i = 0; i < items; ++i)
                {
                    lengthToSkip += lengths.next();
                }

                while (lengthToSkip > 0)
                {
                    lengthToSkip -= stream.skip(lengthToSkip);
                }
            }

            public IntegerReader getLengths()
            {
                return lengths;
            }

            public InStream getStream()
            {
                return stream;
            }
        }

        /**
         * A reader for string columns that are dictionary encoded in the current
         * stripe.
         */
        sealed public class StringDictionaryTreeReader : TreeReader<string>
        {
            private DynamicByteArray dictionaryBuffer;
            private int[] dictionaryOffsets;
            private IntegerReader reader;

            private byte[] dictionaryBufferInBytesCache = null;
            private LongColumnVector scratchlcv;

            public StringDictionaryTreeReader(int columnId, InStream present = null, InStream data = null,
                InStream length = null, InStream dictionary = null, OrcProto.ColumnEncoding encoding = null)
                : base(columnId, present)
            {
                scratchlcv = new LongColumnVector();
                if (data != null && encoding != null)
                {
                    this.reader = createIntegerReader(encoding.Kind, data, false, false);
                }

                if (dictionary != null && encoding != null)
                {
                    readDictionaryStream(dictionary);
                }

                if (length != null && encoding != null)
                {
                    readDictionaryLengthStream(length, encoding);
                }
            }

            public override void checkEncoding(OrcProto.ColumnEncoding encoding)
            {
                if (encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DICTIONARY &&
                    encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DICTIONARY_V2)
                {
                    throw new IOException("Unknown encoding " + encoding + " in column " +
                        columnId);
                }
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter)
            {
                base.startStripe(streams, stripeFooter);

                // read the dictionary blob
                StreamName name = new StreamName(columnId,
                    OrcProto.Stream.Types.Kind.DICTIONARY_DATA);
                InStream @in = streams.get(name);
                readDictionaryStream(@in);

                // read the lengths
                name = new StreamName(columnId, OrcProto.Stream.Types.Kind.LENGTH);
                @in = streams.get(name);
                readDictionaryLengthStream(@in, stripeFooter.ColumnsList[columnId]);

                // set up the row reader
                name = new StreamName(columnId, OrcProto.Stream.Types.Kind.DATA);
                reader = createIntegerReader(stripeFooter.ColumnsList[columnId].Kind,
                    streams.get(name), false, false);
            }

            private void readDictionaryLengthStream(InStream @in, OrcProto.ColumnEncoding encoding)
            {
                int dictionarySize = (int)encoding.DictionarySize;
                if (@in != null)
                { // Guard against empty LENGTH stream.
                    IntegerReader lenReader = createIntegerReader(encoding.Kind, @in, false, false);
                    int offset = 0;
                    if (dictionaryOffsets == null ||
                        dictionaryOffsets.Length < dictionarySize + 1)
                    {
                        dictionaryOffsets = new int[dictionarySize + 1];
                    }
                    for (int i = 0; i < dictionarySize; ++i)
                    {
                        dictionaryOffsets[i] = offset;
                        offset += (int)lenReader.next();
                    }
                    dictionaryOffsets[dictionarySize] = offset;
                    @in.Close();
                }
            }

            private void readDictionaryStream(InStream @in)
            {
                if (@in != null)
                { // Guard against empty dictionary stream.
                    if (@in.available() > 0)
                    {
                        dictionaryBuffer = new DynamicByteArray(64, @in.available());
                        dictionaryBuffer.readAll(@in);
                        // Since its start of strip invalidate the cache.
                        dictionaryBufferInBytesCache = null;
                    }
                    @in.Close();
                }
                else
                {
                    dictionaryBuffer = null;
                }
            }

            public override void seek(PositionProvider[] index)
            {
                seek(index[columnId]);
            }

            public override void seek(PositionProvider index)
            {
                base.seek(index);
                reader.seek(index);
            }

            public override string getNext()
            {
                if (!hasValue())
                {
                    return null;
                }

                int entry = (int)reader.next();
                int offset = dictionaryOffsets[entry];
                int length = getDictionaryEntryLength(entry, offset);
                // If the column is just empty strings, the size will be zero,
                // so the buffer will be null, in that case just return result
                // as it will default to empty
                if (dictionaryBuffer != null)
                {
                    return dictionaryBuffer.getText(offset, length);
                }
                else
                {
                    return string.Empty;
                }
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                BytesColumnVector result;
                int offset;
                int length;
                if (previousVector == null)
                {
                    result = new BytesColumnVector();
                }
                else
                {
                    result = (BytesColumnVector)previousVector;
                }

                // Read present/isNull stream
                base.nextVector(result, batchSize);

                if (dictionaryBuffer != null)
                {

                    // Load dictionaryBuffer into cache.
                    if (dictionaryBufferInBytesCache == null)
                    {
                        dictionaryBufferInBytesCache = dictionaryBuffer.get();
                    }

                    // Read string offsets
                    scratchlcv.isNull = result.isNull;
                    reader.nextVector(scratchlcv, batchSize);
                    if (!scratchlcv.isRepeating)
                    {

                        // The vector has non-repeating strings. Iterate thru the batch
                        // and set strings one by one
                        for (int i = 0; i < batchSize; i++)
                        {
                            if (!scratchlcv.isNull[i])
                            {
                                offset = dictionaryOffsets[(int)scratchlcv.vector[i]];
                                length = getDictionaryEntryLength((int)scratchlcv.vector[i], offset);
                                result.setRef(i, dictionaryBufferInBytesCache, offset, length);
                            }
                            else
                            {
                                // If the value is null then set offset and length to zero (null string)
                                result.setRef(i, dictionaryBufferInBytesCache, 0, 0);
                            }
                        }
                    }
                    else
                    {
                        // If the value is repeating then just set the first value in the
                        // vector and set the isRepeating flag to true. No need to iterate thru and
                        // set all the elements to the same value
                        offset = dictionaryOffsets[(int)scratchlcv.vector[0]];
                        length = getDictionaryEntryLength((int)scratchlcv.vector[0], offset);
                        result.setRef(0, dictionaryBufferInBytesCache, offset, length);
                    }
                    result.isRepeating = scratchlcv.isRepeating;
                }
                else
                {
                    // Entire stripe contains null strings.
                    result.isRepeating = true;
                    result.noNulls = false;
                    result.isNull[0] = true;
                    result.setRef(0, new byte[0], 0, 0);
                }
                return result;
            }

            int getDictionaryEntryLength(int entry, int offset)
            {
                int length;
                // if it isn't the last entry, subtract the offsets otherwise use
                // the buffer length.
                if (entry < dictionaryOffsets.Length - 1)
                {
                    length = dictionaryOffsets[entry + 1] - offset;
                }
                else
                {
                    length = dictionaryBuffer.size() - offset;
                }
                return length;
            }

            public override void skipRows(long items)
            {
                reader.skip(countNonNulls(items));
            }

            public IntegerReader getReader()
            {
                return reader;
            }
        }

        sealed public class CharTreeReader : StringTreeReader
        {
            private readonly int maxLength;

            public CharTreeReader(int columnId, int maxLength, InStream present = null, InStream data = null,
                InStream length = null, InStream dictionary = null, OrcProto.ColumnEncoding encoding = null) :
                base(columnId, present, data, length, dictionary, encoding)
            {
                this.maxLength = maxLength;
            }

            public override string getNext()
            {
                // Use the string reader implementation
                string textVal = base.getNext();
                if (textVal != null)
                {
                    // TODO: enforce char length
                    // enforceMaxLength(maxLength);
                    return null;
                }
                return textVal;
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                // Get the vector of strings from StringTreeReader, then make a 2nd pass to
                // adjust down the length (right trim and truncate) if necessary.
                BytesColumnVector result = (BytesColumnVector)base.nextVector(previousVector, batchSize);

                int adjustedDownLen;
                if (result.isRepeating)
                {
                    if (result.noNulls || !result.isNull[0])
                    {
                        adjustedDownLen = StringExpr
                            .rightTrimAndTruncate(result.vector[0], result.start[0], result.length[0], maxLength);
                        if (adjustedDownLen < result.length[0])
                        {
                            result.setRef(0, result.vector[0], result.start[0], adjustedDownLen);
                        }
                    }
                }
                else
                {
                    if (result.noNulls)
                    {
                        for (int i = 0; i < batchSize; i++)
                        {
                            adjustedDownLen = StringExpr
                                .rightTrimAndTruncate(result.vector[i], result.start[i], result.length[i],
                                    maxLength);
                            if (adjustedDownLen < result.length[i])
                            {
                                result.setRef(i, result.vector[i], result.start[i], adjustedDownLen);
                            }
                        }
                    }
                    else
                    {
                        for (int i = 0; i < batchSize; i++)
                        {
                            if (!result.isNull[i])
                            {
                                adjustedDownLen = StringExpr
                                    .rightTrimAndTruncate(result.vector[i], result.start[i], result.length[i],
                                        maxLength);
                                if (adjustedDownLen < result.length[i])
                                {
                                    result.setRef(i, result.vector[i], result.start[i], adjustedDownLen);
                                }
                            }
                        }
                    }
                }
                return result;
            }
        }

        public class VarcharTreeReader : StringTreeReader
        {
            private readonly int maxLength;

            public VarcharTreeReader(int columnId, int maxLength, InStream present = null, InStream data = null,
                InStream length = null, InStream dictionary = null, OrcProto.ColumnEncoding encoding = null)
                : base(columnId, present, data, length, dictionary, encoding)
            {
                this.maxLength = maxLength;
            }

            public override string getNext()
            {
                // Use the string reader implementation
                string textVal = base.getNext();
                if (textVal != null)
                {
                    // TODO: enforce char length
                    // enforceMaxLength(maxLength);
                    return null;
                }
                return textVal;
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                // Get the vector of strings from StringTreeReader, then make a 2nd pass to
                // adjust down the length (truncate) if necessary.
                BytesColumnVector result = (BytesColumnVector)base.nextVector(previousVector, batchSize);

                int adjustedDownLen;
                if (result.isRepeating)
                {
                    if (result.noNulls || !result.isNull[0])
                    {
                        adjustedDownLen = StringExpr
                            .truncate(result.vector[0], result.start[0], result.length[0], maxLength);
                        if (adjustedDownLen < result.length[0])
                        {
                            result.setRef(0, result.vector[0], result.start[0], adjustedDownLen);
                        }
                    }
                }
                else
                {
                    if (result.noNulls)
                    {
                        for (int i = 0; i < batchSize; i++)
                        {
                            adjustedDownLen = StringExpr
                                .truncate(result.vector[i], result.start[i], result.length[i], maxLength);
                            if (adjustedDownLen < result.length[i])
                            {
                                result.setRef(i, result.vector[i], result.start[i], adjustedDownLen);
                            }
                        }
                    }
                    else
                    {
                        for (int i = 0; i < batchSize; i++)
                        {
                            if (!result.isNull[i])
                            {
                                adjustedDownLen = StringExpr
                                    .truncate(result.vector[i], result.start[i], result.length[i], maxLength);
                                if (adjustedDownLen < result.length[i])
                                {
                                    result.setRef(i, result.vector[i], result.start[i], adjustedDownLen);
                                }
                            }
                        }
                    }
                }
                return result;
            }
        }

        sealed public class StructTreeReader : TreeReader<OrcStruct>
        {
            private int fileColumnCount;
            private int resultColumnCount;
            private TreeReader[] fields;
            private String[] fieldNames;

            public StructTreeReader(
                int columnId,
                TreeReaderSchema treeReaderSchema,
                bool[] included,
                bool skipCorrupt)
                : base(columnId)
            {
                OrcProto.Type fileStructType = treeReaderSchema.getFileTypes()[columnId];
                fileColumnCount = fileStructType.FieldNamesCount;

                OrcProto.Type schemaStructType = treeReaderSchema.getSchemaTypes()[columnId];

                if (columnId == treeReaderSchema.getInnerStructSubtype())
                {
                    // If there are more result columns than reader columns, we will default those additional
                    // columns to NULL.
                    resultColumnCount = schemaStructType.FieldNamesCount;
                }
                else
                {
                    resultColumnCount = fileColumnCount;
                }

                this.fields = new TreeReader[fileColumnCount];
                this.fieldNames = new String[fileColumnCount];

                if (included == null)
                {
                    for (int i = 0; i < fileColumnCount; ++i)
                    {
                        int subtype = (int)schemaStructType.GetSubtypes(i);
                        this.fields[i] = createTreeReader(subtype, treeReaderSchema, included, skipCorrupt);
                        // Use the treeReaderSchema evolution name since file/reader types may not have the real column name.
                        this.fieldNames[i] = schemaStructType.GetFieldNames(i);
                    }
                }
                else
                {
                    for (int i = 0; i < fileColumnCount; ++i)
                    {
                        int subtype = (int)schemaStructType.GetSubtypes(i);
                        if (subtype >= included.Length)
                        {
                            throw new IOException("subtype " + subtype + " exceeds the included array size " +
                                included.Length + " fileTypes " + treeReaderSchema.getFileTypes().ToString() +
                                " schemaTypes " + treeReaderSchema.getSchemaTypes().ToString() +
                                " innerStructSubtype " + treeReaderSchema.getInnerStructSubtype());
                        }
                        if (included[subtype])
                        {
                            this.fields[i] = createTreeReader(subtype, treeReaderSchema, included, skipCorrupt);
                        }
                        // Use the treeReaderSchema evolution name since file/reader types may not have the real column name.
                        this.fieldNames[i] = schemaStructType.GetFieldNames(i);
                    }
                }
            }

            public override void seek(PositionProvider[] index)
            {
                base.seek(index);
                foreach (TreeReader kid in fields)
                {
                    if (kid != null)
                    {
                        kid.seek(index);
                    }
                }
            }

            public override OrcStruct getNext()
            {
                OrcStruct result = null;
                if (hasValue())
                {
                    // TODO: optimize
                    result = new OrcStruct(resultColumnCount);
                    for (int i = 0; i < fileColumnCount; ++i)
                    {
                        if (fields[i] != null)
                        {
                            result.setFieldValue(i, fields[i].next());
                        }
                    }
                    if (resultColumnCount > fileColumnCount)
                    {
                        for (int i = fileColumnCount; i < resultColumnCount; ++i)
                        {
                            // Default new treeReaderSchema evolution fields to NULL.
                            result.setFieldValue(i, null);
                        }
                    }
                }
                return result;
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                ColumnVector[] result;
                if (previousVector == null)
                {
                    result = new ColumnVector[fileColumnCount];
                }
                else
                {
                    result = (ColumnVector[])previousVector;
                }

                // Read all the members of struct as column vectors
                for (int i = 0; i < fileColumnCount; i++)
                {
                    if (fields[i] != null)
                    {
                        if (result[i] == null)
                        {
                            result[i] = (ColumnVector)fields[i].nextVector(null, batchSize);
                        }
                        else
                        {
                            fields[i].nextVector(result[i], batchSize);
                        }
                    }
                }

                // Default additional treeReaderSchema evolution fields to NULL.
                if (vectorColumnCount != -1 && vectorColumnCount > fileColumnCount)
                {
                    for (int i = fileColumnCount; i < vectorColumnCount; ++i)
                    {
                        ColumnVector colVector = result[i];
                        if (colVector != null)
                        {
                            colVector.isRepeating = true;
                            colVector.noNulls = false;
                            colVector.isNull[0] = true;
                        }
                    }
                }

                return result;
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter
            )
            {
                base.startStripe(streams, stripeFooter);
                foreach (TreeReader field in fields)
                {
                    if (field != null)
                    {
                        field.startStripe(streams, stripeFooter);
                    }
                }
            }

            public override void skipRows(long items)
            {
                items = countNonNulls(items);
                foreach (TreeReader field in fields)
                {
                    if (field != null)
                    {
                        field.skipRows(items);
                    }
                }
            }
        }

        sealed public class UnionTreeReader : TreeReader<OrcUnion>
        {
            private TreeReader[] fields;
            private RunLengthByteReader tags;

            public UnionTreeReader(int columnId,
                TreeReaderSchema treeReaderSchema,
                bool[] included,
                bool skipCorrupt)
                : base(columnId)
            {
                OrcProto.Type type = treeReaderSchema.getSchemaTypes()[columnId];
                int fieldCount = type.SubtypesCount;
                this.fields = new TreeReader[fieldCount];
                for (int i = 0; i < fieldCount; ++i)
                {
                    int subtype = (int)type.SubtypesList[i];
                    if (included == null || included[subtype])
                    {
                        this.fields[i] = createTreeReader(subtype, treeReaderSchema, included, skipCorrupt);
                    }
                }
            }

            public override void seek(PositionProvider[] index)
            {
                base.seek(index);
                tags.seek(index[columnId]);
                foreach (TreeReader kid in fields)
                {
                    kid.seek(index);
                }
            }

            public override OrcUnion getNext()
            {
                OrcUnion result = null;
                if (hasValue())
                {
                    result = new OrcUnion();
                    byte tag = tags.next();
                    object previousVal = result.getObject();
                    result.set(tag, fields[tag].next());
                }
                return result;
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                throw new NotSupportedException(
                    "NextVector is not supported operation for Union type");
            }

            public override void startStripe(
                Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter)
            {
                base.startStripe(streams, stripeFooter);
                tags = new RunLengthByteReader(streams.get(new StreamName(columnId,
                    OrcProto.Stream.Types.Kind.DATA)));
                foreach (TreeReader field in fields)
                {
                    if (field != null)
                    {
                        field.startStripe(streams, stripeFooter);
                    }
                }
            }

            public override void skipRows(long items)
            {
                items = countNonNulls(items);
                long[] counts = new long[fields.Length];
                for (int i = 0; i < items; ++i)
                {
                    counts[tags.next()] += 1;
                }
                for (int i = 0; i < counts.Length; ++i)
                {
                    fields[i].skipRows(counts[i]);
                }
            }
        }

        sealed public class ListTreeReader : TreeReader<List<object>>
        {
            private TreeReader elementReader;
            private IntegerReader lengths = null;

            public ListTreeReader(
                int columnId,
                TreeReaderSchema treeReaderSchema,
                bool[] included,
                bool skipCorrupt)
                : base(columnId)
            {
                OrcProto.Type type = treeReaderSchema.getSchemaTypes()[columnId];
                elementReader = createTreeReader((int)type.SubtypesList[0], treeReaderSchema, included, skipCorrupt);
            }

            public override void seek(PositionProvider[] index)
            {
                base.seek(index);
                lengths.seek(index[columnId]);
                elementReader.seek(index);
            }

            public override List<object> getNext()
            {
                List<object> result = null;
                if (hasValue())
                {
                    int length = (int)lengths.next();
                    result = new List<object>(length);

                    // read the new elements into the array
                    for (int i = 0; i < length; i++)
                    {
                        result.Add(elementReader.next());
                    }
                }
                return result;
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                throw new NotSupportedException(
                    "NextVector is not supported operation for List type");
            }

            public override void checkEncoding(OrcProto.ColumnEncoding encoding)
            {
                if ((encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT) &&
                    (encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2))
                {
                    throw new IOException("Unknown encoding " + encoding + " in column " +
                        columnId);
                }
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter
            )
            {
                base.startStripe(streams, stripeFooter);
                lengths = createIntegerReader(stripeFooter.ColumnsList[columnId].Kind,
                    streams.get(new StreamName(columnId,
                        OrcProto.Stream.Types.Kind.LENGTH)), false, false);
                if (elementReader != null)
                {
                    elementReader.startStripe(streams, stripeFooter);
                }
            }

            public override void skipRows(long items)
            {
                items = countNonNulls(items);
                long childSkip = 0;
                for (long i = 0; i < items; ++i)
                {
                    childSkip += lengths.next();
                }
                elementReader.skipRows(childSkip);
            }
        }

        sealed public class MapTreeReader : TreeReader<Dictionary<object, object>>
        {
            private TreeReader keyReader;
            private TreeReader valueReader;
            private IntegerReader lengths = null;

            public MapTreeReader(
                int columnId,
                TreeReaderSchema treeReaderSchema,
                bool[] included,
                bool skipCorrupt)
                : base(columnId)
            {
                OrcProto.Type type = treeReaderSchema.getSchemaTypes()[columnId];
                int keyColumn = (int)type.SubtypesList[0];
                int valueColumn = (int)type.SubtypesList[1];
                if (included == null || included[keyColumn])
                {
                    keyReader = createTreeReader(keyColumn, treeReaderSchema, included, skipCorrupt);
                }
                else
                {
                    keyReader = null;
                }
                if (included == null || included[valueColumn])
                {
                    valueReader = createTreeReader(valueColumn, treeReaderSchema, included, skipCorrupt);
                }
                else
                {
                    valueReader = null;
                }
            }

            public override void seek(PositionProvider[] index)
            {
                base.seek(index);
                lengths.seek(index[columnId]);
                keyReader.seek(index);
                valueReader.seek(index);
            }

            public override Dictionary<object, object> getNext()
            {
                Dictionary<object, object> result = null;
                if (hasValue())
                {
                    int length = (int)lengths.next();
                    result = new Dictionary<object, object>(length);

                    // read the new elements into the array
                    for (int i = 0; i < length; i++)
                    {
                        result[keyReader.next()] = valueReader.next();
                    }
                }
                return result;
            }

            public override object nextVector(object previousVector, long batchSize)
            {
                throw new NotSupportedException(
                    "NextVector is not supported operation for Map type");
            }

            public override void checkEncoding(OrcProto.ColumnEncoding encoding)
            {
                if ((encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT) &&
                    (encoding.Kind != OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2))
                {
                    throw new IOException("Unknown encoding " + encoding + " in column " +
                        columnId);
                }
            }

            public override void startStripe(Dictionary<StreamName, InStream> streams,
                OrcProto.StripeFooter stripeFooter
            )
            {
                base.startStripe(streams, stripeFooter);
                lengths = createIntegerReader(stripeFooter.ColumnsList[columnId].Kind,
                    streams.get(new StreamName(columnId,
                        OrcProto.Stream.Types.Kind.LENGTH)), false, false);
                if (keyReader != null)
                {
                    keyReader.startStripe(streams, stripeFooter);
                }
                if (valueReader != null)
                {
                    valueReader.startStripe(streams, stripeFooter);
                }
            }

            public override void skipRows(long items)
            {
                items = countNonNulls(items);
                long childSkip = 0;
                for (long i = 0; i < items; ++i)
                {
                    childSkip += lengths.next();
                }
                keyReader.skipRows(childSkip);
                valueReader.skipRows(childSkip);
            }
        }

        public static TreeReader createTreeReader(int columnId,
            TreeReaderSchema treeReaderSchema,
            bool[] included,
            bool skipCorrupt)
        {
            OrcProto.Type type = treeReaderSchema.getSchemaTypes()[columnId];
            switch (type.Kind)
            {
                case OrcProto.Type.Types.Kind.BOOLEAN:
                    return new BooleanTreeReader(columnId);
                case OrcProto.Type.Types.Kind.BYTE:
                    return new ByteTreeReader(columnId);
                case OrcProto.Type.Types.Kind.DOUBLE:
                    return new DoubleTreeReader(columnId);
                case OrcProto.Type.Types.Kind.FLOAT:
                    return new FloatTreeReader(columnId);
                case OrcProto.Type.Types.Kind.SHORT:
                    return new ShortTreeReader(columnId);
                case OrcProto.Type.Types.Kind.INT:
                    return new IntTreeReader(columnId);
                case OrcProto.Type.Types.Kind.LONG:
                    return new LongTreeReader(columnId, skipCorrupt);
                case OrcProto.Type.Types.Kind.STRING:
                    return new StringTreeReader(columnId);
                case OrcProto.Type.Types.Kind.CHAR:
                    if (!type.HasMaximumLength)
                    {
                        throw new ArgumentException("ORC char type has no length specified");
                    }
                    return new CharTreeReader(columnId, (int)type.MaximumLength);
                case OrcProto.Type.Types.Kind.VARCHAR:
                    if (!type.HasMaximumLength)
                    {
                        throw new ArgumentException("ORC varchar type has no length specified");
                    }
                    return new VarcharTreeReader(columnId, (int)type.MaximumLength);
                case OrcProto.Type.Types.Kind.BINARY:
                    return new BinaryTreeReader(columnId);
                case OrcProto.Type.Types.Kind.TIMESTAMP:
                    return new TimestampTreeReader(columnId, skipCorrupt);
                case OrcProto.Type.Types.Kind.DATE:
                    return new DateTreeReader(columnId);
                case OrcProto.Type.Types.Kind.DECIMAL:
                    int precision =
                        type.HasPrecision ? (int)type.Precision : HiveDecimal.SYSTEM_DEFAULT_PRECISION;
                    int scale = type.HasScale ? (int)type.Scale : HiveDecimal.SYSTEM_DEFAULT_SCALE;
                    return new DecimalTreeReader(columnId, precision, scale);
                case OrcProto.Type.Types.Kind.STRUCT:
                    return new StructTreeReader(columnId, treeReaderSchema, included, skipCorrupt);
                case OrcProto.Type.Types.Kind.LIST:
                    return new ListTreeReader(columnId, treeReaderSchema, included, skipCorrupt);
                case OrcProto.Type.Types.Kind.MAP:
                    return new MapTreeReader(columnId, treeReaderSchema, included, skipCorrupt);
                case OrcProto.Type.Types.Kind.UNION:
                    return new UnionTreeReader(columnId, treeReaderSchema, included, skipCorrupt);
                default:
                    throw new ArgumentException("Unsupported type " +
                        type.Kind);
            }
        }
    }
}
