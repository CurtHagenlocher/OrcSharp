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

namespace org.apache.hadoop.hive.ql.io.orc.encoded
{
    using OrcProto = global::orc.proto;
    using ColumnStreamData = EncodedColumnBatch<OrcBatchKey>.ColumnStreamData;
    using System.Collections.Generic;
    using System;

    public class EncodedTreeReaderFactory : TreeReaderFactory
    {
        static class Kind
        {
            public const int BLOOM_FILTER_VALUE = (int)OrcProto.Stream.Types.Kind.BLOOM_FILTER;
            public const int DATA_VALUE = (int)OrcProto.Stream.Types.Kind.DATA;
            public const int DICTIONARY_COUNT_VALUE = (int)OrcProto.Stream.Types.Kind.DICTIONARY_COUNT;
            public const int DICTIONARY_DATA_VALUE = (int)OrcProto.Stream.Types.Kind.DICTIONARY_DATA;
            public const int LENGTH_VALUE = (int)OrcProto.Stream.Types.Kind.LENGTH;
            public const int PRESENT_VALUE = (int)OrcProto.Stream.Types.Kind.PRESENT;
            public const int SECONDARY_VALUE = (int)OrcProto.Stream.Types.Kind.SECONDARY;
        }

        /**
         * We choose to use a toy programming language, so we cannot use multiple inheritance.
         * If we could, we could have this inherit TreeReader to contain the common impl, and then
         * have e.g. SettableIntTreeReader inherit both Settable... and Int.. TreeReader-s.
         * Instead, we have a settable interface that the caller will cast to and call setBuffers.
         */
        public interface SettableTreeReader
        {
            void setBuffers(ColumnStreamData[] streamBuffers, bool sameStripe);
        }

        protected class TimestampStreamReader : TimestampTreeReader, SettableTreeReader
        {
            private bool isFileCompressed;
            private SettableUncompressedStream _presentStream;
            private SettableUncompressedStream _secondsStream;
            private SettableUncompressedStream _nanosStream;

            private TimestampStreamReader(int columnId, SettableUncompressedStream present,
                SettableUncompressedStream data, SettableUncompressedStream nanos, bool isFileCompressed,
                OrcProto.ColumnEncoding encoding, bool skipCorrupt) :
                base(columnId, present, data, nanos, encoding, skipCorrupt)
            {
                this.isFileCompressed = isFileCompressed;
                this._presentStream = present;
                this._secondsStream = data;
                this._nanosStream = nanos;
            }

            public void seek(PositionProvider index)
            {
                if (present != null)
                {
                    if (isFileCompressed)
                    {
                        index.getNext();
                    }
                    present.seek(index);
                }

                // data stream could be empty stream or already reached end of stream before present stream.
                // This can happen if all values in stream are nulls or last row group values are all null.
                if (_secondsStream.available() > 0)
                {
                    if (isFileCompressed)
                    {
                        index.getNext();
                    }
                    data.seek(index);
                }

                if (_nanosStream.available() > 0)
                {
                    if (isFileCompressed)
                    {
                        index.getNext();
                    }
                    nanos.seek(index);
                }
            }

            public void setBuffers(ColumnStreamData[] streamsData, bool sameStripe)
            {
                if (_presentStream != null)
                {
                    _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.PRESENT_VALUE]));
                }
                if (_secondsStream != null)
                {
                    _secondsStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DATA_VALUE]));
                }
                if (_nanosStream != null)
                {
                    _nanosStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.SECONDARY_VALUE]));
                }
            }

            public class StreamReaderBuilder
            {
                private long? fileId;
                private int columnIndex;
                private ColumnStreamData presentStream;
                private ColumnStreamData dataStream;
                private ColumnStreamData nanosStream;
                private CompressionCodec compressionCodec;
                private OrcProto.ColumnEncoding columnEncoding;
                private bool _skipCorrupt;

                public StreamReaderBuilder setFileId(long? fileId)
                {
                    this.fileId = fileId;
                    return this;
                }

                public StreamReaderBuilder setColumnIndex(int columnIndex)
                {
                    this.columnIndex = columnIndex;
                    return this;
                }

                public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream)
                {
                    this.presentStream = presentStream;
                    return this;
                }

                public StreamReaderBuilder setSecondsStream(ColumnStreamData dataStream)
                {
                    this.dataStream = dataStream;
                    return this;
                }

                public StreamReaderBuilder setNanosStream(ColumnStreamData secondaryStream)
                {
                    this.nanosStream = secondaryStream;
                    return this;
                }

                public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec)
                {
                    this.compressionCodec = compressionCodec;
                    return this;
                }

                public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding)
                {
                    this.columnEncoding = encoding;
                    return this;
                }

                public StreamReaderBuilder skipCorrupt(bool skipCorrupt)
                {
                    this._skipCorrupt = skipCorrupt;
                    return this;
                }

                public TimestampStreamReader build()
                {
                    SettableUncompressedStream present = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.PRESENT.ToString(),
                            fileId, presentStream);

                    SettableUncompressedStream data = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.DATA.ToString(), fileId,
                            dataStream);

                    SettableUncompressedStream nanos = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.SECONDARY.ToString(),
                            fileId, nanosStream);

                    bool isFileCompressed = compressionCodec != null;
                    return new TimestampStreamReader(columnIndex, present, data, nanos,
                        isFileCompressed, columnEncoding, _skipCorrupt);
                }
            }

            public static StreamReaderBuilder builder()
            {
                return new StreamReaderBuilder();
            }
        }

        protected class StringStreamReader : StringTreeReader, SettableTreeReader
        {
            private bool _isFileCompressed;
            private bool _isDictionaryEncoding;
            private SettableUncompressedStream _presentStream;
            private SettableUncompressedStream _dataStream;
            private SettableUncompressedStream _lengthStream;
            private SettableUncompressedStream _dictionaryStream;

            private StringStreamReader(int columnId, SettableUncompressedStream present,
                SettableUncompressedStream data, SettableUncompressedStream length,
                SettableUncompressedStream dictionary,
                bool isFileCompressed, OrcProto.ColumnEncoding encoding)
                : base(columnId, present, data, length, dictionary, encoding)
            {
                this._isDictionaryEncoding = dictionary != null;
                this._isFileCompressed = isFileCompressed;
                this._presentStream = present;
                this._dataStream = data;
                this._lengthStream = length;
                this._dictionaryStream = dictionary;
            }

            public void seek(PositionProvider index)
            {
                if (present != null)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    reader.getPresent().seek(index);
                }

                if (_isDictionaryEncoding)
                {
                    // DICTIONARY encoding

                    // data stream could be empty stream or already reached end of stream before present stream.
                    // This can happen if all values in stream are nulls or last row group values are all null.
                    if (_dataStream.available() > 0)
                    {
                        if (_isFileCompressed)
                        {
                            index.getNext();
                        }
                        ((StringDictionaryTreeReader)reader).getReader().seek(index);
                    }
                }
                else
                {
                    // DIRECT encoding

                    // data stream could be empty stream or already reached end of stream before present stream.
                    // This can happen if all values in stream are nulls or last row group values are all null.
                    if (_dataStream.available() > 0)
                    {
                        if (_isFileCompressed)
                        {
                            index.getNext();
                        }
                        ((StringDirectTreeReader)reader).getStream().seek(index);
                    }

                    if (_lengthStream.available() > 0)
                    {
                        if (_isFileCompressed)
                        {
                            index.getNext();
                        }
                        ((StringDirectTreeReader)reader).getLengths().seek(index);
                    }
                }
            }

            public void setBuffers(ColumnStreamData[] streamsData, bool sameStripe)
            {
                if (_presentStream != null)
                {
                    _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.PRESENT_VALUE]));
                }
                if (_dataStream != null)
                {
                    _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DATA_VALUE]));
                }
                if (!_isDictionaryEncoding)
                {
                    if (_lengthStream != null)
                    {
                        _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.LENGTH_VALUE]));
                    }
                }

                // set these streams only if the stripe is different
                if (!sameStripe && _isDictionaryEncoding)
                {
                    if (_lengthStream != null)
                    {
                        _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.LENGTH_VALUE]));
                    }
                    if (_dictionaryStream != null)
                    {
                        _dictionaryStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DICTIONARY_DATA_VALUE]));
                    }
                }
            }

            public class StreamReaderBuilder
            {
                private long? fileId;
                private int columnIndex;
                private ColumnStreamData presentStream;
                private ColumnStreamData dataStream;
                private ColumnStreamData dictionaryStream;
                private ColumnStreamData lengthStream;
                private CompressionCodec compressionCodec;
                private OrcProto.ColumnEncoding columnEncoding;

                public StreamReaderBuilder setFileId(long? fileId)
                {
                    this.fileId = fileId;
                    return this;
                }

                public StreamReaderBuilder setColumnIndex(int columnIndex)
                {
                    this.columnIndex = columnIndex;
                    return this;
                }

                public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream)
                {
                    this.presentStream = presentStream;
                    return this;
                }

                public StreamReaderBuilder setDataStream(ColumnStreamData dataStream)
                {
                    this.dataStream = dataStream;
                    return this;
                }

                public StreamReaderBuilder setLengthStream(ColumnStreamData lengthStream)
                {
                    this.lengthStream = lengthStream;
                    return this;
                }

                public StreamReaderBuilder setDictionaryStream(ColumnStreamData dictStream)
                {
                    this.dictionaryStream = dictStream;
                    return this;
                }

                public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec)
                {
                    this.compressionCodec = compressionCodec;
                    return this;
                }

                public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding)
                {
                    this.columnEncoding = encoding;
                    return this;
                }

                public StringStreamReader build()
                {
                    SettableUncompressedStream present = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.PRESENT.ToString(),
                            fileId, presentStream);

                    SettableUncompressedStream data = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.DATA.ToString(), fileId,
                            dataStream);

                    SettableUncompressedStream length = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.LENGTH.ToString(), fileId,
                            lengthStream);

                    SettableUncompressedStream dictionary = StreamUtils.createSettableUncompressedStream(
                        OrcProto.Stream.Types.Kind.DICTIONARY_DATA.ToString(), fileId, dictionaryStream);

                    bool isFileCompressed = compressionCodec != null;
                    return new StringStreamReader(columnIndex, present, data, length, dictionary,
                        isFileCompressed, columnEncoding);
                }
            }

            public static StreamReaderBuilder builder()
            {
                return new StreamReaderBuilder();
            }

        }

        protected class ShortStreamReader : ShortTreeReader, SettableTreeReader
        {
            private bool isFileCompressed;
            private SettableUncompressedStream _presentStream;
            private SettableUncompressedStream _dataStream;

            private ShortStreamReader(int columnId, SettableUncompressedStream present,
                SettableUncompressedStream data, bool isFileCompressed,
                OrcProto.ColumnEncoding encoding)
                : base(columnId, present, data, encoding)
            {
                this.isFileCompressed = isFileCompressed;
                this._presentStream = present;
                this._dataStream = data;
            }

            public void seek(PositionProvider index)
            {
                if (present != null)
                {
                    if (isFileCompressed)
                    {
                        index.getNext();
                    }
                    present.seek(index);
                }

                // data stream could be empty stream or already reached end of stream before present stream.
                // This can happen if all values in stream are nulls or last row group values are all null.
                if (_dataStream.available() > 0)
                {
                    if (isFileCompressed)
                    {
                        index.getNext();
                    }
                    reader.seek(index);
                }
            }

            public void setBuffers(ColumnStreamData[] streamsData, bool sameStripe)
            {
                if (_presentStream != null)
                {
                    _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.PRESENT_VALUE]));
                }
                if (_dataStream != null)
                {
                    _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DATA_VALUE]));
                }
            }

            public class StreamReaderBuilder
            {
                private long? fileId;
                private int columnIndex;
                private ColumnStreamData presentStream;
                private ColumnStreamData dataStream;
                private CompressionCodec compressionCodec;
                private OrcProto.ColumnEncoding columnEncoding;

                public StreamReaderBuilder setFileId(long? fileId)
                {
                    this.fileId = fileId;
                    return this;
                }

                public StreamReaderBuilder setColumnIndex(int columnIndex)
                {
                    this.columnIndex = columnIndex;
                    return this;
                }

                public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream)
                {
                    this.presentStream = presentStream;
                    return this;
                }

                public StreamReaderBuilder setDataStream(ColumnStreamData dataStream)
                {
                    this.dataStream = dataStream;
                    return this;
                }

                public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec)
                {
                    this.compressionCodec = compressionCodec;
                    return this;
                }

                public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding)
                {
                    this.columnEncoding = encoding;
                    return this;
                }

                public ShortStreamReader build()
                {
                    SettableUncompressedStream present = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.PRESENT.ToString(),
                            fileId, presentStream);

                    SettableUncompressedStream data = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.DATA.ToString(), fileId,
                            dataStream);

                    bool isFileCompressed = compressionCodec != null;
                    return new ShortStreamReader(columnIndex, present, data, isFileCompressed,
                        columnEncoding);
                }
            }

            public static StreamReaderBuilder builder()
            {
                return new StreamReaderBuilder();
            }
        }

        protected class LongStreamReader : LongTreeReader, SettableTreeReader
        {
            private bool _isFileCompressed;
            private SettableUncompressedStream _presentStream;
            private SettableUncompressedStream _dataStream;

            private LongStreamReader(int columnId, SettableUncompressedStream present,
                SettableUncompressedStream data, bool isFileCompressed,
                OrcProto.ColumnEncoding encoding, bool skipCorrupt)
                : base(columnId, present, data, encoding, skipCorrupt)
            {
                this._isFileCompressed = isFileCompressed;
                this._presentStream = present;
                this._dataStream = data;
            }

            public void seek(PositionProvider index)
            {
                if (present != null)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    present.seek(index);
                }

                // data stream could be empty stream or already reached end of stream before present stream.
                // This can happen if all values in stream are nulls or last row group values are all null.
                if (_dataStream.available() > 0)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    reader.seek(index);
                }
            }

            public void setBuffers(ColumnStreamData[] streamsData, bool sameStripe)
            {
                if (_presentStream != null)
                {
                    _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.PRESENT_VALUE]));
                }
                if (_dataStream != null)
                {
                    _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DATA_VALUE]));
                }
            }

            public class StreamReaderBuilder
            {
                private long? fileId;
                private int columnIndex;
                private ColumnStreamData presentStream;
                private ColumnStreamData dataStream;
                private CompressionCodec compressionCodec;
                private OrcProto.ColumnEncoding columnEncoding;
                private bool _skipCorrupt;

                public StreamReaderBuilder setFileId(long? fileId)
                {
                    this.fileId = fileId;
                    return this;
                }

                public StreamReaderBuilder setColumnIndex(int columnIndex)
                {
                    this.columnIndex = columnIndex;
                    return this;
                }

                public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream)
                {
                    this.presentStream = presentStream;
                    return this;
                }

                public StreamReaderBuilder setDataStream(ColumnStreamData dataStream)
                {
                    this.dataStream = dataStream;
                    return this;
                }

                public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec)
                {
                    this.compressionCodec = compressionCodec;
                    return this;
                }

                public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding)
                {
                    this.columnEncoding = encoding;
                    return this;
                }

                public StreamReaderBuilder skipCorrupt(bool skipCorrupt)
                {
                    this._skipCorrupt = skipCorrupt;
                    return this;
                }

                public LongStreamReader build()
                {
                    SettableUncompressedStream present = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.PRESENT.ToString(),
                            fileId, presentStream);

                    SettableUncompressedStream data = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.DATA.ToString(), fileId,
                            dataStream);

                    bool isFileCompressed = compressionCodec != null;
                    return new LongStreamReader(columnIndex, present, data, isFileCompressed,
                        columnEncoding, _skipCorrupt);
                }
            }

            public static StreamReaderBuilder builder()
            {
                return new StreamReaderBuilder();
            }
        }

        protected class IntStreamReader : IntTreeReader, SettableTreeReader
        {
            private bool _isFileCompressed;
            private SettableUncompressedStream _presentStream;
            private SettableUncompressedStream _dataStream;

            private IntStreamReader(int columnId, SettableUncompressedStream present,
                SettableUncompressedStream data, bool isFileCompressed,
                OrcProto.ColumnEncoding encoding)
                : base(columnId, present, data, encoding)
            {
                this._isFileCompressed = isFileCompressed;
                this._dataStream = data;
                this._presentStream = present;
            }

            public void seek(PositionProvider index)
            {
                if (present != null)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    present.seek(index);
                }

                // data stream could be empty stream or already reached end of stream before present stream.
                // This can happen if all values in stream are nulls or last row group values are all null.
                if (_dataStream.available() > 0)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    reader.seek(index);
                }
            }

            public void setBuffers(ColumnStreamData[] streamsData, bool sameStripe)
            {
                if (_presentStream != null)
                {
                    _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.PRESENT_VALUE]));
                }
                if (_dataStream != null)
                {
                    _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DATA_VALUE]));
                }
            }

            public class StreamReaderBuilder
            {
                private long? fileId;
                private int columnIndex;
                private ColumnStreamData presentStream;
                private ColumnStreamData dataStream;
                private CompressionCodec compressionCodec;
                private OrcProto.ColumnEncoding columnEncoding;

                public StreamReaderBuilder setFileId(long? fileId)
                {
                    this.fileId = fileId;
                    return this;
                }

                public StreamReaderBuilder setColumnIndex(int columnIndex)
                {
                    this.columnIndex = columnIndex;
                    return this;
                }

                public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream)
                {
                    this.presentStream = presentStream;
                    return this;
                }

                public StreamReaderBuilder setDataStream(ColumnStreamData dataStream)
                {
                    this.dataStream = dataStream;
                    return this;
                }

                public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec)
                {
                    this.compressionCodec = compressionCodec;
                    return this;
                }

                public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding)
                {
                    this.columnEncoding = encoding;
                    return this;
                }

                public IntStreamReader build()
                {
                    SettableUncompressedStream present = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.PRESENT.ToString(),
                            fileId, presentStream);

                    SettableUncompressedStream data = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.DATA.ToString(), fileId,
                            dataStream);

                    bool isFileCompressed = compressionCodec != null;
                    return new IntStreamReader(columnIndex, present, data, isFileCompressed,
                        columnEncoding);
                }
            }

            public static StreamReaderBuilder builder()
            {
                return new StreamReaderBuilder();
            }

        }

        protected class FloatStreamReader : FloatTreeReader, SettableTreeReader
        {
            private bool _isFileCompressed;
            private SettableUncompressedStream _presentStream;
            private SettableUncompressedStream _dataStream;

            private FloatStreamReader(int columnId, SettableUncompressedStream present,
                SettableUncompressedStream data, bool isFileCompressed)
                : base(columnId, present, data)
            {
                this._isFileCompressed = isFileCompressed;
                this._presentStream = present;
                this._dataStream = data;
            }

            public void seek(PositionProvider index)
            {
                if (present != null)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    present.seek(index);
                }

                // data stream could be empty stream or already reached end of stream before present stream.
                // This can happen if all values in stream are nulls or last row group values are all null.
                if (_dataStream.available() > 0)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    stream.seek(index);
                }
            }

            public void setBuffers(ColumnStreamData[] streamsData, bool sameStripe)
            {
                if (_presentStream != null)
                {
                    _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.PRESENT_VALUE]));
                }
                if (_dataStream != null)
                {
                    _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DATA_VALUE]));
                }
            }

            public class StreamReaderBuilder
            {
                private long? fileId;
                private int columnIndex;
                private ColumnStreamData presentStream;
                private ColumnStreamData dataStream;
                private CompressionCodec compressionCodec;

                public StreamReaderBuilder setFileId(long? fileId)
                {
                    this.fileId = fileId;
                    return this;
                }

                public StreamReaderBuilder setColumnIndex(int columnIndex)
                {
                    this.columnIndex = columnIndex;
                    return this;
                }

                public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream)
                {
                    this.presentStream = presentStream;
                    return this;
                }

                public StreamReaderBuilder setDataStream(ColumnStreamData dataStream)
                {
                    this.dataStream = dataStream;
                    return this;
                }

                public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec)
                {
                    this.compressionCodec = compressionCodec;
                    return this;
                }

                public FloatStreamReader build()
                {
                    SettableUncompressedStream present = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.PRESENT.ToString(),
                            fileId, presentStream);

                    SettableUncompressedStream data = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.DATA.ToString(), fileId,
                            dataStream);

                    bool isFileCompressed = compressionCodec != null;
                    return new FloatStreamReader(columnIndex, present, data, isFileCompressed);
                }
            }

            public static StreamReaderBuilder builder()
            {
                return new StreamReaderBuilder();
            }

        }

        protected class DoubleStreamReader : DoubleTreeReader, SettableTreeReader
        {
            private bool _isFileCompressed;
            private SettableUncompressedStream _presentStream;
            private SettableUncompressedStream _dataStream;

            private DoubleStreamReader(int columnId, SettableUncompressedStream present,
                SettableUncompressedStream data, bool isFileCompressed)
                : base(columnId, present, data)
            {
                this._isFileCompressed = isFileCompressed;
                this._presentStream = present;
                this._dataStream = data;
            }

            public void seek(PositionProvider index)
            {
                if (present != null)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    present.seek(index);
                }

                // data stream could be empty stream or already reached end of stream before present stream.
                // This can happen if all values in stream are nulls or last row group values are all null.
                if (_dataStream.available() > 0)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    stream.seek(index);
                }
            }

            public void setBuffers(ColumnStreamData[] streamsData, bool sameStripe)
            {
                if (_presentStream != null)
                {
                    _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.PRESENT_VALUE]));
                }
                if (_dataStream != null)
                {
                    _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DATA_VALUE]));
                }
            }

            public class StreamReaderBuilder
            {
                private long? fileId;
                private int columnIndex;
                private ColumnStreamData presentStream;
                private ColumnStreamData dataStream;
                private CompressionCodec compressionCodec;

                public StreamReaderBuilder setFileId(long? fileId)
                {
                    this.fileId = fileId;
                    return this;
                }

                public StreamReaderBuilder setColumnIndex(int columnIndex)
                {
                    this.columnIndex = columnIndex;
                    return this;
                }

                public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream)
                {
                    this.presentStream = presentStream;
                    return this;
                }

                public StreamReaderBuilder setDataStream(ColumnStreamData dataStream)
                {
                    this.dataStream = dataStream;
                    return this;
                }

                public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec)
                {
                    this.compressionCodec = compressionCodec;
                    return this;
                }

                public DoubleStreamReader build()
                {
                    SettableUncompressedStream present = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.PRESENT.ToString(),
                            fileId, presentStream);

                    SettableUncompressedStream data = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.DATA.ToString(), fileId,
                            dataStream);

                    bool isFileCompressed = compressionCodec != null;
                    return new DoubleStreamReader(columnIndex, present, data, isFileCompressed);
                }
            }

            public static StreamReaderBuilder builder()
            {
                return new StreamReaderBuilder();
            }
        }

        protected class DecimalStreamReader : DecimalTreeReader, SettableTreeReader
        {
            private bool _isFileCompressed;
            private SettableUncompressedStream _presentStream;
            private SettableUncompressedStream _valueStream;
            private SettableUncompressedStream _scaleStream;

            private DecimalStreamReader(int columnId, int precision, int scale,
                SettableUncompressedStream presentStream,
                SettableUncompressedStream valueStream, SettableUncompressedStream scaleStream,
                bool isFileCompressed,
                OrcProto.ColumnEncoding encoding)
                : base(columnId, precision, scale, presentStream, valueStream, scaleStream, encoding)
            {
                this._isFileCompressed = isFileCompressed;
                this._presentStream = presentStream;
                this._valueStream = valueStream;
                this._scaleStream = scaleStream;
            }

            public void seek(PositionProvider index)
            {
                if (present != null)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    present.seek(index);
                }

                // data stream could be empty stream or already reached end of stream before present stream.
                // This can happen if all values in stream are nulls or last row group values are all null.
                if (_valueStream.available() > 0)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    valueStream.seek(index);
                }

                if (_scaleStream.available() > 0)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    scaleReader.seek(index);
                }
            }

            public void setBuffers(ColumnStreamData[] streamsData, bool sameStripe)
            {
                if (_presentStream != null)
                {
                    _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.PRESENT_VALUE]));
                }
                if (_valueStream != null)
                {
                    _valueStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DATA_VALUE]));
                }
                if (_scaleStream != null)
                {
                    _scaleStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.SECONDARY_VALUE]));
                }
            }

            public class StreamReaderBuilder
            {
                private long? fileId;
                private int columnIndex;
                private ColumnStreamData presentStream;
                private ColumnStreamData valueStream;
                private ColumnStreamData scaleStream;
                private int scale;
                private int precision;
                private CompressionCodec compressionCodec;
                private OrcProto.ColumnEncoding columnEncoding;

                public StreamReaderBuilder setFileId(long? fileId)
                {
                    this.fileId = fileId;
                    return this;
                }

                public StreamReaderBuilder setColumnIndex(int columnIndex)
                {
                    this.columnIndex = columnIndex;
                    return this;
                }

                public StreamReaderBuilder setPrecision(int precision)
                {
                    this.precision = precision;
                    return this;
                }

                public StreamReaderBuilder setScale(int scale)
                {
                    this.scale = scale;
                    return this;
                }

                public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream)
                {
                    this.presentStream = presentStream;
                    return this;
                }

                public StreamReaderBuilder setValueStream(ColumnStreamData valueStream)
                {
                    this.valueStream = valueStream;
                    return this;
                }

                public StreamReaderBuilder setScaleStream(ColumnStreamData scaleStream)
                {
                    this.scaleStream = scaleStream;
                    return this;
                }

                public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec)
                {
                    this.compressionCodec = compressionCodec;
                    return this;
                }

                public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding)
                {
                    this.columnEncoding = encoding;
                    return this;
                }

                public DecimalStreamReader build()
                {
                    SettableUncompressedStream presentInStream = StreamUtils.createSettableUncompressedStream(
                        OrcProto.Stream.Types.Kind.PRESENT.ToString(), fileId, presentStream);

                    SettableUncompressedStream valueInStream = StreamUtils.createSettableUncompressedStream(
                        OrcProto.Stream.Types.Kind.DATA.ToString(), fileId, valueStream);

                    SettableUncompressedStream scaleInStream = StreamUtils.createSettableUncompressedStream(
                        OrcProto.Stream.Types.Kind.SECONDARY.ToString(), fileId, scaleStream);

                    bool isFileCompressed = compressionCodec != null;
                    return new DecimalStreamReader(columnIndex, precision, scale, presentInStream,
                        valueInStream,
                        scaleInStream, isFileCompressed, columnEncoding);
                }
            }

            public static StreamReaderBuilder builder()
            {
                return new StreamReaderBuilder();
            }
        }

        protected class DateStreamReader : DateTreeReader, SettableTreeReader
        {
            private bool isFileCompressed;
            private SettableUncompressedStream _presentStream;
            private SettableUncompressedStream _dataStream;

            private DateStreamReader(int columnId, SettableUncompressedStream present,
                SettableUncompressedStream data, bool isFileCompressed,
                OrcProto.ColumnEncoding encoding)
                : base(columnId, present, data, encoding)
            {
                this.isFileCompressed = isFileCompressed;
                this._presentStream = present;
                this._dataStream = data;
            }

            public void seek(PositionProvider index)
            {
                if (present != null)
                {
                    if (isFileCompressed)
                    {
                        index.getNext();
                    }
                    present.seek(index);
                }

                // data stream could be empty stream or already reached end of stream before present stream.
                // This can happen if all values in stream are nulls or last row group values are all null.
                if (_dataStream.available() > 0)
                {
                    if (isFileCompressed)
                    {
                        index.getNext();
                    }
                    reader.seek(index);
                }
            }

            public void setBuffers(ColumnStreamData[] streamsData, bool sameStripe)
            {
                if (_presentStream != null)
                {
                    _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.PRESENT_VALUE]));
                }
                if (_dataStream != null)
                {
                    _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DATA_VALUE]));
                }
            }

            public class StreamReaderBuilder
            {
                private long? fileId;
                private int columnIndex;
                private ColumnStreamData presentStream;
                private ColumnStreamData dataStream;
                private CompressionCodec compressionCodec;
                private OrcProto.ColumnEncoding columnEncoding;

                public StreamReaderBuilder setFileId(long? fileId)
                {
                    this.fileId = fileId;
                    return this;
                }

                public StreamReaderBuilder setColumnIndex(int columnIndex)
                {
                    this.columnIndex = columnIndex;
                    return this;
                }

                public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream)
                {
                    this.presentStream = presentStream;
                    return this;
                }

                public StreamReaderBuilder setDataStream(ColumnStreamData dataStream)
                {
                    this.dataStream = dataStream;
                    return this;
                }

                public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec)
                {
                    this.compressionCodec = compressionCodec;
                    return this;
                }

                public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding)
                {
                    this.columnEncoding = encoding;
                    return this;
                }

                public DateStreamReader build()
                {
                    SettableUncompressedStream present = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.PRESENT.ToString(),
                            fileId, presentStream);


                    SettableUncompressedStream data = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.DATA.ToString(), fileId,
                            dataStream);

                    bool isFileCompressed = compressionCodec != null;
                    return new DateStreamReader(columnIndex, present, data, isFileCompressed,
                        columnEncoding);
                }
            }

            public static StreamReaderBuilder builder()
            {
                return new StreamReaderBuilder();
            }
        }

        protected class CharStreamReader : CharTreeReader, SettableTreeReader
        {
            private bool _isFileCompressed;
            private bool _isDictionaryEncoding;
            private SettableUncompressedStream _presentStream;
            private SettableUncompressedStream _dataStream;
            private SettableUncompressedStream _lengthStream;
            private SettableUncompressedStream _dictionaryStream;

            private CharStreamReader(int columnId, int maxLength,
                SettableUncompressedStream present, SettableUncompressedStream data,
                SettableUncompressedStream length, SettableUncompressedStream dictionary,
                bool isFileCompressed, OrcProto.ColumnEncoding encoding)
                : base(columnId, maxLength, present, data, length,
                    dictionary, encoding)
            {
                this._isDictionaryEncoding = dictionary != null;
                this._isFileCompressed = isFileCompressed;
                this._presentStream = present;
                this._dataStream = data;
                this._lengthStream = length;
                this._dictionaryStream = dictionary;
            }

            public void seek(PositionProvider index)
            {
                if (present != null)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    reader.getPresent().seek(index);
                }

                if (_isDictionaryEncoding)
                {
                    // DICTIONARY encoding

                    // data stream could be empty stream or already reached end of stream before present stream.
                    // This can happen if all values in stream are nulls or last row group values are all null.
                    if (_dataStream.available() > 0)
                    {
                        if (_isFileCompressed)
                        {
                            index.getNext();
                        }
                        ((StringDictionaryTreeReader)reader).getReader().seek(index);
                    }
                }
                else
                {
                    // DIRECT encoding

                    // data stream could be empty stream or already reached end of stream before present stream.
                    // This can happen if all values in stream are nulls or last row group values are all null.
                    if (_dataStream.available() > 0)
                    {
                        if (_isFileCompressed)
                        {
                            index.getNext();
                        }
                        ((StringDirectTreeReader)reader).getStream().seek(index);
                    }

                    if (_lengthStream.available() > 0)
                    {
                        if (_isFileCompressed)
                        {
                            index.getNext();
                        }
                        ((StringDirectTreeReader)reader).getLengths().seek(index);
                    }
                }
            }

            public void setBuffers(ColumnStreamData[] streamsData, bool sameStripe)
            {
                if (_presentStream != null)
                {
                    _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.PRESENT_VALUE]));
                }
                if (_dataStream != null)
                {
                    _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DATA_VALUE]));
                }
                if (!_isDictionaryEncoding)
                {
                    if (_lengthStream != null)
                    {
                        _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.LENGTH_VALUE]));
                    }
                }

                // set these streams only if the stripe is different
                if (!sameStripe && _isDictionaryEncoding)
                {
                    if (_lengthStream != null)
                    {
                        _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.LENGTH_VALUE]));
                    }
                    if (_dictionaryStream != null)
                    {
                        _dictionaryStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DICTIONARY_DATA_VALUE]));
                    }
                }
            }

            public class StreamReaderBuilder
            {
                private long? fileId;
                private int columnIndex;
                private int maxLength;
                private ColumnStreamData presentStream;
                private ColumnStreamData dataStream;
                private ColumnStreamData dictionaryStream;
                private ColumnStreamData lengthStream;
                private CompressionCodec compressionCodec;
                private OrcProto.ColumnEncoding columnEncoding;

                public StreamReaderBuilder setFileId(long? fileId)
                {
                    this.fileId = fileId;
                    return this;
                }

                public StreamReaderBuilder setColumnIndex(int columnIndex)
                {
                    this.columnIndex = columnIndex;
                    return this;
                }

                public StreamReaderBuilder setMaxLength(int maxLength)
                {
                    this.maxLength = maxLength;
                    return this;
                }

                public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream)
                {
                    this.presentStream = presentStream;
                    return this;
                }

                public StreamReaderBuilder setDataStream(ColumnStreamData dataStream)
                {
                    this.dataStream = dataStream;
                    return this;
                }

                public StreamReaderBuilder setLengthStream(ColumnStreamData lengthStream)
                {
                    this.lengthStream = lengthStream;
                    return this;
                }

                public StreamReaderBuilder setDictionaryStream(ColumnStreamData dictStream)
                {
                    this.dictionaryStream = dictStream;
                    return this;
                }

                public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec)
                {
                    this.compressionCodec = compressionCodec;
                    return this;
                }

                public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding)
                {
                    this.columnEncoding = encoding;
                    return this;
                }

                public CharStreamReader build()
                {
                    SettableUncompressedStream present = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.PRESENT.ToString(),
                            fileId, presentStream);

                    SettableUncompressedStream data = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.DATA.ToString(), fileId,
                            dataStream);

                    SettableUncompressedStream length = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.LENGTH.ToString(), fileId,
                            lengthStream);

                    SettableUncompressedStream dictionary = StreamUtils.createSettableUncompressedStream(
                        OrcProto.Stream.Types.Kind.DICTIONARY_DATA.ToString(), fileId, dictionaryStream);

                    bool isFileCompressed = compressionCodec != null;
                    return new CharStreamReader(columnIndex, maxLength, present, data, length,
                        dictionary, isFileCompressed, columnEncoding);
                }
            }

            public static StreamReaderBuilder builder()
            {
                return new StreamReaderBuilder();
            }

        }

        protected class VarcharStreamReader : VarcharTreeReader, SettableTreeReader
        {
            private bool _isFileCompressed;
            private bool _isDictionaryEncoding;
            private SettableUncompressedStream _presentStream;
            private SettableUncompressedStream _dataStream;
            private SettableUncompressedStream _lengthStream;
            private SettableUncompressedStream _dictionaryStream;

            private VarcharStreamReader(int columnId, int maxLength,
                SettableUncompressedStream present, SettableUncompressedStream data,
                SettableUncompressedStream length, SettableUncompressedStream dictionary,
                bool isFileCompressed, OrcProto.ColumnEncoding encoding)
                : base(columnId, maxLength, present, data, length,
                    dictionary, encoding)
            {
                this._isDictionaryEncoding = dictionary != null;
                this._isFileCompressed = isFileCompressed;
                this._presentStream = present;
                this._dataStream = data;
                this._lengthStream = length;
                this._dictionaryStream = dictionary;
            }

            public void seek(PositionProvider index)
            {
                if (present != null)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    reader.getPresent().seek(index);
                }

                if (_isDictionaryEncoding)
                {
                    // DICTIONARY encoding

                    // data stream could be empty stream or already reached end of stream before present stream.
                    // This can happen if all values in stream are nulls or last row group values are all null.
                    if (_dataStream.available() > 0)
                    {
                        if (_isFileCompressed)
                        {
                            index.getNext();
                        }
                        ((StringDictionaryTreeReader)reader).getReader().seek(index);
                    }
                }
                else
                {
                    // DIRECT encoding

                    // data stream could be empty stream or already reached end of stream before present stream.
                    // This can happen if all values in stream are nulls or last row group values are all null.
                    if (_dataStream.available() > 0)
                    {
                        if (_isFileCompressed)
                        {
                            index.getNext();
                        }
                        ((StringDirectTreeReader)reader).getStream().seek(index);
                    }

                    if (_lengthStream.available() > 0)
                    {
                        if (_isFileCompressed)
                        {
                            index.getNext();
                        }
                        ((StringDirectTreeReader)reader).getLengths().seek(index);
                    }
                }
            }

            public void setBuffers(ColumnStreamData[] streamsData, bool sameStripe)
            {
                if (_presentStream != null)
                {
                    _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.PRESENT_VALUE]));
                }
                if (_dataStream != null)
                {
                    _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DATA_VALUE]));
                }
                if (!_isDictionaryEncoding)
                {
                    if (_lengthStream != null)
                    {
                        _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.LENGTH_VALUE]));
                    }
                }

                // set these streams only if the stripe is different
                if (!sameStripe && _isDictionaryEncoding)
                {
                    if (_lengthStream != null)
                    {
                        _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.LENGTH_VALUE]));
                    }
                    if (_dictionaryStream != null)
                    {
                        _dictionaryStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DICTIONARY_DATA_VALUE]));
                    }
                }
            }

            public class StreamReaderBuilder
            {
                private long? fileId;
                private int columnIndex;
                private int maxLength;
                private ColumnStreamData presentStream;
                private ColumnStreamData dataStream;
                private ColumnStreamData dictionaryStream;
                private ColumnStreamData lengthStream;
                private CompressionCodec compressionCodec;
                private OrcProto.ColumnEncoding columnEncoding;

                public StreamReaderBuilder setFileId(long? fileId)
                {
                    this.fileId = fileId;
                    return this;
                }

                public StreamReaderBuilder setColumnIndex(int columnIndex)
                {
                    this.columnIndex = columnIndex;
                    return this;
                }

                public StreamReaderBuilder setMaxLength(int maxLength)
                {
                    this.maxLength = maxLength;
                    return this;
                }

                public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream)
                {
                    this.presentStream = presentStream;
                    return this;
                }

                public StreamReaderBuilder setDataStream(ColumnStreamData dataStream)
                {
                    this.dataStream = dataStream;
                    return this;
                }

                public StreamReaderBuilder setLengthStream(ColumnStreamData lengthStream)
                {
                    this.lengthStream = lengthStream;
                    return this;
                }

                public StreamReaderBuilder setDictionaryStream(ColumnStreamData dictStream)
                {
                    this.dictionaryStream = dictStream;
                    return this;
                }

                public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec)
                {
                    this.compressionCodec = compressionCodec;
                    return this;
                }

                public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding)
                {
                    this.columnEncoding = encoding;
                    return this;
                }

                public VarcharStreamReader build()
                {
                    SettableUncompressedStream present = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.PRESENT.ToString(),
                            fileId, presentStream);

                    SettableUncompressedStream data = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.DATA.ToString(), fileId,
                            dataStream);

                    SettableUncompressedStream length = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.LENGTH.ToString(), fileId,
                            lengthStream);

                    SettableUncompressedStream dictionary = StreamUtils.createSettableUncompressedStream(
                        OrcProto.Stream.Types.Kind.DICTIONARY_DATA.ToString(), fileId, dictionaryStream);

                    bool isFileCompressed = compressionCodec != null;
                    return new VarcharStreamReader(columnIndex, maxLength, present, data, length,
                        dictionary, isFileCompressed, columnEncoding);
                }
            }

            public static StreamReaderBuilder builder()
            {
                return new StreamReaderBuilder();
            }

        }

        protected class ByteStreamReader : ByteTreeReader, SettableTreeReader
        {
            private bool _isFileCompressed;
            private SettableUncompressedStream _presentStream;
            private SettableUncompressedStream _dataStream;

            private ByteStreamReader(int columnId, SettableUncompressedStream present,
                SettableUncompressedStream data, bool isFileCompressed)
                : base(columnId, present, data)
            {
                this._isFileCompressed = isFileCompressed;
                this._presentStream = present;
                this._dataStream = data;
            }

            public void seek(PositionProvider index)
            {
                if (present != null)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    present.seek(index);
                }

                // data stream could be empty stream or already reached end of stream before present stream.
                // This can happen if all values in stream are nulls or last row group values are all null.
                if (_dataStream.available() > 0)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    reader.seek(index);
                }
            }

            public void setBuffers(ColumnStreamData[] streamsData, bool sameStripe)
            {
                if (_presentStream != null)
                {
                    _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.PRESENT_VALUE]));
                }
                if (_dataStream != null)
                {
                    _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DATA_VALUE]));
                }
            }

            public class StreamReaderBuilder
            {
                private long? fileId;
                private int columnIndex;
                private ColumnStreamData presentStream;
                private ColumnStreamData dataStream;
                private CompressionCodec compressionCodec;

                public StreamReaderBuilder setFileId(long? fileId)
                {
                    this.fileId = fileId;
                    return this;
                }

                public StreamReaderBuilder setColumnIndex(int columnIndex)
                {
                    this.columnIndex = columnIndex;
                    return this;
                }

                public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream)
                {
                    this.presentStream = presentStream;
                    return this;
                }

                public StreamReaderBuilder setDataStream(ColumnStreamData dataStream)
                {
                    this.dataStream = dataStream;
                    return this;
                }

                public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec)
                {
                    this.compressionCodec = compressionCodec;
                    return this;
                }

                public ByteStreamReader build()
                {
                    SettableUncompressedStream present = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.PRESENT.ToString(),
                            fileId, presentStream);

                    SettableUncompressedStream data = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.DATA.ToString(), fileId,
                            dataStream);

                    bool isFileCompressed = compressionCodec != null;
                    return new ByteStreamReader(columnIndex, present, data, isFileCompressed);
                }
            }

            public static StreamReaderBuilder builder()
            {
                return new StreamReaderBuilder();
            }
        }

        protected class BinaryStreamReader : BinaryTreeReader, SettableTreeReader
        {
            private bool _isFileCompressed;
            private SettableUncompressedStream _presentStream;
            private SettableUncompressedStream _dataStream;
            private SettableUncompressedStream _lengthsStream;

            private BinaryStreamReader(int columnId, SettableUncompressedStream present,
                SettableUncompressedStream data, SettableUncompressedStream length,
                bool isFileCompressed,
                OrcProto.ColumnEncoding encoding)
                : base(columnId, present, data, length, encoding)
            {
                this._isFileCompressed = isFileCompressed;
                this._presentStream = present;
                this._dataStream = data;
                this._lengthsStream = length;
            }

            public void seek(PositionProvider index)
            {
                if (present != null)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    present.seek(index);
                }

                // data stream could be empty stream or already reached end of stream before present stream.
                // This can happen if all values in stream are nulls or last row group values are all null.
                if (_dataStream.available() > 0)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    stream.seek(index);
                }

                if (lengths != null && _lengthsStream.available() > 0)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    lengths.seek(index);
                }
            }

            public void setBuffers(ColumnStreamData[] streamsData, bool sameStripe)
            {
                if (_presentStream != null)
                {
                    _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.PRESENT_VALUE]));
                }
                if (_dataStream != null)
                {
                    _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DATA_VALUE]));
                }
                if (_lengthsStream != null)
                {
                    _lengthsStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.LENGTH_VALUE]));
                }
            }

            public class StreamReaderBuilder
            {
                private long? fileId;
                private int columnIndex;
                private ColumnStreamData presentStream;
                private ColumnStreamData dataStream;
                private ColumnStreamData lengthStream;
                private CompressionCodec compressionCodec;
                private OrcProto.ColumnEncoding columnEncoding;

                public StreamReaderBuilder setFileId(long? fileId)
                {
                    this.fileId = fileId;
                    return this;
                }

                public StreamReaderBuilder setColumnIndex(int columnIndex)
                {
                    this.columnIndex = columnIndex;
                    return this;
                }

                public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream)
                {
                    this.presentStream = presentStream;
                    return this;
                }

                public StreamReaderBuilder setDataStream(ColumnStreamData dataStream)
                {
                    this.dataStream = dataStream;
                    return this;
                }

                public StreamReaderBuilder setLengthStream(ColumnStreamData secondaryStream)
                {
                    this.lengthStream = secondaryStream;
                    return this;
                }

                public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec)
                {
                    this.compressionCodec = compressionCodec;
                    return this;
                }

                public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding)
                {
                    this.columnEncoding = encoding;
                    return this;
                }

                public BinaryStreamReader build()
                {
                    SettableUncompressedStream present = StreamUtils.createSettableUncompressedStream(
                        OrcProto.Stream.Types.Kind.PRESENT.ToString(), fileId, presentStream);

                    SettableUncompressedStream data = StreamUtils.createSettableUncompressedStream(
                        OrcProto.Stream.Types.Kind.DATA.ToString(), fileId, dataStream);

                    SettableUncompressedStream length = StreamUtils.createSettableUncompressedStream(
                        OrcProto.Stream.Types.Kind.LENGTH.ToString(), fileId, lengthStream);

                    bool isFileCompressed = compressionCodec != null;
                    return new BinaryStreamReader(columnIndex, present, data, length, isFileCompressed,
                        columnEncoding);
                }
            }

            public static StreamReaderBuilder builder()
            {
                return new StreamReaderBuilder();
            }
        }

        protected class BooleanStreamReader : BooleanTreeReader, SettableTreeReader
        {
            private bool _isFileCompressed;
            private SettableUncompressedStream _presentStream;
            private SettableUncompressedStream _dataStream;

            private BooleanStreamReader(int columnId, SettableUncompressedStream present,
                SettableUncompressedStream data, bool isFileCompressed)
                : base(columnId, present, data)
            {
                this._isFileCompressed = isFileCompressed;
                this._presentStream = present;
                this._dataStream = data;
            }

            public void seek(PositionProvider index)
            {
                if (present != null)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    present.seek(index);
                }

                // data stream could be empty stream or already reached end of stream before present stream.
                // This can happen if all values in stream are nulls or last row group values are all null.
                if (_dataStream.available() > 0)
                {
                    if (_isFileCompressed)
                    {
                        index.getNext();
                    }
                    reader.seek(index);
                }
            }

            public void setBuffers(ColumnStreamData[] streamsData, bool sameStripe)
            {
                if (_presentStream != null)
                {
                    _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.PRESENT_VALUE]));
                }
                if (_dataStream != null)
                {
                    _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[Kind.DATA_VALUE]));
                }
            }

            public class StreamReaderBuilder
            {
                private long? fileId;
                private int columnIndex;
                private ColumnStreamData presentStream;
                private ColumnStreamData dataStream;
                private CompressionCodec compressionCodec;

                public StreamReaderBuilder setFileId(long? fileId)
                {
                    this.fileId = fileId;
                    return this;
                }

                public StreamReaderBuilder setColumnIndex(int columnIndex)
                {
                    this.columnIndex = columnIndex;
                    return this;
                }

                public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream)
                {
                    this.presentStream = presentStream;
                    return this;
                }

                public StreamReaderBuilder setDataStream(ColumnStreamData dataStream)
                {
                    this.dataStream = dataStream;
                    return this;
                }

                public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec)
                {
                    this.compressionCodec = compressionCodec;
                    return this;
                }

                public BooleanStreamReader build()
                {
                    SettableUncompressedStream present = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.PRESENT.ToString(),
                            fileId, presentStream);

                    SettableUncompressedStream data = StreamUtils
                        .createSettableUncompressedStream(OrcProto.Stream.Types.Kind.DATA.ToString(), fileId,
                            dataStream);

                    bool isFileCompressed = compressionCodec != null;
                    return new BooleanStreamReader(columnIndex, present, data, isFileCompressed);
                }
            }

            public static StreamReaderBuilder builder()
            {
                return new StreamReaderBuilder();
            }
        }

        public static TreeReader[] createEncodedTreeReader(int numCols,
            List<OrcProto.Type> types,
            List<OrcProto.ColumnEncoding> encodings,
            EncodedColumnBatch<OrcBatchKey> batch,
            CompressionCodec codec, bool skipCorrupt)
        {
            long file = batch.getBatchKey().file;
            TreeReader[] treeReaders = new TreeReader[numCols];
            for (int i = 0; i < numCols; i++)
            {
                int columnIndex = batch.getColumnIxs()[i];
                ColumnStreamData[] streamBuffers = batch.getColumnData()[i];
                OrcProto.Type columnType = types[columnIndex];

                // EncodedColumnBatch is already decompressed, we don't really need to pass codec.
                // But we need to know if the original data is compressed or not. This is used to skip
                // positions in row index properly. If the file is originally compressed,
                // then 1st position (compressed offset) in row index should be skipped to get
                // uncompressed offset, else 1st position should not be skipped.
                // TODO: there should be a better way to do this, code just needs to be modified
                OrcProto.ColumnEncoding columnEncoding = encodings[columnIndex];

                // stream buffers are arranged in enum order of stream kind
                ColumnStreamData present = streamBuffers[Kind.PRESENT_VALUE],
                  data = streamBuffers[Kind.DATA_VALUE],
                  dictionary = streamBuffers[Kind.DICTIONARY_DATA_VALUE],
                  lengths = streamBuffers[Kind.LENGTH_VALUE],
                  secondary = streamBuffers[Kind.SECONDARY_VALUE];

                switch (columnType.Kind)
                {
                    case OrcProto.Type.Types.Kind.BINARY:
                        treeReaders[i] = BinaryStreamReader.builder()
                            .setFileId(file)
                            .setColumnIndex(columnIndex)
                            .setPresentStream(present)
                            .setDataStream(data)
                            .setLengthStream(lengths)
                            .setCompressionCodec(codec)
                            .setColumnEncoding(columnEncoding)
                            .build();
                        break;
                    case OrcProto.Type.Types.Kind.BOOLEAN:
                        treeReaders[i] = BooleanStreamReader.builder()
                            .setFileId(file)
                            .setColumnIndex(columnIndex)
                            .setPresentStream(present)
                            .setDataStream(data)
                            .setCompressionCodec(codec)
                            .build();
                        break;
                    case OrcProto.Type.Types.Kind.BYTE:
                        treeReaders[i] = ByteStreamReader.builder()
                            .setFileId(file)
                            .setColumnIndex(columnIndex)
                            .setPresentStream(present)
                            .setDataStream(data)
                            .setCompressionCodec(codec)
                            .build();
                        break;
                    case OrcProto.Type.Types.Kind.SHORT:
                        treeReaders[i] = ShortStreamReader.builder()
                            .setFileId(file)
                            .setColumnIndex(columnIndex)
                            .setPresentStream(present)
                            .setDataStream(data)
                            .setCompressionCodec(codec)
                            .setColumnEncoding(columnEncoding)
                            .build();
                        break;
                    case OrcProto.Type.Types.Kind.INT:
                        treeReaders[i] = IntStreamReader.builder()
                            .setFileId(file)
                            .setColumnIndex(columnIndex)
                            .setPresentStream(present)
                            .setDataStream(data)
                            .setCompressionCodec(codec)
                            .setColumnEncoding(columnEncoding)
                            .build();
                        break;
                    case OrcProto.Type.Types.Kind.LONG:
                        treeReaders[i] = LongStreamReader.builder()
                            .setFileId(file)
                            .setColumnIndex(columnIndex)
                            .setPresentStream(present)
                            .setDataStream(data)
                            .setCompressionCodec(codec)
                            .setColumnEncoding(columnEncoding)
                            .skipCorrupt(skipCorrupt)
                            .build();
                        break;
                    case OrcProto.Type.Types.Kind.FLOAT:
                        treeReaders[i] = FloatStreamReader.builder()
                            .setFileId(file)
                            .setColumnIndex(columnIndex)
                            .setPresentStream(present)
                            .setDataStream(data)
                            .setCompressionCodec(codec)
                            .build();
                        break;
                    case OrcProto.Type.Types.Kind.DOUBLE:
                        treeReaders[i] = DoubleStreamReader.builder()
                            .setFileId(file)
                            .setColumnIndex(columnIndex)
                            .setPresentStream(present)
                            .setDataStream(data)
                            .setCompressionCodec(codec)
                            .build();
                        break;
                    case OrcProto.Type.Types.Kind.CHAR:
                        treeReaders[i] = CharStreamReader.builder()
                            .setFileId(file)
                            .setColumnIndex(columnIndex)
                            .setMaxLength((int)columnType.MaximumLength)
                            .setPresentStream(present)
                            .setDataStream(data)
                            .setLengthStream(lengths)
                            .setDictionaryStream(dictionary)
                            .setCompressionCodec(codec)
                            .setColumnEncoding(columnEncoding)
                            .build();
                        break;
                    case OrcProto.Type.Types.Kind.VARCHAR:
                        treeReaders[i] = VarcharStreamReader.builder()
                            .setFileId(file)
                            .setColumnIndex(columnIndex)
                            .setMaxLength((int)columnType.MaximumLength)
                            .setPresentStream(present)
                            .setDataStream(data)
                            .setLengthStream(lengths)
                            .setDictionaryStream(dictionary)
                            .setCompressionCodec(codec)
                            .setColumnEncoding(columnEncoding)
                            .build();
                        break;
                    case OrcProto.Type.Types.Kind.STRING:
                        treeReaders[i] = StringStreamReader.builder()
                            .setFileId(file)
                            .setColumnIndex(columnIndex)
                            .setPresentStream(present)
                            .setDataStream(data)
                            .setLengthStream(lengths)
                            .setDictionaryStream(dictionary)
                            .setCompressionCodec(codec)
                            .setColumnEncoding(columnEncoding)
                            .build();
                        break;
                    case OrcProto.Type.Types.Kind.DECIMAL:
                        treeReaders[i] = DecimalStreamReader.builder()
                            .setFileId(file)
                            .setColumnIndex(columnIndex)
                            .setPrecision((int)columnType.Precision)
                            .setScale((int)columnType.Scale)
                            .setPresentStream(present)
                            .setValueStream(data)
                            .setScaleStream(secondary)
                            .setCompressionCodec(codec)
                            .setColumnEncoding(columnEncoding)
                            .build();
                        break;
                    case OrcProto.Type.Types.Kind.TIMESTAMP:
                        treeReaders[i] = TimestampStreamReader.builder()
                            .setFileId(file)
                            .setColumnIndex(columnIndex)
                            .setPresentStream(present)
                            .setSecondsStream(data)
                            .setNanosStream(secondary)
                            .setCompressionCodec(codec)
                            .setColumnEncoding(columnEncoding)
                            .skipCorrupt(skipCorrupt)
                            .build();
                        break;
                    case OrcProto.Type.Types.Kind.DATE:
                        treeReaders[i] = DateStreamReader.builder()
                            .setFileId(file)
                            .setColumnIndex(columnIndex)
                            .setPresentStream(present)
                            .setDataStream(data)
                            .setCompressionCodec(codec)
                            .setColumnEncoding(columnEncoding)
                            .build();
                        break;
                    default:
                        throw new NotSupportedException("Data type not supported yet! " + columnType);
                }
            }

            return treeReaders;
        }
    }
}
