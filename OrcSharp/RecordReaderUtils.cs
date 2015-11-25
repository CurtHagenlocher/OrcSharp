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
    using System.Text;
    using org.apache.hadoop.hive.ql.io.orc.external;
    using OrcProto = global::orc.proto;

    /**
     * Stateless methods shared between RecordReaderImpl and EncodedReaderImpl.
     */
    public class RecordReaderUtils
    {
        private class DefaultDataReader : DataReader
        {
            private Stream file;
            private string path;
            private bool useZeroCopy;
            private CompressionCodec codec;

            public DefaultDataReader(
                Stream file, string path, bool useZeroCopy, CompressionCodec codec)
            {
                this.file = file;
                this.path = path;
                this.useZeroCopy = useZeroCopy;
                this.codec = codec;
            }

            public void open()
            {
                // this.file = fs.open(path);
            }

            public DiskRangeList readFileData(
                DiskRangeList range, long baseOffset, bool doForceDirect)
            {
                return RecordReaderUtils.readDiskRanges(file, baseOffset, range, doForceDirect);
            }

            public void close()
            {
                if (file != null)
                {
                    file.Close();
                }
            }

            public bool isTrackingDiskRanges()
            {
                return false;
            }

            public void releaseBuffer(ByteBuffer toRelease)
            {
            }
        }

        public static DataReader createDefaultDataReader(
            Stream file, string path, bool useZeroCopy, CompressionCodec codec)
        {
            return new DefaultDataReader(file, path, useZeroCopy, codec);
        }

        public static bool[] findPresentStreamsByColumn(
            IList<OrcProto.Stream> streamList, IList<OrcProto.Type> types)
        {
            bool[] hasNull = new bool[types.Count];
            foreach (OrcProto.Stream stream in streamList)
            {
                if (stream.HasKind && (stream.Kind == OrcProto.Stream.Types.Kind.PRESENT))
                {
                    hasNull[stream.Column] = true;
                }
            }
            return hasNull;
        }

        /**
         * Does region A overlap region B? The end points are inclusive on both sides.
         * @param leftA A's left point
         * @param rightA A's right point
         * @param leftB B's left point
         * @param rightB B's right point
         * @return Does region A overlap region B?
         */
        static bool overlap(long leftA, long rightA, long leftB, long rightB)
        {
            if (leftA <= leftB)
            {
                return rightA >= leftB;
            }
            return rightB >= leftA;
        }

        public static void addEntireStreamToRanges(
            long offset, long length, DiskRangeList.CreateHelper list, bool doMergeBuffers)
        {
            list.addOrMerge(offset, offset + length, doMergeBuffers, false);
        }

        public static void addRgFilteredStreamToRanges(OrcProto.Stream stream,
            bool[] includedRowGroups, bool isCompressed, OrcProto.RowIndex index,
            OrcProto.ColumnEncoding encoding, OrcProto.Type type, int compressionSize, bool hasNull,
            long offset, long length, DiskRangeList.CreateHelper list, bool doMergeBuffers)
        {
            for (int group = 0; group < includedRowGroups.Length; ++group)
            {
                if (!includedRowGroups[group]) continue;
                int posn = getIndexPosition(
                    encoding.Kind, type.Kind, stream.Kind, isCompressed, hasNull);
                long start = (long)index.EntryList[group].PositionsList[posn];
                long nextGroupOffset;
                bool isLast = group == (includedRowGroups.Length - 1);
                nextGroupOffset = isLast ? length : (int)index.EntryList[group + 1].PositionsList[posn];

                start += offset;
                long end = offset + estimateRgEndOffset(
                    isCompressed, isLast, nextGroupOffset, length, compressionSize);
                list.addOrMerge(start, end, doMergeBuffers, true);
            }
        }

        public static long estimateRgEndOffset(bool isCompressed, bool isLast,
            long nextGroupOffset, long streamLength, int bufferSize)
        {
            // figure out the worst case last location
            // if adjacent groups have the same compressed block offset then stretch the slop
            // by factor of 2 to safely accommodate the next compression block.
            // One for the current compression block and another for the next compression block.
            long slop = isCompressed ? 2 * (OutStream.HEADER_SIZE + bufferSize) : WORST_UNCOMPRESSED_SLOP;
            return isLast ? streamLength : Math.Min(streamLength, nextGroupOffset + slop);
        }

        private static int BYTE_STREAM_POSITIONS = 1;
        private static int RUN_LENGTH_BYTE_POSITIONS = BYTE_STREAM_POSITIONS + 1;
        private static int BITFIELD_POSITIONS = RUN_LENGTH_BYTE_POSITIONS + 1;
        private static int RUN_LENGTH_INT_POSITIONS = BYTE_STREAM_POSITIONS + 1;

        /**
         * Get the offset in the index positions for the column that the given
         * stream starts.
         * @param columnEncoding the encoding of the column
         * @param columnType the type of the column
         * @param streamType the kind of the stream
         * @param isCompressed is the file compressed
         * @param hasNulls does the column have a PRESENT stream?
         * @return the number of positions that will be used for that stream
         */
        public static int getIndexPosition(OrcProto.ColumnEncoding.Types.Kind columnEncoding,
                                    OrcProto.Type.Types.Kind columnType,
                                    OrcProto.Stream.Types.Kind streamType,
                                    bool isCompressed,
                                    bool hasNulls)
        {
            if (streamType == OrcProto.Stream.Types.Kind.PRESENT)
            {
                return 0;
            }
            int compressionValue = isCompressed ? 1 : 0;
            int @base = hasNulls ? (BITFIELD_POSITIONS + compressionValue) : 0;
            switch (columnType)
            {
                case OrcProto.Type.Types.Kind.BOOLEAN:
                case OrcProto.Type.Types.Kind.BYTE:
                case OrcProto.Type.Types.Kind.SHORT:
                case OrcProto.Type.Types.Kind.INT:
                case OrcProto.Type.Types.Kind.LONG:
                case OrcProto.Type.Types.Kind.FLOAT:
                case OrcProto.Type.Types.Kind.DOUBLE:
                case OrcProto.Type.Types.Kind.DATE:
                case OrcProto.Type.Types.Kind.STRUCT:
                case OrcProto.Type.Types.Kind.MAP:
                case OrcProto.Type.Types.Kind.LIST:
                case OrcProto.Type.Types.Kind.UNION:
                    return @base;
                case OrcProto.Type.Types.Kind.CHAR:
                case OrcProto.Type.Types.Kind.VARCHAR:
                case OrcProto.Type.Types.Kind.STRING:
                    if (columnEncoding == OrcProto.ColumnEncoding.Types.Kind.DICTIONARY ||
                        columnEncoding == OrcProto.ColumnEncoding.Types.Kind.DICTIONARY_V2)
                    {
                        return @base;
                    }
                    else
                    {
                        if (streamType == OrcProto.Stream.Types.Kind.DATA)
                        {
                            return @base;
                        }
                        else
                        {
                            return @base + BYTE_STREAM_POSITIONS + compressionValue;
                        }
                    }
                case OrcProto.Type.Types.Kind.BINARY:
                    if (streamType == OrcProto.Stream.Types.Kind.DATA)
                    {
                        return @base;
                    }
                    return @base + BYTE_STREAM_POSITIONS + compressionValue;
                case OrcProto.Type.Types.Kind.DECIMAL:
                    if (streamType == OrcProto.Stream.Types.Kind.DATA)
                    {
                        return @base;
                    }
                    return @base + BYTE_STREAM_POSITIONS + compressionValue;
                case OrcProto.Type.Types.Kind.TIMESTAMP:
                    if (streamType == OrcProto.Stream.Types.Kind.DATA)
                    {
                        return @base;
                    }
                    return @base + RUN_LENGTH_INT_POSITIONS + compressionValue;
                default:
                    throw new ArgumentException("Unknown type " + columnType);
            }
        }

        // for uncompressed streams, what is the most overlap with the following set
        // of rows (long vint literal group).
        static int WORST_UNCOMPRESSED_SLOP = 2 + 8 * 512;

        /**
         * Is this stream part of a dictionary?
         * @return is this part of a dictionary?
         */
        public static bool isDictionary(OrcProto.Stream.Types.Kind kind,
                                    OrcProto.ColumnEncoding encoding)
        {
            Debug.Assert(kind != OrcProto.Stream.Types.Kind.DICTIONARY_COUNT);
            OrcProto.ColumnEncoding.Types.Kind encodingKind = encoding.Kind;
            return kind == OrcProto.Stream.Types.Kind.DICTIONARY_DATA ||
              (kind == OrcProto.Stream.Types.Kind.LENGTH &&
               (encodingKind == OrcProto.ColumnEncoding.Types.Kind.DICTIONARY ||
                encodingKind == OrcProto.ColumnEncoding.Types.Kind.DICTIONARY_V2));
        }

        /**
         * Build a string representation of a list of disk ranges.
         * @param ranges ranges to stringify
         * @return the resulting string
         */
        public static string stringifyDiskRanges(DiskRangeList range)
        {
            StringBuilder buffer = new StringBuilder();
            buffer.Append("[");
            bool isFirst = true;
            while (range != null)
            {
                if (!isFirst)
                {
                    buffer.Append(", {");
                }
                else
                {
                    buffer.Append("{");
                }
                isFirst = false;
                buffer.Append(range.ToString());
                buffer.Append("}");
                range = range.next;
            }
            buffer.Append("]");
            return buffer.ToString();
        }

        /**
         * Read the list of ranges from the file.
         * @param file the file to read
         * @param base the base of the stripe
         * @param ranges the disk ranges within the stripe to read
         * @return the bytes read for each disk range, which is the same length as
         *    ranges
         * @
         */
        static DiskRangeList readDiskRanges(Stream file,
                                       long @base,
                                       DiskRangeList range,
                                       bool doForceDirect)
        {
            if (range == null) return null;
            DiskRangeList prev = range.prev;
            if (prev == null)
            {
                prev = new DiskRangeList.MutateHelper(range);
            }
            while (range != null)
            {
                if (range.hasData())
                {
                    range = range.next;
                    continue;
                }
                int len = (int)(range.getEnd() - range.getOffset());
                long off = range.getOffset();
                if (doForceDirect)
                {
                    file.Seek(@base + off, SeekOrigin.Current);
                    ByteBuffer directBuf = ByteBuffer.allocateDirect(len);
                    readDirect(file, len, directBuf);
                    range = range.replaceSelfWith(new RecordReaderImpl.BufferChunk(directBuf, range.getOffset()));
                }
                else
                {
                    byte[] buffer = new byte[len];
                    file.readFully((@base + off), buffer, 0, buffer.Length);
                    range = range.replaceSelfWith(new RecordReaderImpl.BufferChunk(ByteBuffer.wrap(buffer), range.getOffset()));
                }
                range = range.next;
            }
            return prev.next;
        }

        private static int readByteBuffer(Stream file, ByteBuffer dest)
        {
            int pos = dest.position();
            int result = dest.readRemaining(file);
            if (result > 0)
            {
                // Ensure this explicitly since versions before 2.7 read doesn't do it.
                dest.position(pos + result);
            }
            return result;
        }

        public static void readDirect(Stream file, int len, ByteBuffer directBuf)
        {
            // TODO: HDFS API is a mess, so handle all kinds of cases.
            // Before 2.7, read() also doesn't adjust position correctly, so track it separately.
            int pos = directBuf.position(), startPos = pos, endPos = pos + len;
            try
            {
                while (pos < endPos)
                {
                    int count = readByteBuffer(file, directBuf);
                    if (count < 0) throw new EndOfStreamException();
                    Debug.Assert(count != 0, "0-length read: " + (endPos - pos) + "@" + (pos - startPos));
                    pos += count;
                    Debug.Assert(pos <= endPos, "Position " + pos + " > " + endPos + " after reading " + count);
                    directBuf.position(pos);
                }
            }
            catch (NotSupportedException)
            {
                Debug.Assert(pos == startPos);
                // Happens in q files and such.
                RecordReaderImpl.LOG.error("Stream does not support direct read; we will copy.");
                byte[] buffer = new byte[len];
                file.readFully(buffer, 0, buffer.Length);
                directBuf.put(buffer);
            }
            directBuf.position(startPos);
            directBuf.limit(startPos + len);
        }


        internal static List<DiskRange> getStreamBuffers(DiskRangeList range, long offset, long length)
        {
            // This assumes sorted ranges (as do many other parts of ORC code.
            List<DiskRange> buffers = new List<DiskRange>();
            if (length == 0) return buffers;
            long streamEnd = offset + length;
            bool inRange = false;
            while (range != null)
            {
                if (!inRange)
                {
                    if (range.getEnd() <= offset)
                    {
                        range = range.next;
                        continue; // Skip until we are in range.
                    }
                    inRange = true;
                    if (range.getOffset() < offset)
                    {
                        // Partial first buffer, add a slice of it.
                        buffers.Add(range.sliceAndShift(offset, Math.Min(streamEnd, range.getEnd()), -offset));
                        if (range.getEnd() >= streamEnd) break; // Partial first buffer is also partial last buffer.
                        range = range.next;
                        continue;
                    }
                }
                else if (range.getOffset() >= streamEnd)
                {
                    break;
                }
                if (range.getEnd() > streamEnd)
                {
                    // Partial last buffer (may also be the first buffer), add a slice of it.
                    buffers.Add(range.sliceAndShift(range.getOffset(), streamEnd, -offset));
                    break;
                }
                // Buffer that belongs entirely to one stream.
                // TODO: ideally we would want to reuse the object and remove it from the list, but we cannot
                //       because bufferChunks is also used by clearStreams for zcr. Create a useless dup.
                buffers.Add(range.sliceAndShift(range.getOffset(), range.getEnd(), -offset));
                if (range.getEnd() == streamEnd) break;
                range = range.next;
            }
            return buffers;
        }
    }
}
