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
    using System.Collections.Generic;
    using org.apache.hadoop.hive.ql.io.orc.external;




    /**
     * OrcFileSplit. Holds file meta info
     *
     */
    public class OrcSplit : FileSplit, ColumnarSplit
    {
        private static Log LOG = LogFactory.getLog(typeof(OrcSplit));

        private FileMetaInfo fileMetaInfo;
        private bool _hasFooter;
        private bool _isOriginal;
        private bool _hasBase;
        private List<AcidInputFormat.DeltaMetaData> deltas = new List<AcidInputFormat.DeltaMetaData>();
        private OrcFile.WriterVersion writerVersion;
        private long projColsUncompressedSize;
        private volatile long? fileId;

        internal static int HAS_FILEID_FLAG = 8;
        internal static int BASE_FLAG = 4;
        internal static int ORIGINAL_FLAG = 2;
        internal static int FOOTER_FLAG = 1;

        protected OrcSplit()
            //The FileSplit() constructor in hadoop 0.20 and 1.x is package private so can't use it.
            //This constructor is used to create the object and then call readFields()
            // so just pass nulls to this super constructor.
            : base(null, 0, 0, (string[])null)
        {
        }

        public OrcSplit(Path path, long fileId, long offset, long length, string[] hosts,
            FileMetaInfo fileMetaInfo, bool isOriginal, bool hasBase,
            List<AcidInputFormat.DeltaMetaData> deltas, long projectedDataSize) :
            base(path, offset, length, hosts)
        {
            // We could avoid serializing file ID and just replace the path with inode-based path.
            // However, that breaks bunch of stuff because Hive later looks up things by split path.
            this.fileId = fileId;
            this.fileMetaInfo = fileMetaInfo;
            this._hasFooter = this.fileMetaInfo != null;
            this._isOriginal = isOriginal;
            this._hasBase = hasBase;
            this.deltas.AddRange(deltas);
            this.projColsUncompressedSize = projectedDataSize <= 0 ? length : projectedDataSize;
        }

        public void write(DataOutput @out)
        {
            //serialize path, offset, length using FileSplit
            base.write(@out);

            int flags = (_hasBase ? BASE_FLAG : 0) |
                (_isOriginal ? ORIGINAL_FLAG : 0) |
                (_hasFooter ? FOOTER_FLAG : 0) |
                (fileId != null ? HAS_FILEID_FLAG : 0);
            @out.writeByte(flags);
            @out.writeInt(deltas.Count);
            foreach (AcidInputFormat.DeltaMetaData delta in deltas)
            {
                delta.write(@out);
            }
            if (_hasFooter)
            {
                // serialize FileMetaInfo fields
                Text.writeString(@out, fileMetaInfo.compressionType);
                WritableUtils.writeVInt(@out, fileMetaInfo.bufferSize);
                WritableUtils.writeVInt(@out, fileMetaInfo.metadataSize);

                // serialize FileMetaInfo field footer
                ByteBuffer footerBuff = fileMetaInfo.footerBuffer;
                footerBuff.reset();

                // write length of buffer
                WritableUtils.writeVInt(@out, footerBuff.limit() - footerBuff.position());
                @out.write(footerBuff.array(), footerBuff.position(),
                    footerBuff.limit() - footerBuff.position());
                WritableUtils.writeVInt(@out, fileMetaInfo.writerVersion.getId());
            }
            if (fileId != null)
            {
                @out.writeLong(fileId.longValue());
            }
        }

        public void readFields(DataInput @in)
        {
            //deserialize path, offset, length using FileSplit
            base.readFields(@in);

            byte flags = @in.readByte();
            hasFooter = (FOOTER_FLAG & flags) != 0;
            isOriginal = (ORIGINAL_FLAG & flags) != 0;
            hasBase = (BASE_FLAG & flags) != 0;
            bool hasFileId = (HAS_FILEID_FLAG & flags) != 0;

            deltas.Clear();
            int numDeltas = @in.readInt();
            for (int i = 0; i < numDeltas; i++)
            {
                AcidInputFormat.DeltaMetaData dmd = new AcidInputFormat.DeltaMetaData();
                dmd.readFields(@in);
                deltas.Add(dmd);
            }
            if (hasFooter)
            {
                // deserialize FileMetaInfo fields
                string compressionType = Text.readString(@in);
                int bufferSize = WritableUtils.readVInt(@in);
                int metadataSize = WritableUtils.readVInt(@in);

                // deserialize FileMetaInfo field footer
                int footerBuffSize = WritableUtils.readVInt(@in);
                ByteBuffer footerBuff = ByteBuffer.allocate(footerBuffSize);
                @in.readFully(footerBuff.array(), 0, footerBuffSize);
                OrcFile.WriterVersion writerVersion =
                    ReaderImpl.getWriterVersion(WritableUtils.readVInt(@in));

                fileMetaInfo = new FileMetaInfo(compressionType, bufferSize,
                    metadataSize, footerBuff, writerVersion);
            }
            if (hasFileId)
            {
                fileId = @in.readLong();
            }
        }

        FileMetaInfo getFileMetaInfo()
        {
            return fileMetaInfo;
        }

        public bool hasFooter()
        {
            return _hasFooter;
        }

        public bool isOriginal()
        {
            return _isOriginal;
        }

        public bool hasBase()
        {
            return _hasBase;
        }

        public List<AcidInputFormat.DeltaMetaData> getDeltas()
        {
            return deltas;
        }

        public long getProjectedColumnsUncompressedSize()
        {
            return projColsUncompressedSize;
        }

        public long? getFileId()
        {
            return fileId;
        }

        public long getColumnarProjectionSize()
        {
            return projColsUncompressedSize;
        }

        internal object getLocations()
        {
            throw new System.NotImplementedException();
        }

        internal FileMetaInfo getFileMetaInfo()
        {
            throw new System.NotImplementedException();
        }
    }
}
