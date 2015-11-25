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
    public class OrcNewSplit : FileSplit
    {
        private FileMetaInfo fileMetaInfo;
        private bool _hasFooter;
        private bool _isOriginal;
        private bool _hasBase;
        private List<AcidInputFormat.DeltaMetaData> deltas = new List<AcidInputFormat.DeltaMetaData>();
        private OrcFile.WriterVersion writerVersion;

        public OrcNewSplit(OrcSplit inner)
            : base(inner.getPath(), inner.getStart(), inner.getLength(),
                  inner.getLocations())
        {
            this.fileMetaInfo = inner.getFileMetaInfo();
            this._hasFooter = inner.hasFooter();
            this._isOriginal = inner.isOriginal();
            this._hasBase = inner.hasBase();
            this.deltas.AddRange(inner.getDeltas());
        }

        public void write(DataOutput @out)
        {
            //serialize path, offset, length using FileSplit
            base.write(@out);

            int flags = (_hasBase ? OrcSplit.BASE_FLAG : 0) |
                (_isOriginal ? OrcSplit.ORIGINAL_FLAG : 0) |
                (_hasFooter ? OrcSplit.FOOTER_FLAG : 0);
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
        }

        public void readFields(DataInput @in)
        {
            //deserialize path, offset, length using FileSplit
            base.readFields(@in);

            byte flags = @in.readByte();
            _hasFooter = (OrcSplit.FOOTER_FLAG & flags) != 0;
            _isOriginal = (OrcSplit.ORIGINAL_FLAG & flags) != 0;
            _hasBase = (OrcSplit.BASE_FLAG & flags) != 0;

            deltas.Clear();
            int numDeltas = @in.readInt();
            for (int i = 0; i < numDeltas; i++)
            {
                AcidInputFormat.DeltaMetaData dmd = new AcidInputFormat.DeltaMetaData();
                dmd.readFields(@in);
                deltas.Add(dmd);
            }
            if (_hasFooter)
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
    }
}
