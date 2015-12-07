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
    using org.apache.hadoop.hive.ql.io.orc.external;
    using OrcProto = global::orc.proto;

    /**
     * Key for OrcFileMergeMapper task. Contains orc file related information that
     * should match before merging two orc files.
     */
    public class OrcFileKeyWrapper : WritableComparable<OrcFileKeyWrapper>
    {
        private Path inputPath;
        private CompressionKind compression;
        private int compressBufferSize;
        private List<OrcProto.Type> types;
        private int rowIndexStride;
        private OrcFile.Version version;
        private bool _isIncompatFile;

        public bool isIncompatFile()
        {
            return _isIncompatFile;
        }

        public void setIsIncompatFile(bool isIncompatFile)
        {
            this._isIncompatFile = isIncompatFile;
        }

        public OrcFile.Version getVersion()
        {
            return version;
        }

        public void setVersion(OrcFile.Version version)
        {
            this.version = version;
        }

        public int getRowIndexStride()
        {
            return rowIndexStride;
        }

        public void setRowIndexStride(int rowIndexStride)
        {
            this.rowIndexStride = rowIndexStride;
        }

        public int getCompressBufferSize()
        {
            return compressBufferSize;
        }

        public void setCompressBufferSize(int compressBufferSize)
        {
            this.compressBufferSize = compressBufferSize;
        }

        public CompressionKind getCompression()
        {
            return compression;
        }

        public void setCompression(CompressionKind compression)
        {
            this.compression = compression;
        }

        public List<OrcProto.Type> getTypes()
        {
            return types;
        }

        public void setTypes(List<OrcProto.Type> types)
        {
            this.types = types;
        }

        public Path getInputPath()
        {
            return inputPath;
        }

        public void setInputPath(Path inputPath)
        {
            this.inputPath = inputPath;
        }

        public void write(DataOutput @out)
        {
            throw new NotImplementedException("Not supported.");
        }

        public void readFields(DataInput @in)
        {
            throw new NotImplementedException("Not supported.");
        }

        public int compareTo(OrcFileKeyWrapper o)
        {
            return inputPath.compareTo(o.inputPath);
        }
    }
}
