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
    using org.apache.hadoop.hive.ql.io.orc.external;

    class ReaderImpl : org.apache.hadoop.hive.ql.io.orc.ReaderImpl, Reader
    {

        public ReaderImpl(Path path, OrcFile.ReaderOptions options)
            : base(path, options)
        {
        }

        public override EncodedReader encodedReader(
            long fileId, DataCache dataCache, DataReader dataReader, Reader.PoolFactory pf)
        {
            return new EncodedReaderImpl(fileId, types,
                codec, bufferSize, rowIndexStride, dataCache, dataReader, pf);
        }
    }
}
