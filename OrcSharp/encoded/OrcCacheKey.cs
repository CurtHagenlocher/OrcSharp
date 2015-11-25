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
    public class OrcCacheKey : OrcBatchKey {
        public int colIx;

        public OrcCacheKey(long file, int stripeIx, int rgIx, int colIx) : base(file, stripeIx, rgIx)
        {
            this.colIx = colIx;
        }

        public OrcCacheKey(OrcBatchKey batchKey, int colIx) : base(batchKey.file, batchKey.stripeIx, batchKey.rgIx)
        {
            this.colIx = colIx;
        }

        public OrcBatchKey copyToPureBatchKey() {
            return new OrcBatchKey(file, stripeIx, rgIx);
        }

        public override string ToString() {
            return "[" + file + ", stripe " + stripeIx + ", rgIx " + rgIx + ", rgIx " + colIx + "]";
        }

        public override int GetHashCode() {
            int prime = 31;
            return base.GetHashCode() * prime + colIx;
        }

        public override bool Equals(object obj) {
            OrcCacheKey other = obj as OrcCacheKey;
            if (other == null)
            {
                return false;
            }
            return stripeIx == other.stripeIx && rgIx == other.rgIx
                && file == other.file && other.colIx == colIx;
        }
    }
}
