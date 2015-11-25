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
    public class OrcBatchKey {
        public long file;
        public int stripeIx, rgIx;

        public OrcBatchKey(long file, int stripeIx, int rgIx) {
            set(file, stripeIx, rgIx);
        }

        public void set(long file, int stripeIx, int rgIx) {
            this.file = file;
            this.stripeIx = stripeIx;
            this.rgIx = rgIx;
        }

        public override string ToString() {
            return "[" + file + ", stripe " + stripeIx + ", rgIx " + rgIx + "]";
        }

        public override int GetHashCode() {
            int prime = 31;
            int result = prime + (int)(file ^ (file >>> 32));
            return (prime * result + rgIx) * prime + stripeIx;
        }

        public override bool Equals(object obj) {
            OrcBatchKey other = obj as OrcBatchKey;
            if (other == null || other.GetType() != typeof(OrcBatchKey)) return false;
            return stripeIx == other.stripeIx && rgIx == other.rgIx && file == other.file;
        }

        public OrcBatchKey clone() {
            return new OrcBatchKey(file, stripeIx, rgIx);
        }
    }
}
