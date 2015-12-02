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
    using OrcSharp.Types;

    /// <summary>
    /// Interface for reading integers.
    /// </summary>
    public interface IntegerReader
    {
        /// <summary>
        /// Seek to the position provided by index.
        /// </summary>
        /// <param name="index"></param>
        void seek(PositionProvider index);

        /// <summary>
        /// Skip number of specified rows.
        /// </summary>
        /// <param name="numValues"></param>
        void skip(long numValues);

        /// <summary>
        /// Check if there are any more values left.
        /// </summary>
        bool hasNext();

        /// <summary>
        /// Return the next available value.
        /// </summary>
        long next();

        /// <summary>
        /// Return the next available vector for values.
        /// </summary>
        /// <param name="previous"></param>
        /// <param name="previousLen"></param>
        void nextVector(LongColumnVector previous, long previousLen);

        void setInStream(InStream data);
    }
}
