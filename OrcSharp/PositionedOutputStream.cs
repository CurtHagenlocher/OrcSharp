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
    using System.IO;
    using OrcSharp.External;

    abstract public class PositionedOutputStream : OutputStream
    {
        /// <summary>
        /// Record the current position to the recorder.
        /// </summary>
        /// <param name="recorder">the object that receives the position</param>
        abstract public void getPosition(PositionRecorder recorder);

        /// <summary>
        /// Gets the memory size currently allocated as buffer associated with this stream.
        /// Returns the number of bytes used by buffers.
        /// </summary>
        abstract public long bufferSize { get; }
    }
}
