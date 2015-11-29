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
    public class BitFieldWriter
    {
        private RunLengthByteWriter output;
        private int bitSize;
        private byte current = 0;
        private int bitsLeft = 8;

        public BitFieldWriter(PositionedOutputStream output, int bitSize)
        {
            this.output = new RunLengthByteWriter(output);
            this.bitSize = bitSize;
        }

        private void writeByte()
        {
            output.write(current);
            current = 0;
            bitsLeft = 8;
        }

        public void flush()
        {
            if (bitsLeft != 8)
            {
                writeByte();
            }
            output.flush();
        }

        public void write(int value)
        {
            int bitsToWrite = bitSize;
            while (bitsToWrite > bitsLeft)
            {
                // add the bits to the bottom of the current word
                current |= (byte)((uint)value >> (bitsToWrite - bitsLeft));
                // subtract out the bits we just added
                bitsToWrite -= bitsLeft;
                // zero out the bits above bitsToWrite
                value &= (1 << bitsToWrite) - 1;
                writeByte();
            }
            bitsLeft -= bitsToWrite;
            current |= (byte)(value << bitsLeft);
            if (bitsLeft == 0)
            {
                writeByte();
            }
        }

        public void getPosition(PositionRecorder recorder)
        {
            output.getPosition(recorder);
            recorder.addPosition(8 - bitsLeft);
        }
    }
}
