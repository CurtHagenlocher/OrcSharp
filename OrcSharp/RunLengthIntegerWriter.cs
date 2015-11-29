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
    /**
     * A streamFactory that writes a sequence of integers. A control byte is written before
     * each run with positive values 0 to 127 meaning 3 to 130 repetitions, each
     * repetition is offset by a delta. If the control byte is -1 to -128, 1 to 128
     * literal vint values follow.
     */
    class RunLengthIntegerWriter : IntegerWriter
    {
        internal const int MIN_REPEAT_SIZE = 3;
        internal const int MAX_DELTA = 127;
        internal const int MIN_DELTA = -128;
        public const int MAX_LITERAL_SIZE = 128;
        private const int MAX_REPEAT_SIZE = 127 + MIN_REPEAT_SIZE;
        private PositionedOutputStream output;
        private bool signed;
        private long[] literals = new long[MAX_LITERAL_SIZE];
        private int numLiterals = 0;
        private long delta = 0;
        private bool repeat = false;
        private int tailRunLength = 0;
        private SerializationUtils utils;

        public RunLengthIntegerWriter(PositionedOutputStream output, bool signed)
        {
            this.output = output;
            this.signed = signed;
            this.utils = new SerializationUtils();
        }

        private void writeValues()
        {
            if (numLiterals != 0)
            {
                if (repeat)
                {
                    output.WriteByte((byte)(numLiterals - MIN_REPEAT_SIZE));
                    output.WriteByte((byte)delta);
                    if (signed)
                    {
                        utils.writeVslong(output, literals[0]);
                    }
                    else
                    {
                        utils.writeVulong(output, (ulong)literals[0]);
                    }
                }
                else
                {
                    output.WriteByte((byte)-numLiterals);
                    for (int i = 0; i < numLiterals; ++i)
                    {
                        if (signed)
                        {
                            utils.writeVslong(output, literals[i]);
                        }
                        else
                        {
                            utils.writeVulong(output, (ulong)literals[i]);
                        }
                    }
                }
                repeat = false;
                numLiterals = 0;
                tailRunLength = 0;
            }
        }

        public void flush()
        {
            writeValues();
            output.Flush();
        }

        public void write(long value)
        {
            if (numLiterals == 0)
            {
                literals[numLiterals++] = value;
                tailRunLength = 1;
            }
            else if (repeat)
            {
                if (value == literals[0] + delta * numLiterals)
                {
                    numLiterals += 1;
                    if (numLiterals == MAX_REPEAT_SIZE)
                    {
                        writeValues();
                    }
                }
                else
                {
                    writeValues();
                    literals[numLiterals++] = value;
                    tailRunLength = 1;
                }
            }
            else
            {
                if (tailRunLength == 1)
                {
                    delta = value - literals[numLiterals - 1];
                    if (delta < MIN_DELTA || delta > MAX_DELTA)
                    {
                        tailRunLength = 1;
                    }
                    else
                    {
                        tailRunLength = 2;
                    }
                }
                else if (value == literals[numLiterals - 1] + delta)
                {
                    tailRunLength += 1;
                }
                else
                {
                    delta = value - literals[numLiterals - 1];
                    if (delta < MIN_DELTA || delta > MAX_DELTA)
                    {
                        tailRunLength = 1;
                    }
                    else
                    {
                        tailRunLength = 2;
                    }
                }
                if (tailRunLength == MIN_REPEAT_SIZE)
                {
                    if (numLiterals + 1 == MIN_REPEAT_SIZE)
                    {
                        repeat = true;
                        numLiterals += 1;
                    }
                    else
                    {
                        numLiterals -= MIN_REPEAT_SIZE - 1;
                        long @base = literals[numLiterals];
                        writeValues();
                        literals[0] = @base;
                        repeat = true;
                        numLiterals = MIN_REPEAT_SIZE;
                    }
                }
                else
                {
                    literals[numLiterals++] = value;
                    if (numLiterals == MAX_LITERAL_SIZE)
                    {
                        writeValues();
                    }
                }
            }
        }

        public void getPosition(PositionRecorder recorder)
        {
            output.getPosition(recorder);
            recorder.addPosition(numLiterals);
        }
    }
}
