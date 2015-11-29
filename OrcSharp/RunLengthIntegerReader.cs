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
    using System;
    using System.IO;
    using OrcSharp.External;

    /// <summary>
    /// A reader that reads a sequence of integers.
    /// </summary>
    public class RunLengthIntegerReader : IntegerReader
    {
        private InStream input;
        private bool signed;
        private long[] literals = new long[RunLengthIntegerWriter.MAX_LITERAL_SIZE];
        private int numLiterals = 0;
        private int delta = 0;
        private int used = 0;
        private bool repeat = false;
        private SerializationUtils utils;

        public RunLengthIntegerReader(InStream input, bool signed)
        {
            this.input = input;
            this.signed = signed;
            this.utils = new SerializationUtils();
        }

        private void readValues(bool ignoreEof)
        {
            int control = input.ReadByte();
            if (control == -1)
            {
                if (!ignoreEof)
                {
                    throw new EndOfStreamException("Read past end of RLE integer from " + input);
                }
                used = numLiterals = 0;
                return;
            }
            else if (control < 0x80)
            {
                numLiterals = control + RunLengthIntegerWriter.MIN_REPEAT_SIZE;
                used = 0;
                repeat = true;
                delta = input.ReadByte();
                if (delta == -1)
                {
                    throw new EndOfStreamException("End of stream in RLE Integer from " + input);
                }
                // convert from 0 to 255 to -128 to 127 by converting to a signed byte
                delta = unchecked((sbyte)(byte)delta);
                if (signed)
                {
                    literals[0] = utils.readVslong(input);
                }
                else
                {
                    literals[0] = (long)utils.readVulong(input);
                }
            }
            else
            {
                repeat = false;
                numLiterals = 0x100 - control;
                used = 0;
                for (int i = 0; i < numLiterals; ++i)
                {
                    if (signed)
                    {
                        literals[i] = utils.readVslong(input);
                    }
                    else
                    {
                        literals[i] = (long)utils.readVulong(input);
                    }
                }
            }
        }

        public bool hasNext()
        {
            return used != numLiterals || input.available() > 0;
        }

        public long next()
        {
            long result;
            if (used == numLiterals)
            {
                readValues(false);
            }
            if (repeat)
            {
                result = literals[0] + (used++) * delta;
            }
            else
            {
                result = literals[used++];
            }
            return result;
        }

        public void nextVector(LongColumnVector previous, long previousLen)
        {
            previous.isRepeating = true;
            for (int i = 0; i < previousLen; i++)
            {
                if (!previous.isNull[i])
                {
                    previous.vector[i] = next();
                }
                else
                {
                    // The default value of null for int type in vectorized
                    // processing is 1, so set that if the value is null
                    previous.vector[i] = 1;
                }

                // The default value for nulls in Vectorization for int types is 1
                // and given that non null value can also be 1, we need to check for isNull also
                // when determining the isRepeating flag.
                if (previous.isRepeating
                    && i > 0
                    && (previous.vector[i - 1] != previous.vector[i] || previous.isNull[i - 1] != previous.isNull[i]))
                {
                    previous.isRepeating = false;
                }
            }
        }

        public void setInStream(InStream data)
        {
            input = data;
        }

        public void seek(PositionProvider index)
        {
            input.seek(index);
            int consumed = (int)index.getNext();
            if (consumed != 0)
            {
                // a loop is required for cases where we break the run into two parts
                while (consumed > 0)
                {
                    readValues(false);
                    used = consumed;
                    consumed -= numLiterals;
                }
            }
            else
            {
                used = 0;
                numLiterals = 0;
            }
        }

        public void skip(long numValues)
        {
            while (numValues > 0)
            {
                if (used == numLiterals)
                {
                    readValues(false);
                }
                long consume = Math.Min(numValues, numLiterals - used);
                used += (int)consume;
                numValues -= consume;
            }
        }
    }
}
