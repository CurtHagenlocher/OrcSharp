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
    using System.Diagnostics;
    using System.IO;
    using OrcSharp.External;
    using OrcSharp.Types;

    /**
     * A reader that reads a sequence of light weight compressed integers. Refer
     * {@link RunLengthIntegerWriterV2} for description of various lightweight
     * compression techniques.
     */
    public class RunLengthIntegerReaderV2 : IntegerReader
    {
        private static readonly Logger LOG = LoggerFactory.getLog(typeof(RunLengthIntegerReaderV2));

        private InStream input;
        private bool signed;
        private long[] literals = new long[RunLengthIntegerWriterV2.MAX_SCOPE];
        private bool isRepeating = false;
        private int numLiterals = 0;
        private int used = 0;
        private bool skipCorrupt;
        private SerializationUtils utils;
        private RunLengthIntegerWriterV2.EncodingType currentEncoding;

        public RunLengthIntegerReaderV2(InStream input, bool signed,
            bool skipCorrupt)
        {
            this.input = input;
            this.signed = signed;
            this.skipCorrupt = skipCorrupt;
            this.utils = new SerializationUtils();
        }

        private static RunLengthIntegerWriterV2.EncodingType[] encodings =
            (RunLengthIntegerWriterV2.EncodingType[])Enum.GetValues(typeof(RunLengthIntegerWriterV2.EncodingType));

        private void readValues(bool ignoreEof)
        {
            // read the first 2 bits and determine the encoding type
            isRepeating = false;
            int firstByte = input.ReadByte();
            if (firstByte < 0)
            {
                if (!ignoreEof)
                {
                    throw new EndOfStreamException("Read past end of RLE integer from " + input);
                }
                used = numLiterals = 0;
                return;
            }
            currentEncoding = encodings[(firstByte >> 6) & 0x03];
            switch (currentEncoding)
            {
                case RunLengthIntegerWriterV2.EncodingType.SHORT_REPEAT:
                    readShortRepeatValues(firstByte);
                    break;
                case RunLengthIntegerWriterV2.EncodingType.DIRECT:
                    readDirectValues(firstByte);
                    break;
                case RunLengthIntegerWriterV2.EncodingType.PATCHED_BASE:
                    readPatchedBaseValues(firstByte);
                    break;
                case RunLengthIntegerWriterV2.EncodingType.DELTA:
                    readDeltaValues(firstByte);
                    break;
                default: throw new IOException("Unknown encoding " + currentEncoding);
            }
        }

        private void readDeltaValues(int firstByte)
        {
            // extract the number of fixed bits
            int fb = (firstByte >> 1) & 0x1f;
            if (fb != 0)
            {
                fb = utils.decodeBitWidth(fb);
            }

            // extract the blob run length
            int len = (firstByte & 0x01) << 8;
            len |= input.ReadByte();

            // read the first value stored as vint
            long firstVal = 0;
            if (signed)
            {
                firstVal = utils.readVslong(input);
            }
            else
            {
                firstVal = (long)utils.readVulong(input);
            }

            // store first value to result buffer
            long prevVal = firstVal;
            literals[numLiterals++] = firstVal;

            // if fixed bits is 0 then all values have fixed delta
            if (fb == 0)
            {
                // read the fixed delta value stored as vint (deltas can be negative even
                // if all number are positive)
                long fd = utils.readVslong(input);
                if (fd == 0)
                {
                    isRepeating = true;
                    Debug.Assert(numLiterals == 1);
                    Arrays.fill(literals, numLiterals, numLiterals + len, literals[0]);
                    numLiterals += len;
                }
                else
                {
                    // add fixed deltas to adjacent values
                    for (int i = 0; i < len; i++)
                    {
                        literals[numLiterals++] = literals[numLiterals - 2] + fd;
                    }
                }
            }
            else
            {
                long deltaBase = utils.readVslong(input);
                // add delta base and first value
                literals[numLiterals++] = firstVal + deltaBase;
                prevVal = literals[numLiterals - 1];
                len -= 1;

                // write the unpacked values, add it to previous value and store final
                // value to result buffer. if the delta base value is negative then it
                // is a decreasing sequence else an increasing sequence
                utils.readInts(literals, numLiterals, len, fb, input);
                while (len > 0)
                {
                    if (deltaBase < 0)
                    {
                        literals[numLiterals] = prevVal - literals[numLiterals];
                    }
                    else
                    {
                        literals[numLiterals] = prevVal + literals[numLiterals];
                    }
                    prevVal = literals[numLiterals];
                    len--;
                    numLiterals++;
                }
            }
        }

        private void readPatchedBaseValues(int firstByte)
        {
            // extract the number of fixed bits
            int fbo = (firstByte >> 1) & 0x1f;
            int fb = utils.decodeBitWidth(fbo);

            // extract the run length of data blob
            int len = (firstByte & 0x01) << 8;
            len |= input.ReadByte();
            // runs are always one off
            len += 1;

            // extract the number of bytes occupied by base
            int thirdByte = input.ReadByte();
            int bw = (thirdByte >> 5) & 0x07;
            // base width is one off
            bw += 1;

            // extract patch width
            int pwo = thirdByte & 0x1f;
            int pw = utils.decodeBitWidth(pwo);

            // read fourth byte and extract patch gap width
            int fourthByte = input.ReadByte();
            int pgw = (fourthByte >> 5) & 0x07;
            // patch gap width is one off
            pgw += 1;

            // extract the length of the patch list
            int pl = fourthByte & 0x1f;

            // read the next base width number of bytes to extract base value
            long @base = utils.bytesToLongBE(input, bw);
            long mask = (1L << ((bw * 8) - 1));
            // if MSB of base value is 1 then base is negative value else positive
            if ((@base & mask) != 0)
            {
                @base = @base & ~mask;
                @base = -@base;
            }

            // unpack the data blob
            long[] unpacked = new long[len];
            utils.readInts(unpacked, 0, len, fb, input);

            // unpack the patch blob
            long[] unpackedPatch = new long[pl];

            if ((pw + pgw) > 64 && !skipCorrupt)
            {
                throw new IOException("Corruption in ORC data encountered. To skip" +
                    " reading corrupted data, set hive.exec.orc.skip.corrupt.data to" +
                    " true");
            }
            int bitSize = utils.getClosestFixedBits(pw + pgw);
            utils.readInts(unpackedPatch, 0, pl, bitSize, input);

            // apply the patch directly when decoding the packed data
            int patchIdx = 0;
            long currGap = 0;
            long currPatch = 0;
            long patchMask = ((1L << pw) - 1);
            currGap = (long)((ulong)unpackedPatch[patchIdx] >> pw);
            currPatch = unpackedPatch[patchIdx] & patchMask;
            long actualGap = 0;

            // special case: gap is >255 then patch value will be 0.
            // if gap is <=255 then patch value cannot be 0
            while (currGap == 255 && currPatch == 0)
            {
                actualGap += 255;
                patchIdx++;
                currGap = (long)((ulong)unpackedPatch[patchIdx] >> pw);
                currPatch = unpackedPatch[patchIdx] & patchMask;
            }
            // add the left over gap
            actualGap += currGap;

            // unpack data blob, patch it (if required), add base to get result
            for (int i = 0; i < unpacked.Length; i++)
            {
                if (i == actualGap)
                {
                    // extract the patch value
                    long patchedVal = unpacked[i] | (currPatch << fb);

                    // add base to patched value
                    literals[numLiterals++] = @base + patchedVal;

                    // increment the patch to point to next entry in patch list
                    patchIdx++;

                    if (patchIdx < pl)
                    {
                        // read the next gap and patch
                        currGap = (long)((ulong)unpackedPatch[patchIdx] >> pw);
                        currPatch = unpackedPatch[patchIdx] & patchMask;
                        actualGap = 0;

                        // special case: gap is >255 then patch will be 0. if gap is
                        // <=255 then patch cannot be 0
                        while (currGap == 255 && currPatch == 0)
                        {
                            actualGap += 255;
                            patchIdx++;
                            currGap = (long)((ulong)unpackedPatch[patchIdx] >> pw);
                            currPatch = unpackedPatch[patchIdx] & patchMask;
                        }
                        // add the left over gap
                        actualGap += currGap;

                        // next gap is relative to the current gap
                        actualGap += i;
                    }
                }
                else
                {
                    // no patching required. add base to unpacked value to get value
                    literals[numLiterals++] = @base + unpacked[i];
                }
            }
        }

        private void readDirectValues(int firstByte)
        {
            // extract the number of fixed bits
            int fbo = (firstByte >> 1) & 0x1f;
            int fb = utils.decodeBitWidth(fbo);

            // extract the run length
            int len = (firstByte & 0x01) << 8;
            len |= input.ReadByte();
            // runs are one off
            len += 1;

            // write the unpacked values and zigzag decode to result buffer
            utils.readInts(literals, numLiterals, len, fb, input);
            if (signed)
            {
                for (int i = 0; i < len; i++)
                {
                    literals[numLiterals] = utils.zigzagDecode(literals[numLiterals]);
                    numLiterals++;
                }
            }
            else
            {
                numLiterals += len;
            }
        }

        private void readShortRepeatValues(int firstByte)
        {
            // read the number of bytes occupied by the value
            int size = (firstByte >> 3) & 0x07;
            // #bytes are one off
            size += 1;

            // read the run length
            int len = firstByte & 0x07;
            // run lengths values are stored only after MIN_REPEAT value is met
            len += RunLengthIntegerWriterV2.MIN_REPEAT;

            // read the repeated value which is store using fixed bytes
            long val = utils.bytesToLongBE(input, size);

            if (signed)
            {
                val = utils.zigzagDecode(val);
            }

            if (numLiterals != 0)
            {
                // Currently this always holds, which makes peekNextAvailLength simpler.
                // If this changes, peekNextAvailLength should be adjusted accordingly.
                throw new InvalidOperationException("readValues called with existing values present");
            }
            // repeat the value for length times
            isRepeating = true;
            // TODO: this is not so useful and V1 reader doesn't do that. Fix? Same if delta == 0
            for (int i = 0; i < len; i++)
            {
                literals[i] = val;
            }
            numLiterals = len;
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
                numLiterals = 0;
                used = 0;
                readValues(false);
            }
            result = literals[used++];
            return result;
        }

        public void seek(PositionProvider index)
        {
            input.seek(index);
            int consumed = (int)index.getNext();
            if (consumed != 0)
            {
                // a loop is required for cases where we break the run into two
                // parts
                while (consumed > 0)
                {
                    numLiterals = 0;
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
                    numLiterals = 0;
                    used = 0;
                    readValues(false);
                }
                long consume = Math.Min(numValues, numLiterals - used);
                used += (int)consume;
                numValues -= consume;
            }
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
                    && (previous.vector[i - 1] != previous.vector[i] ||
                    previous.isNull[i - 1] != previous.isNull[i]))
                {
                    previous.isRepeating = false;
                }
            }
        }

        public void setInStream(InStream data)
        {
            input = data;
        }
    }
}
