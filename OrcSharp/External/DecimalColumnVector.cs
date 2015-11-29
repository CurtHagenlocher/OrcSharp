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

namespace org.apache.hadoop.hive.ql.io.orc.external
{
    using System;
    using System.Numerics;
    using System.Text;

    public class DecimalColumnVector : ColumnVector
    {
        /**
         * A vector of HiveDecimalWritable objects.
         *
         * For high performance and easy access to this low-level structure,
         * the fields are public by design (as they are in other ColumnVector
         * types).
         */
        public HiveDecimal[] vector;
        public short scale;
        public short precision;

        public DecimalColumnVector(int precision, int scale) :
            this(DEFAULT_SIZE, precision, scale)
        {
        }

        public DecimalColumnVector(int size, int precision, int scale)
            : base(size)
        {
            this.precision = (short)precision;
            this.scale = (short)scale;
            vector = new HiveDecimal[size];
            for (int i = 0; i < size; i++)
            {
                vector[i] = HiveDecimal.Zero;
            }
        }

        // Fill the all the vector entries with provided value
        public void fill(HiveDecimal value)
        {
            noNulls = true;
            isRepeating = true;
            vector[0] = value;
        }

        // Fill the column vector with nulls
        public void fillWithNulls()
        {
            noNulls = false;
            isRepeating = true;
            vector[0] = null;
            isNull[0] = true;
        }

        public override void flatten(bool selectedInUse, int[] sel, int size)
        {
            // TODO Auto-generated method stub
        }

        public override void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector)
        {
            if (inputVector.isRepeating)
            {
                inputElementNum = 0;
            }
            if (inputVector.noNulls || !inputVector.isNull[inputElementNum])
            {
                HiveDecimal hiveDec =
                    ((DecimalColumnVector)inputVector).vector[inputElementNum];
                if (hiveDec == null)
                {
                    isNull[outElementNum] = true;
                    noNulls = false;
                }
                else
                {
                    isNull[outElementNum] = false;
                    vector[outElementNum] = hiveDec;
                }
            }
            else
            {
                isNull[outElementNum] = true;
                noNulls = false;
            }
        }

        public override void stringifyValue(StringBuilder buffer, int row)
        {
            if (isRepeating)
            {
                row = 0;
            }
            if (noNulls || !isNull[row])
            {
                buffer.Append(vector[row].ToString());
            }
            else
            {
                buffer.Append("null");
            }
        }

        public void set(int elementNum, HiveDecimal hiveDec)
        {
            HiveDecimal checkedDec = HiveDecimal.enforcePrecisionScale(hiveDec, precision, scale);
            if (checkedDec == null)
            {
                noNulls = false;
                isNull[elementNum] = true;
            }
            else
            {
                vector[elementNum] = checkedDec;
            }
        }

        public void setNullDataValue(int elementNum)
        {
            // E.g. For scale 2 the minimum is "0.01"
            HiveDecimal minimumNonZeroValue = HiveDecimal.create(BigInteger.One, scale);
            vector[elementNum] = minimumNonZeroValue;
        }

        public override void ensureSize(int size, bool preserveData)
        {
            if (size > vector.Length)
            {
                base.ensureSize(size, preserveData);
                HiveDecimal[] oldArray = vector;
                vector = new HiveDecimal[size];
                if (preserveData)
                {
                    // we copy all of the values to avoid creating more objects
                    Array.Copy(oldArray, 0, vector, 0, oldArray.Length);
                    for (int i = oldArray.Length; i < vector.Length; ++i)
                    {
                        vector[i] = HiveDecimal.Zero;
                    }
                }
            }
        }
    }
}
