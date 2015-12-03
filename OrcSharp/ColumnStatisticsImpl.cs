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
    using System.Text;
    using OrcSharp.External;
    using OrcSharp.Types;
    using OrcProto = global::orc.proto;

    class ColumnStatisticsImpl : ColumnStatistics
    {
        class BooleanStatisticsImpl : ColumnStatisticsImpl, BooleanColumnStatistics
        {
            private ulong trueCount = 0;

            public BooleanStatisticsImpl(OrcProto.ColumnStatistics stats)
                : base(stats)
            {
                trueCount = stats.BucketStatistics.CountList[0];
            }

            public BooleanStatisticsImpl()
            {
            }

            public override void reset()
            {
                base.reset();
                trueCount = 0;
            }

            protected internal override void updateBoolean(bool value, int repetitions = 1)
            {
                if (value)
                {
                    trueCount += (uint)repetitions;
                }
            }

            public override void merge(ColumnStatisticsImpl other)
            {
                if (other is BooleanStatisticsImpl)
                {
                    BooleanStatisticsImpl bkt = (BooleanStatisticsImpl)other;
                    trueCount += bkt.trueCount;
                }
                else
                {
                    if (isStatsExists() && trueCount != 0)
                    {
                        throw new ArgumentException("Incompatible merging of bool column statistics");
                    }
                }
                base.merge(other);
            }

            public override OrcProto.ColumnStatistics.Builder serialize()
            {
                OrcProto.ColumnStatistics.Builder builder = base.serialize();
                OrcProto.BucketStatistics.Builder bucket =
                  OrcProto.BucketStatistics.CreateBuilder();
                bucket.AddCount(trueCount);
                builder.SetBucketStatistics(bucket);
                return builder;
            }

            public long getFalseCount()
            {
                return getNumberOfValues() - (long)trueCount;
            }

            public long getTrueCount()
            {
                return (long)trueCount;
            }

            public override string ToString()
            {
                return base.ToString() + " true: " + trueCount;
            }
        }

        class IntegerStatisticsImpl : ColumnStatisticsImpl, IntegerColumnStatistics
        {
            private long minimum = Int64.MaxValue;
            private long maximum = Int64.MinValue;
            private long sum = 0;
            private bool hasMinimum = false;
            private bool overflow = false;

            public IntegerStatisticsImpl()
            {
            }

            public IntegerStatisticsImpl(OrcProto.ColumnStatistics stats)
                : base(stats)
            {
                OrcProto.IntegerStatistics intStat = stats.IntStatistics;
                if (intStat.HasMinimum)
                {
                    hasMinimum = true;
                    minimum = intStat.Minimum;
                }
                if (intStat.HasMaximum)
                {
                    maximum = intStat.Maximum;
                }
                if (intStat.HasSum)
                {
                    sum = intStat.Sum;
                }
                else
                {
                    overflow = true;
                }
            }

            public override void reset()
            {
                base.reset();
                hasMinimum = false;
                minimum = Int64.MaxValue;
                maximum = Int64.MinValue;
                sum = 0;
                overflow = false;
            }

            protected internal override void updateInteger(long value, int repetitions = 1)
            {
                if (!hasMinimum)
                {
                    hasMinimum = true;
                    minimum = value;
                    maximum = value;
                }
                else if (value < minimum)
                {
                    minimum = value;
                }
                else if (value > maximum)
                {
                    maximum = value;
                }
                if (!overflow)
                {
                    bool wasPositive = sum >= 0;
                    sum += (value * repetitions);
                    if ((value >= 0) == wasPositive)
                    {
                        overflow = (sum >= 0) != wasPositive;
                    }
                }
            }

            public override void merge(ColumnStatisticsImpl other)
            {
                if (other is IntegerStatisticsImpl)
                {
                    IntegerStatisticsImpl otherInt = (IntegerStatisticsImpl)other;
                    if (!hasMinimum)
                    {
                        hasMinimum = otherInt.hasMinimum;
                        minimum = otherInt.minimum;
                        maximum = otherInt.maximum;
                    }
                    else if (otherInt.hasMinimum)
                    {
                        if (otherInt.minimum < minimum)
                        {
                            minimum = otherInt.minimum;
                        }
                        if (otherInt.maximum > maximum)
                        {
                            maximum = otherInt.maximum;
                        }
                    }

                    overflow |= otherInt.overflow;
                    if (!overflow)
                    {
                        bool wasPositive = sum >= 0;
                        sum += otherInt.sum;
                        if ((otherInt.sum >= 0) == wasPositive)
                        {
                            overflow = (sum >= 0) != wasPositive;
                        }
                    }
                }
                else
                {
                    if (isStatsExists() && hasMinimum)
                    {
                        throw new ArgumentException("Incompatible merging of integer column statistics");
                    }
                }
                base.merge(other);
            }

            public override OrcProto.ColumnStatistics.Builder serialize()
            {
                OrcProto.ColumnStatistics.Builder builder = base.serialize();
                OrcProto.IntegerStatistics.Builder intb =
                  OrcProto.IntegerStatistics.CreateBuilder();
                if (hasMinimum)
                {
                    intb.SetMinimum(minimum);
                    intb.SetMaximum(maximum);
                }
                if (!overflow)
                {
                    intb.SetSum(sum);
                }
                builder.SetIntStatistics(intb);
                return builder;
            }

            public long getMinimum()
            {
                return minimum;
            }

            public long getMaximum()
            {
                return maximum;
            }

            public bool isSumDefined()
            {
                return !overflow;
            }

            public long getSum()
            {
                return sum;
            }

            public override string ToString()
            {
                StringBuilder buf = new StringBuilder(base.ToString());
                if (hasMinimum)
                {
                    buf.Append(" min: ");
                    buf.Append(minimum);
                    buf.Append(" max: ");
                    buf.Append(maximum);
                }
                if (!overflow)
                {
                    buf.Append(" sum: ");
                    buf.Append(sum);
                }
                return buf.ToString();
            }
        }

        class DoubleStatisticsImpl : ColumnStatisticsImpl, DoubleColumnStatistics
        {
            private bool hasMinimum = false;
            private double minimum = Double.MaxValue;
            private double maximum = Double.MinValue;
            private double sum = 0;

            public DoubleStatisticsImpl()
            {
            }

            public DoubleStatisticsImpl(OrcProto.ColumnStatistics stats)
                : base(stats)
            {
                OrcProto.DoubleStatistics dbl = stats.DoubleStatistics;
                if (dbl.HasMinimum)
                {
                    hasMinimum = true;
                    minimum = dbl.Minimum;
                }
                if (dbl.HasMaximum)
                {
                    maximum = dbl.Maximum;
                }
                if (dbl.HasSum)
                {
                    sum = dbl.Sum;
                }
            }

            public override void reset()
            {
                base.reset();
                hasMinimum = false;
                minimum = Double.MaxValue;
                maximum = Double.MinValue;
                sum = 0;
            }

            protected internal override void updateDouble(double value)
            {
                if (!hasMinimum)
                {
                    hasMinimum = true;
                    minimum = value;
                    maximum = value;
                }
                else if (value < minimum)
                {
                    minimum = value;
                }
                else if (value > maximum)
                {
                    maximum = value;
                }
                sum += value;
            }

            public override void merge(ColumnStatisticsImpl other)
            {
                if (other is DoubleStatisticsImpl)
                {
                    DoubleStatisticsImpl dbl = (DoubleStatisticsImpl)other;
                    if (!hasMinimum)
                    {
                        hasMinimum = dbl.hasMinimum;
                        minimum = dbl.minimum;
                        maximum = dbl.maximum;
                    }
                    else if (dbl.hasMinimum)
                    {
                        if (dbl.minimum < minimum)
                        {
                            minimum = dbl.minimum;
                        }
                        if (dbl.maximum > maximum)
                        {
                            maximum = dbl.maximum;
                        }
                    }
                    sum += dbl.sum;
                }
                else
                {
                    if (isStatsExists() && hasMinimum)
                    {
                        throw new ArgumentException("Incompatible merging of double column statistics");
                    }
                }
                base.merge(other);
            }

            public override OrcProto.ColumnStatistics.Builder serialize()
            {
                OrcProto.ColumnStatistics.Builder builder = base.serialize();
                OrcProto.DoubleStatistics.Builder dbl = OrcProto.DoubleStatistics.CreateBuilder();
                if (hasMinimum)
                {
                    dbl.SetMinimum(minimum);
                    dbl.SetMaximum(maximum);
                }
                dbl.SetSum(sum);
                builder.SetDoubleStatistics(dbl);
                return builder;
            }

            public double getMinimum()
            {
                return minimum;
            }

            public double getMaximum()
            {
                return maximum;
            }

            public double getSum()
            {
                return sum;
            }

            public override string ToString()
            {
                StringBuilder buf = new StringBuilder(base.ToString());
                if (hasMinimum)
                {
                    buf.Append(" min: ");
                    buf.Append(minimum);
                    buf.Append(" max: ");
                    buf.Append(maximum);
                }
                buf.Append(" sum: ");
                buf.Append(sum);
                return buf.ToString();
            }
        }

        protected class StringStatisticsImpl : ColumnStatisticsImpl, StringColumnStatistics
        {
            private string minimum = null;
            private string maximum = null;
            private long sum = 0;

            public StringStatisticsImpl()
            {
            }

            public StringStatisticsImpl(OrcProto.ColumnStatistics stats)
                : base(stats)
            {
                OrcProto.StringStatistics str = stats.StringStatistics;
                if (str.HasMaximum)
                {
                    maximum = str.Maximum;
                }
                if (str.HasMinimum)
                {
                    minimum = str.Minimum;
                }
                if (str.HasSum)
                {
                    sum = str.Sum;
                }
            }

            public override void reset()
            {
                base.reset();
                minimum = null;
                maximum = null;
                sum = 0;
            }

            protected internal override void updateString(string value)
            {
                if (minimum == null)
                {
                    minimum = value;
                    maximum = value;
                }
                else if (string.CompareOrdinal(minimum, value) > 0)
                {
                    minimum = value;
                }
                else if (string.CompareOrdinal(maximum, value) < 0)
                {
                    maximum = value;
                }
                sum += value.Length;
            }

            protected internal override void updateString(byte[] value, int offset, int length, int repetitions = 1)
            {
                string s = Encoding.UTF8.GetString(value, offset, length);
                updateString(s);
                if (repetitions > 1)
                {
                    sum += s.Length * (repetitions - 1);
                }
            }

            public override void merge(ColumnStatisticsImpl other)
            {
                if (other is StringStatisticsImpl)
                {
                    StringStatisticsImpl str = (StringStatisticsImpl)other;
                    if (minimum == null)
                    {
                        if (str.minimum != null)
                        {
                            maximum = str.getMaximum();
                            minimum = str.getMinimum();
                        }
                        else
                        {
                            /* both are empty */
                            maximum = minimum = null;
                        }
                    }
                    else if (str.minimum != null)
                    {
                        if (string.CompareOrdinal(minimum, str.minimum) > 0)
                        {
                            minimum = str.minimum;
                        }
                        if (string.CompareOrdinal(maximum, str.maximum) < 0)
                        {
                            maximum = str.maximum;
                        }
                    }
                    sum += str.sum;
                }
                else
                {
                    if (isStatsExists() && minimum != null)
                    {
                        throw new ArgumentException("Incompatible merging of string column statistics");
                    }
                }
                base.merge(other);
            }

            public override OrcProto.ColumnStatistics.Builder serialize()
            {
                OrcProto.ColumnStatistics.Builder result = base.serialize();
                OrcProto.StringStatistics.Builder str =
                  OrcProto.StringStatistics.CreateBuilder();
                if (getNumberOfValues() != 0)
                {
                    str.SetMinimum(getMinimum());
                    str.SetMaximum(getMaximum());
                    str.SetSum(sum);
                }
                result.SetStringStatistics(str);
                return result;
            }

            public string getMinimum()
            {
                return minimum == null ? null : minimum.ToString();
            }

            public string getMaximum()
            {
                return maximum == null ? null : maximum.ToString();
            }

            public long getSum()
            {
                return sum;
            }

            public override string ToString()
            {
                StringBuilder buf = new StringBuilder(base.ToString());
                if (getNumberOfValues() != 0)
                {
                    buf.Append(" min: ");
                    buf.Append(getMinimum());
                    buf.Append(" max: ");
                    buf.Append(getMaximum());
                    buf.Append(" sum: ");
                    buf.Append(sum);
                }
                return buf.ToString();
            }
        }

        protected class BinaryStatisticsImpl : ColumnStatisticsImpl, BinaryColumnStatistics
        {
            private long sum = 0;

            public BinaryStatisticsImpl()
            {
            }

            public BinaryStatisticsImpl(OrcProto.ColumnStatistics stats)
                : base(stats)
            {
                OrcProto.BinaryStatistics binStats = stats.BinaryStatistics;
                if (binStats.HasSum)
                {
                    sum = binStats.Sum;
                }
            }

            public override void reset()
            {
                base.reset();
                sum = 0;
            }

            protected internal override void updateBinary(byte[] value)
            {
                sum += value.Length;
            }

            protected internal override void updateBinary(byte[] bytes, int offset, int length, int repetitions = 1)
            {
                sum += length * repetitions;
            }

            public override void merge(ColumnStatisticsImpl other)
            {
                if (other is BinaryColumnStatistics)
                {
                    BinaryStatisticsImpl bin = (BinaryStatisticsImpl)other;
                    sum += bin.sum;
                }
                else
                {
                    if (isStatsExists() && sum != 0)
                    {
                        throw new ArgumentException("Incompatible merging of binary column statistics");
                    }
                }
                base.merge(other);
            }

            public long getSum()
            {
                return sum;
            }

            public override OrcProto.ColumnStatistics.Builder serialize()
            {
                OrcProto.ColumnStatistics.Builder result = base.serialize();
                OrcProto.BinaryStatistics.Builder bin = OrcProto.BinaryStatistics.CreateBuilder();
                bin.SetSum(sum);
                result.SetBinaryStatistics(bin);
                return result;
            }

            public override string ToString()
            {
                StringBuilder buf = new StringBuilder(base.ToString());
                if (getNumberOfValues() != 0)
                {
                    buf.Append(" sum: ");
                    buf.Append(sum);
                }
                return buf.ToString();
            }
        }

        private class DecimalStatisticsImpl : ColumnStatisticsImpl, DecimalColumnStatistics
        {
            private HiveDecimal minimum = null;
            private HiveDecimal maximum = null;
            private HiveDecimal sum = HiveDecimal.Zero;

            public DecimalStatisticsImpl()
            {
            }

            public DecimalStatisticsImpl(OrcProto.ColumnStatistics stats)
                : base(stats)
            {
                OrcProto.DecimalStatistics dec = stats.DecimalStatistics;
                if (dec.HasMaximum)
                {
                    maximum = HiveDecimal.Parse(dec.Maximum);
                }
                if (dec.HasMinimum)
                {
                    minimum = HiveDecimal.Parse(dec.Minimum);
                }
                if (dec.HasSum)
                {
                    sum = HiveDecimal.Parse(dec.Sum);
                }
                else
                {
                    sum = null;
                }
            }

            public override void reset()
            {
                base.reset();
                minimum = null;
                maximum = null;
                sum = HiveDecimal.Zero;
            }

            protected internal override void updateDecimal(HiveDecimal value)
            {
                if (minimum == null)
                {
                    minimum = value;
                    maximum = value;
                }
                else if (minimum.CompareTo(value) > 0)
                {
                    minimum = value;
                }
                else if (maximum.CompareTo(value) < 0)
                {
                    maximum = value;
                }
                if (sum != null)
                {
                    sum += value;
                }
            }

            public override void merge(ColumnStatisticsImpl other)
            {
                if (other is DecimalStatisticsImpl)
                {
                    DecimalStatisticsImpl dec = (DecimalStatisticsImpl)other;
                    if (minimum == null)
                    {
                        minimum = dec.minimum;
                        maximum = dec.maximum;
                        sum = dec.sum;
                    }
                    else if (dec.minimum != null)
                    {
                        if (minimum.CompareTo(dec.minimum) > 0)
                        {
                            minimum = dec.minimum;
                        }
                        if (maximum.CompareTo(dec.maximum) < 0)
                        {
                            maximum = dec.maximum;
                        }
                        if (sum == null || dec.sum == null)
                        {
                            sum = null;
                        }
                        else
                        {
                            sum += dec.sum;
                        }
                    }
                }
                else
                {
                    if (isStatsExists() && minimum != null)
                    {
                        throw new ArgumentException("Incompatible merging of decimal column statistics");
                    }
                }
                base.merge(other);
            }

            public override OrcProto.ColumnStatistics.Builder serialize()
            {
                OrcProto.ColumnStatistics.Builder result = base.serialize();
                OrcProto.DecimalStatistics.Builder dec =
                    OrcProto.DecimalStatistics.CreateBuilder();
                if (getNumberOfValues() != 0 && minimum != null)
                {
                    dec.SetMinimum(minimum.ToString());
                    dec.SetMaximum(maximum.ToString());
                }
                if (sum != null)
                {
                    dec.SetSum(sum.ToString());
                }
                result.SetDecimalStatistics(dec);
                return result;
            }

            public HiveDecimal getMinimum()
            {
                return minimum;
            }

            public HiveDecimal getMaximum()
            {
                return maximum;
            }

            public HiveDecimal getSum()
            {
                return sum;
            }

            public override string ToString()
            {
                StringBuilder buf = new StringBuilder(base.ToString());
                if (getNumberOfValues() != 0)
                {
                    buf.Append(" min: ");
                    buf.Append(minimum);
                    buf.Append(" max: ");
                    buf.Append(maximum);
                    if (sum != null)
                    {
                        buf.Append(" sum: ");
                        buf.Append(sum);
                    }
                }
                return buf.ToString();
            }
        }

        private class DateStatisticsImpl : ColumnStatisticsImpl, DateColumnStatistics
        {
            private Date? minimum = null;
            private Date? maximum = null;

            public DateStatisticsImpl()
            {
            }

            public DateStatisticsImpl(OrcProto.ColumnStatistics stats)
                : base(stats)
            {
                OrcProto.DateStatistics dateStats = stats.DateStatistics;
                // min,max values serialized/deserialized as int (days since epoch)
                if (dateStats.HasMaximum)
                {
                    maximum = new Date(dateStats.Maximum);
                }
                if (dateStats.HasMinimum)
                {
                    minimum = new Date(dateStats.Minimum);
                }
            }

            public override void reset()
            {
                base.reset();
                minimum = null;
                maximum = null;
            }

            protected internal override void updateDate(Date value)
            {
                if (minimum == null)
                {
                    minimum = value;
                    maximum = value;
                }
                else if (minimum > value)
                {
                    minimum = value;
                }
                else if (maximum < value)
                {
                    maximum = value;
                }
            }

            protected internal override void updateDate(int value)
            {
                updateDate(new Date(value));
            }

            public override void merge(ColumnStatisticsImpl other)
            {
                if (other is DateStatisticsImpl)
                {
                    DateStatisticsImpl dateStats = (DateStatisticsImpl)other;
                    if (minimum == null)
                    {
                        minimum = dateStats.minimum;
                        maximum = dateStats.maximum;
                    }
                    else if (dateStats.minimum != null)
                    {
                        if (minimum > dateStats.minimum)
                        {
                            minimum = dateStats.minimum;
                        }
                        if (maximum < dateStats.maximum)
                        {
                            maximum = dateStats.maximum;
                        }
                    }
                }
                else
                {
                    if (isStatsExists() && minimum != null)
                    {
                        throw new ArgumentException("Incompatible merging of date column statistics");
                    }
                }
                base.merge(other);
            }

            public override OrcProto.ColumnStatistics.Builder serialize()
            {
                OrcProto.ColumnStatistics.Builder result = base.serialize();
                OrcProto.DateStatistics.Builder dateStats =
                    OrcProto.DateStatistics.CreateBuilder();
                if (getNumberOfValues() != 0 && minimum != null)
                {
                    dateStats.SetMinimum(minimum.Value.Days);
                    dateStats.SetMaximum(maximum.Value.Days);
                }
                result.SetDateStatistics(dateStats);
                return result;
            }

            public Date? getMinimum()
            {
                return minimum;
            }

            public Date? getMaximum()
            {
                return maximum;
            }

            public override string ToString()
            {
                StringBuilder buf = new StringBuilder(base.ToString());
                if (getNumberOfValues() != 0)
                {
                    buf.Append(" min: ");
                    buf.Append(getMinimum());
                    buf.Append(" max: ");
                    buf.Append(getMaximum());
                }
                return buf.ToString();
            }
        }

        private class TimestampStatisticsImpl : ColumnStatisticsImpl, TimestampColumnStatistics
        {
            private Timestamp? minimum = null;
            private Timestamp? maximum = null;

            public TimestampStatisticsImpl()
            {
            }

            public TimestampStatisticsImpl(OrcProto.ColumnStatistics stats)
                : base(stats)
            {
                OrcProto.TimestampStatistics timestampStats = stats.TimestampStatistics;
                // min,max values serialized/deserialized as int (milliseconds since epoch)
                if (timestampStats.HasMaximum)
                {
                    maximum = new Timestamp(timestampStats.Maximum);
                }
                if (timestampStats.HasMinimum)
                {
                    minimum = new Timestamp(timestampStats.Minimum);
                }
            }

            public override void reset()
            {
                base.reset();
                minimum = null;
                maximum = null;
            }

            protected internal override void updateTimestamp(Timestamp value)
            {
                if (minimum == null)
                {
                    minimum = value;
                    maximum = value;
                }
                else if (minimum > value)
                {
                    minimum = value;
                }
                else if (maximum < value)
                {
                    maximum = value;
                }
            }

            protected internal override void updateTimestamp(long milliseconds)
            {
                updateTimestamp(new Timestamp(milliseconds));
            }

            public override void merge(ColumnStatisticsImpl other)
            {
                if (other is TimestampStatisticsImpl)
                {
                    TimestampStatisticsImpl timestampStats = (TimestampStatisticsImpl)other;
                    if (minimum == null)
                    {
                        minimum = timestampStats.minimum;
                        maximum = timestampStats.maximum;
                    }
                    else if (timestampStats.minimum != null)
                    {
                        if (minimum > timestampStats.minimum)
                        {
                            minimum = timestampStats.minimum;
                        }
                        if (maximum < timestampStats.maximum)
                        {
                            maximum = timestampStats.maximum;
                        }
                    }
                }
                else
                {
                    if (isStatsExists() && minimum != null)
                    {
                        throw new ArgumentException("Incompatible merging of timestamp column statistics");
                    }
                }
                base.merge(other);
            }

            public override OrcProto.ColumnStatistics.Builder serialize()
            {
                OrcProto.ColumnStatistics.Builder result = base.serialize();
                OrcProto.TimestampStatistics.Builder timestampStats = OrcProto.TimestampStatistics
                    .CreateBuilder();
                if (getNumberOfValues() != 0 && minimum != null)
                {
                    timestampStats.SetMinimum(minimum.Value.Milliseconds);
                    timestampStats.SetMaximum(maximum.Value.Milliseconds);
                }
                result.SetTimestampStatistics(timestampStats);
                return result;
            }

            public Timestamp? getMinimum()
            {
                return minimum;
            }

            public Timestamp? getMaximum()
            {
                return maximum;
            }

            public override string ToString()
            {
                StringBuilder buf = new StringBuilder(base.ToString());
                if (getNumberOfValues() != 0)
                {
                    buf.Append(" min: ");
                    buf.Append(getMinimum());
                    buf.Append(" max: ");
                    buf.Append(getMaximum());
                }
                return buf.ToString();
            }
        }

        private ulong count = 0;
        private bool _hasNull = false;

        protected ColumnStatisticsImpl(OrcProto.ColumnStatistics stats)
        {
            if (stats.HasNumberOfValues)
            {
                count = stats.NumberOfValues;
            }

            if (stats.HasHasNull)
            {
                _hasNull = stats.HasNull;
            }
            else
            {
                _hasNull = true;
            }
        }

        protected ColumnStatisticsImpl()
        {
        }

        public void increment(int repetitions = 1)
        {
            count += (uint)repetitions;
        }

        public void setNull()
        {
            _hasNull = true;
        }

        protected internal virtual void updateBoolean(bool value, int repetitions = 1)
        {
            throw new NotSupportedException("Can't update boolean");
        }

        protected internal virtual void updateInteger(long value, int repetitions = 1)
        {
            throw new NotSupportedException("Can't update integer");
        }

        protected internal virtual void updateDouble(double value)
        {
            throw new NotSupportedException("Can't update double");
        }

        protected internal virtual void updateString(string value)
        {
            throw new NotSupportedException("Can't update string");
        }

        protected internal virtual void updateString(byte[] value, int offset, int length, int repetitions = 1)
        {
            throw new NotSupportedException("Can't update string");
        }

        protected internal virtual void updateBinary(byte[] value)
        {
            throw new NotSupportedException("Can't update binary");
        }

        protected internal virtual void updateBinary(byte[] bytes, int offset, int length, int repetitions = 1)
        {
            throw new NotSupportedException("Can't update binary");
        }

        protected internal virtual void updateDecimal(HiveDecimal value)
        {
            throw new NotSupportedException("Can't update decimal");
        }

        protected internal virtual void updateDate(Date value)
        {
            throw new NotSupportedException("Can't update date");
        }

        protected internal virtual void updateDate(int days)
        {
            throw new NotSupportedException("Can't update date");
        }

        protected internal virtual void updateTimestamp(Timestamp value)
        {
            throw new NotSupportedException("Can't update timestamp");
        }

        protected internal virtual void updateTimestamp(long milliseconds)
        {
            throw new NotSupportedException("Can't update timestamp");
        }

        bool isStatsExists()
        {
            return (count > 0 || _hasNull == true);
        }

        public virtual void merge(ColumnStatisticsImpl stats)
        {
            count += stats.count;
            _hasNull |= stats._hasNull;
        }

        public virtual void reset()
        {
            count = 0;
            _hasNull = false;
        }

        public long getNumberOfValues()
        {
            return (long)count;
        }

        public bool hasNull()
        {
            return _hasNull;
        }

        public override string ToString()
        {
            return "count: " + count + " hasNull: " + _hasNull;
        }

        public virtual OrcProto.ColumnStatistics.Builder serialize()
        {
            OrcProto.ColumnStatistics.Builder builder =
              OrcProto.ColumnStatistics.CreateBuilder();
            builder.SetNumberOfValues(count);
            builder.SetHasNull(_hasNull);
            return builder;
        }

        public static ColumnStatisticsImpl create(TypeDescription schema)
        {
            switch (schema.getCategory())
            {
                case Category.BOOLEAN:
                    return new BooleanStatisticsImpl();
                case Category.BYTE:
                case Category.SHORT:
                case Category.INT:
                case Category.LONG:
                    return new IntegerStatisticsImpl();
                case Category.FLOAT:
                case Category.DOUBLE:
                    return new DoubleStatisticsImpl();
                case Category.STRING:
                case Category.CHAR:
                case Category.VARCHAR:
                    return new StringStatisticsImpl();
                case Category.DECIMAL:
                    return new DecimalStatisticsImpl();
                case Category.DATE:
                    return new DateStatisticsImpl();
                case Category.TIMESTAMP:
                    return new TimestampStatisticsImpl();
                case Category.BINARY:
                    return new BinaryStatisticsImpl();
                default:
                    return new ColumnStatisticsImpl();
            }
        }

        public static ColumnStatisticsImpl deserialize(OrcProto.ColumnStatistics stats)
        {
            if (stats.HasBucketStatistics)
            {
                return new BooleanStatisticsImpl(stats);
            }
            else if (stats.HasIntStatistics)
            {
                return new IntegerStatisticsImpl(stats);
            }
            else if (stats.HasDoubleStatistics)
            {
                return new DoubleStatisticsImpl(stats);
            }
            else if (stats.HasStringStatistics)
            {
                return new StringStatisticsImpl(stats);
            }
            else if (stats.HasDecimalStatistics)
            {
                return new DecimalStatisticsImpl(stats);
            }
            else if (stats.HasDateStatistics)
            {
                return new DateStatisticsImpl(stats);
            }
            else if (stats.HasTimestampStatistics)
            {
                return new TimestampStatisticsImpl(stats);
            }
            else if (stats.HasBinaryStatistics)
            {
                return new BinaryStatisticsImpl(stats);
            }
            else
            {
                return new ColumnStatisticsImpl(stats);
            }
        }
    }
}
