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

namespace OrcSharp.Types
{
    using System;
    using OrcSharp.External;

    public struct Date : IEquatable<Date>, IComparable<Date>
    {
        private readonly int days;

        public Date(int days)
        {
            this.days = days;
        }

        public Date(long milliseconds)
        {
            this.days = Epoch.getTimestamp(milliseconds).getDays();
        }

        public Date(DateTime date)
        {
            days = Epoch.getDays(date);
        }

        public Date(int year, int month, int day)
        {
            days = Epoch.getDays(new DateTime(year + 1900, month, day));
        }

        public static Date Parse(string date)
        {
            return new Date(DateTime.Parse(date));
        }

        public DateTime AsDateTime
        {
            get { return Epoch.getDate(days); }
        }

        public override string ToString()
        {
            return Epoch.getDate(days).ToString("yyyy-MM-dd");
        }

        public override int GetHashCode()
        {
            return days.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (obj is Date)
            {
                return Equals((Date)obj);
            }
            return false;
        }

        public bool Equals(Date other)
        {
            return days == other.days;
        }

        public int CompareTo(Date other)
        {
            return days.CompareTo(other.days);
        }
    }
}
