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

namespace org.apache.hadoop.hive.ql.io.orc
{
    using System;
    using OrcProto = global::orc.proto;

    /**
     * The name of a stream within a stripe.
     */
    public class StreamName : IComparable<StreamName>
    {
        private int column;
        private OrcProto.Stream.Types.Kind kind;

        public enum Area
        {
            DATA, INDEX
        }

        public StreamName(int column, OrcProto.Stream.Types.Kind kind)
        {
            this.column = column;
            this.kind = kind;
        }

        public override bool Equals(object obj)
        {
            if (obj != null && obj is StreamName)
            {
                StreamName other = (StreamName)obj;
                return other.column == column && other.kind == kind;
            }
            else
            {
                return false;
            }
        }

        public int CompareTo(StreamName streamName)
        {
            if (streamName == null)
            {
                return -1;
            }
            Area area = getArea(kind);
            Area otherArea = StreamName.getArea(streamName.kind);
            if (area != otherArea)
            {
                return -area.CompareTo(otherArea);
            }
            if (column != streamName.column)
            {
                return column < streamName.column ? -1 : 1;
            }
            return kind.CompareTo(streamName.kind);
        }

        public int getColumn()
        {
            return column;
        }

        public OrcProto.Stream.Types.Kind getKind()
        {
            return kind;
        }

        public Area getArea()
        {
            return getArea(kind);
        }

        public static Area getArea(OrcProto.Stream.Types.Kind kind)
        {
            switch (kind)
            {
                case OrcProto.Stream.Types.Kind.ROW_INDEX:
                case OrcProto.Stream.Types.Kind.DICTIONARY_COUNT:
                case OrcProto.Stream.Types.Kind.BLOOM_FILTER:
                    return Area.INDEX;
                default:
                    return Area.DATA;
            }
        }

        public override string ToString()
        {
            return "Stream for column " + column + " kind " + kind;
        }

        public override int GetHashCode()
        {
            return column * 101 + (int)kind;
        }
    }
}
