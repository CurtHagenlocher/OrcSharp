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
    using System.Collections.Generic;
    using System.Text;
    using OrcSharp.Serialization;
    using OrcProto = global::orc.proto;

    /// <summary>
    /// An in-memory representation of a union type.
    /// </summary>
    sealed public class OrcUnion
    {
        private byte tag;
        private object @object;

        public void set(byte tag, object @object)
        {
            this.tag = tag;
            this.@object = @object;
        }

        public byte getTag()
        {
            return tag;
        }

        public object getObject()
        {
            return @object;
        }

        public override bool Equals(object other)
        {
            OrcUnion union = other as OrcUnion;
            if (union == null)
            {
                return false;
            }
            if (tag != union.tag)
            {
                return false;
            }
            else if (@object == null)
            {
                return union.@object == null;
            }
            else
            {
                return object.ReferenceEquals(@object, union.@object);
            }
        }

        public override int GetHashCode()
        {
            int result = tag;
            if (@object != null)
            {
                result ^= @object.GetHashCode();
            }
            return result;
        }

        public override string ToString()
        {
            return "union(" + (tag & 0xff).ToString() + ", " + @object + ")";
        }

        internal class OrcUnionObjectInspector : UnionObjectInspector
        {
            private List<ObjectInspector> children;

            protected OrcUnionObjectInspector()
            {
            }

            public OrcUnionObjectInspector(int columnId, IList<OrcProto.Type> types)
            {
                OrcProto.Type type = types[columnId];
                children = new List<ObjectInspector>(type.SubtypesCount);
                for (int i = 0; i < type.SubtypesCount; ++i)
                {
                    children.Add(OrcStruct.createObjectInspector((int)type.SubtypesList[i],
                        types));
                }
            }

            public OrcUnionObjectInspector(UnionTypeInfo info)
            {
                List<TypeInfo> unionChildren = info.getAllUnionObjectTypeInfos();
                this.children = new List<ObjectInspector>(unionChildren.Count);
                foreach (TypeInfo child in info.getAllUnionObjectTypeInfos())
                {
                    this.children.Add(OrcStruct.createObjectInspector(child));
                }
            }

            public override IList<ObjectInspector> getObjectInspectors()
            {
                return children;
            }

            public override byte getTag(object obj)
            {
                return ((OrcUnion)obj).tag;
            }

            public override object getField(object obj)
            {
                return ((OrcUnion)obj).@object;
            }

            public override string getTypeName()
            {
                StringBuilder builder = new StringBuilder("uniontype<");
                bool first = true;
                foreach (ObjectInspector child in children)
                {
                    if (first)
                    {
                        first = false;
                    }
                    else
                    {
                        builder.Append(",");
                    }
                    builder.Append(child.getTypeName());
                }
                builder.Append(">");
                return builder.ToString();
            }

            public override ObjectInspectorCategory getCategory()
            {
                return ObjectInspectorCategory.UNION;
            }

            public override bool Equals(object o)
            {
                if (o == null || o.GetType() != GetType())
                {
                    return false;
                }
                else if (o == this)
                {
                    return true;
                }
                else
                {
                    List<ObjectInspector> other = ((OrcUnionObjectInspector)o).children;
                    if (other.Count != children.Count)
                    {
                        return false;
                    }
                    for (int i = 0; i < children.Count; ++i)
                    {
                        if (!other[i].Equals(children[i]))
                        {
                            return false;
                        }
                    }
                    return true;
                }
            }
        }
    }
}
