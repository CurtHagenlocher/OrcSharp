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
    using System.Collections.Generic;
    using System.Text;
    using OrcSharp.External;
    using OrcSharp.Serialization;
    using OrcSharp.Types;
    using OrcProto = global::orc.proto;

    sealed public class OrcStruct
    {
        private object[] fields;

        public OrcStruct(int children)
        {
            fields = new object[children];
        }

        public object getFieldValue(int fieldIndex)
        {
            return fields[fieldIndex];
        }

        public void setFieldValue(int fieldIndex, object value)
        {
            fields[fieldIndex] = value;
        }

        public int getNumFields()
        {
            return fields.Length;
        }

        /**
         * Change the number of fields in the struct. No effect if the number of
         * fields is the same. The old field values are copied to the new array.
         * @param numFields the new number of fields
         */
        public void setNumFields(int numFields)
        {
            if (fields.Length != numFields)
            {
                object[] oldFields = fields;
                fields = new object[numFields];
                Array.Copy(oldFields, 0, fields, 0,
                    Math.Min(oldFields.Length, numFields));
            }
        }

        /**
         * Destructively make this object link to other's values.
         * @param other the value to point to
         */
        void linkFields(OrcStruct other)
        {
            fields = other.fields;
        }

        public override bool Equals(object other)
        {
            OrcStruct oth = other as OrcStruct;
            if (other == null)
            {
                return false;
            }
            else
            {
                if (fields.Length != oth.fields.Length)
                {
                    return false;
                }
                for (int i = 0; i < fields.Length; ++i)
                {
                    if (fields[i] == null)
                    {
                        if (oth.fields[i] != null)
                        {
                            return false;
                        }
                    }
                    else
                    {
                        if (!fields[i].Equals(oth.fields[i]))
                        {
                            return false;
                        }
                    }
                }
                return true;
            }
        }

        public override int GetHashCode()
        {
            int result = fields.Length;
            foreach (object field in fields)
            {
                if (field != null)
                {
                    result ^= field.GetHashCode();
                }
            }
            return result;
        }

        public override string ToString()
        {
            StringBuilder buffer = new StringBuilder();
            buffer.Append("{");
            for (int i = 0; i < fields.Length; ++i)
            {
                if (i != 0)
                {
                    buffer.Append(", ");
                }
                buffer.Append(fields[i]);
            }
            buffer.Append("}");
            return buffer.ToString();
        }

        public class Field : StructField
        {
            private string name;
            private ObjectInspector inspector;
            internal int offset;

            public Field(string name, ObjectInspector inspector, int offset)
            {
                this.name = name;
                this.inspector = inspector;
                this.offset = offset;
            }

            public override string getFieldName()
            {
                return name;
            }

            public override ObjectInspector getFieldObjectInspector()
            {
                return inspector;
            }

            public int getFieldID()
            {
                return offset;
            }

            public override string getFieldComment()
            {
                return null;
            }
        }

        class OrcStructInspector : SettableStructObjectInspector
        {
            private IList<StructField> fields;

            protected OrcStructInspector()
            {
            }

            public OrcStructInspector(IList<StructField> fields)
            {
                this.fields = fields;
            }

            public OrcStructInspector(StructTypeInfo info)
            {
                List<string> fieldNames = info.getAllStructFieldNames();
                List<TypeInfo> fieldTypes = info.getAllStructFieldTypeInfos();
                fields = new List<StructField>(fieldNames.Count);
                for (int i = 0; i < fieldNames.Count; ++i)
                {
                    fields.Add(new Field(fieldNames[i],
                      createObjectInspector(fieldTypes[i]), i));
                }
            }

            public OrcStructInspector(int columnId, IList<OrcProto.Type> types)
            {
                OrcProto.Type type = types[columnId];
                int fieldCount = type.SubtypesCount;
                fields = new List<StructField>(fieldCount);
                for (int i = 0; i < fieldCount; ++i)
                {
                    int fieldType = (int)type.SubtypesList[i];
                    fields.Add(new Field(type.FieldNamesList[i],
                      createObjectInspector(fieldType, types), i));
                }
            }

            public override IList<StructField> getAllStructFieldRefs()
            {
                return fields;
            }

            public override StructField getStructFieldRef(string s)
            {
                foreach (StructField field in fields)
                {
                    if (string.Equals(field.getFieldName(), s, StringComparison.OrdinalIgnoreCase))
                    {
                        return field;
                    }
                }
                return null;
            }

            public override object getStructFieldData(object @object, StructField field)
            {
                if (@object == null)
                {
                    return null;
                }
                int offset = ((Field)field).offset;
                OrcStruct @struct = (OrcStruct)@object;
                if (offset >= @struct.fields.Length)
                {
                    return null;
                }

                return @struct.fields[offset];
            }

            public override List<object> getStructFieldsDataAsList(object @object)
            {
                if (@object == null)
                {
                    return null;
                }
                OrcStruct @struct = (OrcStruct)@object;
                List<object> result = new List<object>(@struct.fields.Length);
                foreach (object child in @struct.fields)
                {
                    result.Add(child);
                }
                return result;
            }

            public override string getTypeName()
            {
                StringBuilder buffer = new StringBuilder();
                buffer.Append("struct<");
                for (int i = 0; i < fields.Count; ++i)
                {
                    StructField field = fields[i];
                    if (i != 0)
                    {
                        buffer.Append(",");
                    }
                    buffer.Append(field.getFieldName());
                    buffer.Append(":");
                    buffer.Append(field.getFieldObjectInspector().getTypeName());
                }
                buffer.Append(">");
                return buffer.ToString();
            }

            public override ObjectInspectorCategory getCategory()
            {
                return ObjectInspectorCategory.STRUCT;
            }

            public object create()
            {
                return new OrcStruct(0);
            }

            public object setStructFieldData(object @struct, StructField field, object fieldValue)
            {
                OrcStruct orcStruct = (OrcStruct)@struct;
                int offset = ((Field)field).offset;
                // if the offset is bigger than our current number of fields, grow it
                if (orcStruct.getNumFields() <= offset)
                {
                    orcStruct.setNumFields(offset + 1);
                }
                orcStruct.setFieldValue(offset, fieldValue);
                return @struct;
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
                    IList<StructField> other = ((OrcStructInspector)o).fields;
                    if (other.Count != fields.Count)
                    {
                        return false;
                    }
                    for (int i = 0; i < fields.Count; ++i)
                    {
                        StructField left = other[i];
                        StructField right = fields[i];
                        if (!string.Equals(left.getFieldName(), right.getFieldName(), StringComparison.OrdinalIgnoreCase)
                            && left.getFieldObjectInspector().Equals(right.getFieldObjectInspector()))
                        {
                            return false;
                        }
                    }
                    return true;
                }
            }
        }

        class OrcMapObjectInspector : MapObjectInspector, SettableMapObjectInspector
        {
            private ObjectInspector key;
            private ObjectInspector value;

            private OrcMapObjectInspector()
            {
            }

            public OrcMapObjectInspector(MapTypeInfo info)
            {
                key = createObjectInspector(info.getMapKeyTypeInfo());
                value = createObjectInspector(info.getMapValueTypeInfo());
            }

            public OrcMapObjectInspector(int columnId, IList<OrcProto.Type> types)
            {
                OrcProto.Type type = types[columnId];
                key = createObjectInspector((int)type.SubtypesList[0], types);
                value = createObjectInspector((int)type.SubtypesList[1], types);
            }

            public override ObjectInspector getMapKeyObjectInspector()
            {
                return key;
            }

            public override ObjectInspector getMapValueObjectInspector()
            {
                return value;
            }

            public override object getMapValueElement(object map, object key)
            {
                return ((map == null || key == null) ? null : ((IDictionary<object, object>)map).get(key));
            }

            public override IDictionary<object, object> getMap(object map)
            {
                return map as IDictionary<object, object>;
            }

            public override int getMapSize(object map)
            {
                if (map == null)
                {
                    return -1;
                }
                return ((IDictionary<object, object>)map).Count;
            }

            public override string getTypeName()
            {
                return "map<" + key.getTypeName() + "," + value.getTypeName() + ">";
            }

            public override ObjectInspectorCategory getCategory()
            {
                return ObjectInspectorCategory.MAP;
            }

            public object create()
            {
                return new Dictionary<object, object>();
            }

            public object put(object map, object key, object value)
            {
                ((IDictionary<object, object>)map)[key] = value;
                return map;
            }

            public object remove(object map, object key)
            {
                ((IDictionary<object, object>)map).Remove(key);
                return map;
            }

            public object clear(object map)
            {
                ((IDictionary<object, object>)map).Clear();
                return map;
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
                    OrcMapObjectInspector other = (OrcMapObjectInspector)o;
                    return other.key.Equals(key) && other.value.Equals(value);
                }
            }
        }

        class OrcListObjectInspector : ListObjectInspector, SettableListObjectInspector
        {
            private ObjectInspector child;

            private OrcListObjectInspector()
            {
            }

            public OrcListObjectInspector(ListTypeInfo info)
            {
                child = createObjectInspector(info.getListElementTypeInfo());
            }

            public OrcListObjectInspector(int columnId, IList<OrcProto.Type> types)
            {
                OrcProto.Type type = types[columnId];
                child = createObjectInspector((int)type.SubtypesList[0], types);
            }

            public override ObjectInspector getListElementObjectInspector()
            {
                return child;
            }

            public override object getListElement(object list, int i)
            {
                if (list == null || i < 0 || i >= getListLength(list))
                {
                    return null;
                }
                return ((IList<object>)list)[i];
            }

            public override int getListLength(object list)
            {
                if (list == null)
                {
                    return -1;
                }
                return ((IList<object>)list).Count;
            }

            public override IList<object> getList(object list)
            {
                return (IList<object>)list;
            }

            public override string getTypeName()
            {
                return "array<" + child.getTypeName() + ">";
            }

            public override ObjectInspectorCategory getCategory()
            {
                return ObjectInspectorCategory.LIST;
            }

            public object create(int size)
            {
                List<object> result = new List<object>(size);
                for (int i = 0; i < size; ++i)
                {
                    result.Add(null);
                }
                return result;
            }

            public object set(object list, int index, object element)
            {
                IList<object> l = (IList<object>)list;
                for (int i = l.Count; i < index + 1; ++i)
                {
                    l.Add(null);
                }
                l[index] = element;
                return list;
            }

            public object resize(object list, int newSize)
            {
                // TODO:
                // ((IList<object>)list).ensureCapacity(newSize);
                return list;
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
                    ObjectInspector other = ((OrcListObjectInspector)o).child;
                    return other.Equals(child);
                }
            }
        }

        static public ObjectInspector createObjectInspector(TypeInfo info)
        {
            switch (info.getCategory())
            {
                case ObjectInspectorCategory.PRIMITIVE:
                    switch (((PrimitiveTypeInfo)info).getPrimitiveCategory())
                    {
                        case PrimitiveCategory.FLOAT:
                            return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
                        case PrimitiveCategory.DOUBLE:
                            return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
                        case PrimitiveCategory.BOOLEAN:
                            return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
                        case PrimitiveCategory.BYTE:
                            return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
                        case PrimitiveCategory.SHORT:
                            return PrimitiveObjectInspectorFactory.writableShortObjectInspector;
                        case PrimitiveCategory.INT:
                            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
                        case PrimitiveCategory.LONG:
                            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
                        case PrimitiveCategory.BINARY:
                            return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
                        case PrimitiveCategory.STRING:
                            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
                        case PrimitiveCategory.CHAR:
                            return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                                (PrimitiveTypeInfo)info);
                        case PrimitiveCategory.VARCHAR:
                            return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                                (PrimitiveTypeInfo)info);
                        case PrimitiveCategory.TIMESTAMP:
                            return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
                        case PrimitiveCategory.DATE:
                            return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
                        case PrimitiveCategory.DECIMAL:
                            return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                                (PrimitiveTypeInfo)info);
                        default:
                            throw new ArgumentException("Unknown primitive type " +
                              ((PrimitiveTypeInfo)info).getPrimitiveCategory());
                    }
                case ObjectInspectorCategory.STRUCT:
                    return new OrcStructInspector((StructTypeInfo)info);
                case ObjectInspectorCategory.UNION:
                    return new OrcUnion.OrcUnionObjectInspector((UnionTypeInfo)info);
                case ObjectInspectorCategory.MAP:
                    return new OrcMapObjectInspector((MapTypeInfo)info);
                case ObjectInspectorCategory.LIST:
                    return new OrcListObjectInspector((ListTypeInfo)info);
                default:
                    throw new ArgumentException("Unknown type " +
                      info.getCategory());
            }
        }

        public static ObjectInspector createObjectInspector(int columnId, IList<OrcProto.Type> types)
        {
            OrcProto.Type type = types[columnId];
            switch (type.Kind)
            {
                case OrcProto.Type.Types.Kind.FLOAT:
                    return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
                case OrcProto.Type.Types.Kind.DOUBLE:
                    return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
                case OrcProto.Type.Types.Kind.BOOLEAN:
                    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
                case OrcProto.Type.Types.Kind.BYTE:
                    return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
                case OrcProto.Type.Types.Kind.SHORT:
                    return PrimitiveObjectInspectorFactory.writableShortObjectInspector;
                case OrcProto.Type.Types.Kind.INT:
                    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
                case OrcProto.Type.Types.Kind.LONG:
                    return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
                case OrcProto.Type.Types.Kind.BINARY:
                    return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
                case OrcProto.Type.Types.Kind.STRING:
                    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
                case OrcProto.Type.Types.Kind.CHAR:
                    if (!type.HasMaximumLength)
                    {
                        throw new NotSupportedException(
                            "Illegal use of char type without length in ORC type definition.");
                    }
                    return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                        TypeInfoFactory.getCharTypeInfo((int)type.MaximumLength));
                case OrcProto.Type.Types.Kind.VARCHAR:
                    if (!type.HasMaximumLength)
                    {
                        throw new NotSupportedException(
                            "Illegal use of varchar type without length in ORC type definition.");
                    }
                    return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                        TypeInfoFactory.getVarcharTypeInfo((int)type.MaximumLength));
                case OrcProto.Type.Types.Kind.TIMESTAMP:
                    return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
                case OrcProto.Type.Types.Kind.DATE:
                    return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
                case OrcProto.Type.Types.Kind.DECIMAL:
                    int precision = type.HasPrecision ? (int)type.Precision : HiveDecimal.SYSTEM_DEFAULT_PRECISION;
                    int scale = type.HasScale ? (int)type.Scale : HiveDecimal.SYSTEM_DEFAULT_SCALE;
                    return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                        TypeInfoFactory.getDecimalTypeInfo(precision, scale));
                case OrcProto.Type.Types.Kind.STRUCT:
                    return new OrcStructInspector(columnId, types);
                case OrcProto.Type.Types.Kind.UNION:
                    return new OrcUnion.OrcUnionObjectInspector(columnId, types);
                case OrcProto.Type.Types.Kind.MAP:
                    return new OrcMapObjectInspector(columnId, types);
                case OrcProto.Type.Types.Kind.LIST:
                    return new OrcListObjectInspector(columnId, types);
                default:
                    throw new NotSupportedException("Unknown type " +
                      type.Kind);
            }
        }
    }
}
