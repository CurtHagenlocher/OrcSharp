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
    using System.Collections.Generic;

    public class ObjectInspector
    {
        public virtual string getTypeName()
        {
            throw new NotImplementedException();
        }

        public virtual ObjectInspectorCategory getCategory()
        {
            throw new NotImplementedException();
        }
    }

    class PrimitiveObjectInspector : ObjectInspector
    {
        public override ObjectInspectorCategory getCategory()
        {
            return ObjectInspectorCategory.PRIMITIVE;
        }

        public virtual PrimitiveCategory getPrimitiveCategory()
        {
            throw new NotImplementedException();
        }

        public virtual TypeInfo getTypeInfo()
        {
            throw new NotImplementedException();
        }
    }

    class ListObjectInspector : ObjectInspector
    {
        public virtual ObjectInspector getListElementObjectInspector()
        {
            throw new NotImplementedException();
        }

        public virtual object getListElement(object list, int position)
        {
            throw new NotImplementedException();
        }

        public virtual int getListLength(object list)
        {
            throw new NotImplementedException();
        }
    }

    class MapObjectInspector : ObjectInspector
    {
        public virtual ObjectInspector getMapKeyObjectInspector()
        {
            throw new NotImplementedException();
        }

        public virtual ObjectInspector getMapValueObjectInspector()
        {
            throw new NotImplementedException();
        }

        public virtual int getMapSize(object map)
        {
            throw new NotImplementedException();
        }

        public virtual object getMapValueElement(object map, object key)
        {
            throw new NotImplementedException();
        }

        public virtual IDictionary<object, object> getMap(object obj)
        {
            throw new NotImplementedException();
        }
    }

    public class StructObjectInspector : ObjectInspector
    {
        public virtual IList<StructField> getAllStructFieldRefs()
        {
            throw new NotImplementedException();
        }

        public virtual StructField getStructFieldRef(string fieldName)
        {
            throw new NotImplementedException();
        }

        public virtual object getStructFieldData(object data, StructField fieldRef)
        {
            throw new NotImplementedException();
        }

        public virtual List<object> getStructFieldsDataAsList(object data)
        {
            throw new NotImplementedException();
        }
    }

    class SettableStructObjectInspector : StructObjectInspector
    {
    }

    class UnionObjectInspector : ObjectInspector
    {
        public virtual IList<ObjectInspector> getObjectInspectors()
        {
            throw new NotImplementedException();
        }

        public virtual byte getTag(object obj)
        {
            throw new NotImplementedException();
        }

        public virtual object getField(object obj)
        {
            throw new NotImplementedException();
        }
    }

    class LongObjectInspector : PrimitiveObjectInspector
    {
        internal long get(object obj)
        {
            throw new NotImplementedException();
        }
    }

    class BooleanObjectInspector : PrimitiveObjectInspector
    {
        internal bool get(object obj)
        {
            throw new NotImplementedException();
        }
    }

    class ShortObjectInspector : PrimitiveObjectInspector
    {
        internal short get(object obj)
        {
            throw new NotImplementedException();
        }
    }

    class IntObjectInspector : PrimitiveObjectInspector
    {
        internal int get(object obj)
        {
            throw new NotImplementedException();
        }
    }

    class ByteObjectInspector : PrimitiveObjectInspector
    {
        internal byte get(object obj)
        {
            throw new NotImplementedException();
        }
    }

    class StringObjectInspector : PrimitiveObjectInspector
    {
        public override string getTypeName()
        {
            return serdeConstants.STRING_TYPE_NAME;
        }

        public override PrimitiveCategory getPrimitiveCategory()
        {
            return PrimitiveCategory.STRING;
        }

        public override TypeInfo getTypeInfo()
        {
            return TypeInfoFactory.stringTypeInfo;
        }

        internal string get(object obj)
        {
            return (string)obj;
        }
    }

    class BinaryObjectInspector : PrimitiveObjectInspector
    {
        internal byte[] get(object obj)
        {
            throw new NotImplementedException();
        }
    }

    class FloatObjectInspector : PrimitiveObjectInspector
    {
        internal float get(object obj)
        {
            throw new NotImplementedException();
        }
    }

    class DoubleObjectInspector : PrimitiveObjectInspector
    {
        internal double get(object obj)
        {
            throw new NotImplementedException();
        }
    }

    class HiveCharObjectInspector : PrimitiveObjectInspector
    {
        internal string get(object obj)
        {
            throw new NotImplementedException();
        }
    }

    class HiveVarcharObjectInspector : PrimitiveObjectInspector
    {
        internal string get()
        {
            throw new NotImplementedException();
        }
    }

    class HiveDecimalObjectInspector : PrimitiveObjectInspector
    {
        internal HiveDecimal get()
        {
            throw new NotImplementedException();
        }
    }

    class DateObjectInspector : PrimitiveObjectInspector
    {
        internal DateTime get(object obj)
        {
            throw new NotImplementedException();
        }
    }

    class TimestampObjectInspector : PrimitiveObjectInspector
    {
        internal DateTime get()
        {
            throw new NotImplementedException();
        }
    }

    public enum ObjectInspectorCategory
    {
        PRIMITIVE,
        LIST,
        MAP,
        STRUCT,
        UNION
    }

    public enum PrimitiveCategory
    {
        BOOLEAN,
        BYTE,
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        STRING,
        CHAR,
        VARCHAR,
        BINARY,
        TIMESTAMP,
        DATE,
        DECIMAL,
        UNKNOWN,
        VOID,
    }

    public abstract class TypeInfo
    {
        public abstract ObjectInspectorCategory getCategory();

        public virtual object getTypeName()
        {
            throw new NotImplementedException();
        }
    }

    public class PrimitiveTypeInfo : TypeInfo
    {
        private readonly string typeName;
        private readonly PrimitiveCategory primitiveCategory;

        public PrimitiveTypeInfo(string typeName, PrimitiveCategory primitiveCategory)
        {
            this.typeName = typeName;
            this.primitiveCategory = primitiveCategory;
        }

        internal PrimitiveTypeInfo(Dictionary<string, TypeInfo> types, string typeName, PrimitiveCategory primitiveCategory)
            : this(typeName, primitiveCategory)
        {
            types.Add(typeName, this);
        }

        public override ObjectInspectorCategory getCategory()
        {
            return ObjectInspectorCategory.PRIMITIVE;
        }

        public PrimitiveCategory getPrimitiveCategory()
        {
            return primitiveCategory;
        }

        public override object getTypeName()
        {
            return typeName;
        }
    }

    public abstract class BaseCharTypeInfo : PrimitiveTypeInfo
    {
        public BaseCharTypeInfo(string name, PrimitiveCategory primitiveCategory)
            : base(name, primitiveCategory)
        {
        }

        public abstract int getLength();
    }

    class CharTypeInfo : BaseCharTypeInfo
    {
        readonly int maxLength;

        public CharTypeInfo(int maxLength)
            : base(serdeConstants.CHAR_TYPE_NAME, PrimitiveCategory.CHAR)
        {
            this.maxLength = maxLength;
        }

        public override int getLength()
        {
            return maxLength;
        }
    }

    class VarcharTypeInfo : BaseCharTypeInfo
    {
        readonly int maxLength;

        public VarcharTypeInfo(int maxLength)
            : base(serdeConstants.VARCHAR_TYPE_NAME, PrimitiveCategory.VARCHAR)
        {
            this.maxLength = maxLength;
        }

        public override int getLength()
        {
            return maxLength;
        }
    }

    class DecimalTypeInfo : PrimitiveTypeInfo
    {
        readonly int _precision;
        readonly int _scale;

        public DecimalTypeInfo(int precision, int scale)
            : base(serdeConstants.DECIMAL_TYPE_NAME, PrimitiveCategory.DECIMAL)
        {
            this._precision = precision;
            this._scale = scale;
        }

        internal int precision()
        {
            return _precision;
        }

        internal int scale()
        {
            return _scale;
        }
    }

    class StructTypeInfo : TypeInfo
    {
        private readonly List<string> fieldNames;
        private readonly List<TypeInfo> fieldTypes;

        public StructTypeInfo(List<string> fieldNames, List<TypeInfo> fieldTypes)
        {
            this.fieldNames = fieldNames;
            this.fieldTypes = fieldTypes;
        }

        public override ObjectInspectorCategory getCategory()
        {
            return ObjectInspectorCategory.STRUCT;
        }

        internal List<string> getAllStructFieldNames()
        {
            return this.fieldNames;
        }

        internal List<TypeInfo> getAllStructFieldTypeInfos()
        {
            return this.fieldTypes;
        }

        internal TypeInfo getStructFieldTypeInfo(string fieldName)
        {
            for (int i = 0; i < fieldNames.Count; i++)
            {
                if (fieldNames[i] == fieldName)
                {
                    return fieldTypes[i];
                }
            }
            return null;
        }
    }

    class ListTypeInfo : TypeInfo
    {
        private readonly TypeInfo elementTypeInfo;

        public ListTypeInfo(TypeInfo elementTypeInfo)
        {
            this.elementTypeInfo = elementTypeInfo;
        }

        public override ObjectInspectorCategory getCategory()
        {
            return ObjectInspectorCategory.LIST;
        }

        internal TypeInfo getListElementTypeInfo()
        {
            return elementTypeInfo;
        }
    }

    class MapTypeInfo : TypeInfo
    {
        private readonly TypeInfo keyTypeInfo;
        private readonly TypeInfo valueTypeInfo;

        public MapTypeInfo(TypeInfo keyTypeInfo, TypeInfo valueTypeInfo)
        {
            this.keyTypeInfo = keyTypeInfo;
            this.valueTypeInfo = valueTypeInfo;

        }

        public override ObjectInspectorCategory getCategory()
        {
            return ObjectInspectorCategory.MAP;
        }

        internal TypeInfo getMapKeyTypeInfo()
        {
            return keyTypeInfo;
        }

        internal TypeInfo getMapValueTypeInfo()
        {
            return valueTypeInfo;
        }
    }

    class UnionTypeInfo : TypeInfo
    {
        private readonly List<TypeInfo> unionTypeInfos;

        public UnionTypeInfo(List<TypeInfo> unionTypeInfos)
        {
            this.unionTypeInfos = unionTypeInfos;
        }

        public override ObjectInspectorCategory getCategory()
        {
            return ObjectInspectorCategory.UNION;
        }

        internal List<TypeInfo> getAllUnionObjectTypeInfos()
        {
            return this.unionTypeInfos;
        }
    }

    class TypeInfoFactory
    {
        private static readonly Dictionary<string, TypeInfo> types = new Dictionary<string, TypeInfo>();
        public static readonly PrimitiveTypeInfo unknownTypeInfo = new PrimitiveTypeInfo("unknown", PrimitiveCategory.UNKNOWN);
        public static readonly PrimitiveTypeInfo voidTypeInfo = new PrimitiveTypeInfo(types, serdeConstants.VOID_TYPE_NAME, PrimitiveCategory.VOID);
        public static readonly PrimitiveTypeInfo booleanTypeInfo = new PrimitiveTypeInfo(types, serdeConstants.BOOLEAN_TYPE_NAME, PrimitiveCategory.BOOLEAN);
        public static readonly PrimitiveTypeInfo intTypeInfo = new PrimitiveTypeInfo(types, serdeConstants.INT_TYPE_NAME, PrimitiveCategory.INT);
        public static readonly PrimitiveTypeInfo longTypeInfo = new PrimitiveTypeInfo(types, serdeConstants.BIGINT_TYPE_NAME, PrimitiveCategory.LONG);
        public static readonly PrimitiveTypeInfo stringTypeInfo = new PrimitiveTypeInfo(types, serdeConstants.STRING_TYPE_NAME, PrimitiveCategory.STRING);
        public static readonly PrimitiveTypeInfo charTypeInfo = new CharTypeInfo(HiveChar.MAX_CHAR_LENGTH);
        public static readonly PrimitiveTypeInfo varcharTypeInfo = new VarcharTypeInfo(HiveVarchar.MAX_VARCHAR_LENGTH);
        public static readonly PrimitiveTypeInfo floatTypeInfo = new PrimitiveTypeInfo(types, serdeConstants.FLOAT_TYPE_NAME, PrimitiveCategory.FLOAT);
        public static readonly PrimitiveTypeInfo doubleTypeInfo = new PrimitiveTypeInfo(types, serdeConstants.DOUBLE_TYPE_NAME, PrimitiveCategory.DOUBLE);
        public static readonly PrimitiveTypeInfo byteTypeInfo = new PrimitiveTypeInfo(types, serdeConstants.TINYINT_TYPE_NAME, PrimitiveCategory.BYTE);
        public static readonly PrimitiveTypeInfo shortTypeInfo = new PrimitiveTypeInfo(types, serdeConstants.SMALLINT_TYPE_NAME, PrimitiveCategory.SHORT);
        public static readonly PrimitiveTypeInfo dateTypeInfo = new PrimitiveTypeInfo(types, serdeConstants.DATE_TYPE_NAME, PrimitiveCategory.DATE);
        public static readonly PrimitiveTypeInfo timestampTypeInfo = new PrimitiveTypeInfo(types, serdeConstants.TIMESTAMP_TYPE_NAME, PrimitiveCategory.TIMESTAMP);
        public static readonly PrimitiveTypeInfo intervalYearMonthTypeInfo = new PrimitiveTypeInfo(types, serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME, PrimitiveCategory.DATE);
        public static readonly PrimitiveTypeInfo intervalDayTimeTypeInfo = new PrimitiveTypeInfo(types, serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME, PrimitiveCategory.DATE);
        public static readonly PrimitiveTypeInfo binaryTypeInfo = new PrimitiveTypeInfo(types, serdeConstants.BINARY_TYPE_NAME, PrimitiveCategory.BINARY);

        public static PrimitiveTypeInfo getCharTypeInfo(int maxLength)
        {
            return new CharTypeInfo(maxLength);
        }

        internal static PrimitiveTypeInfo getVarcharTypeInfo(int maxLength)
        {
            return new VarcharTypeInfo(maxLength);
        }

        internal static PrimitiveTypeInfo getDecimalTypeInfo(int precision, int scale)
        {
            return new DecimalTypeInfo(precision, scale);
        }

        internal static TypeInfo getListTypeInfo(TypeInfo typeInfo)
        {
            return new ListTypeInfo(typeInfo);
        }

        internal static TypeInfo getMapTypeInfo(TypeInfo keyTypeInfo, TypeInfo valueTypeInfo)
        {
            return new MapTypeInfo(keyTypeInfo, valueTypeInfo);
        }

        internal static TypeInfo getStructTypeInfo(List<string> fieldNames, List<TypeInfo> fieldTypeInfos)
        {
            return new StructTypeInfo(fieldNames, fieldTypeInfos);
        }

        internal static TypeInfo getUnionTypeInfo(List<TypeInfo> objectTypeInfos)
        {
            return new UnionTypeInfo(objectTypeInfos);
        }

        internal static TypeInfo getPrimitiveTypeInfo(string typeName)
        {
            TypeInfo result;
            if (types.TryGetValue(typeName, out result))
            {
                return result;
            }
            return null;
        }
    }

    class ObjectInspectorFactory
    {
        internal static ObjectInspector getStandardListObjectInspector(ObjectInspector elementObjectInspector)
        {
            throw new NotImplementedException();
        }

        internal static ObjectInspector getStandardMapObjectInspector(ObjectInspector keyObjectInspector, ObjectInspector valueObjectInspector)
        {
            throw new NotImplementedException();
        }

        internal static ObjectInspector getStandardStructObjectInspector(List<string> fieldNames, List<ObjectInspector> fieldObjectInspectors)
        {
            throw new NotImplementedException();
        }

        internal static ObjectInspector getStandardUnionObjectInspector(List<ObjectInspector> fieldObjectInspectors)
        {
            throw new NotImplementedException();
        }
    }

    class PrimitiveObjectInspectorFactory
    {
        public static readonly ObjectInspector writableFloatObjectInspector = new PrimitiveObjectInspector<float>(serdeConstants.FLOAT_TYPE_NAME);
        public static readonly ObjectInspector writableDoubleObjectInspector = new PrimitiveObjectInspector<double>(serdeConstants.DOUBLE_TYPE_NAME);
        public static readonly ObjectInspector writableBooleanObjectInspector = new PrimitiveObjectInspector<bool>(serdeConstants.BOOLEAN_TYPE_NAME);
        public static readonly ObjectInspector writableByteObjectInspector = new PrimitiveObjectInspector<sbyte>(serdeConstants.TINYINT_TYPE_NAME);
        public static readonly ObjectInspector writableShortObjectInspector = new PrimitiveObjectInspector<short>(serdeConstants.SMALLINT_TYPE_NAME);
        public static readonly ObjectInspector writableIntObjectInspector = new PrimitiveObjectInspector<int>(serdeConstants.INT_TYPE_NAME);
        public static readonly ObjectInspector writableLongObjectInspector = new PrimitiveObjectInspector<long>(serdeConstants.BIGINT_TYPE_NAME);
        public static readonly ObjectInspector writableBinaryObjectInspector = new PrimitiveObjectInspector<byte[]>(serdeConstants.BINARY_TYPE_NAME);
        public static readonly ObjectInspector writableStringObjectInspector = new StringObjectInspector();
        public static readonly ObjectInspector writableTimestampObjectInspector = new PrimitiveObjectInspector<DateTime>(serdeConstants.TIMESTAMP_TYPE_NAME);
        public static readonly ObjectInspector writableDateObjectInspector = new PrimitiveObjectInspector<DateTime>(serdeConstants.DATE_TYPE_NAME);

        internal static ObjectInspector getPrimitiveWritableObjectInspector(PrimitiveTypeInfo primitiveTypeInfo)
        {
            throw new NotImplementedException();
        }

        internal static ObjectInspector getPrimitiveJavaObjectInspector(PrimitiveCategory primitiveCategory)
        {
            throw new NotImplementedException();
        }

        internal static ObjectInspector getPrimitiveWritableObjectInspector(PrimitiveCategory primitiveCategory)
        {
            throw new NotImplementedException();
        }

        internal static ObjectInspector getPrimitiveJavaObjectInspector(PrimitiveTypeInfo primitiveTypeInfo)
        {
            throw new NotImplementedException();
        }

        class PrimitiveObjectInspector<T> : ObjectInspector
        {
            private readonly string typeName;

            public PrimitiveObjectInspector(string typeName)
            {
                this.typeName = typeName;
            }

            public override string getTypeName()
            {
                return typeName;
            }
        }
    }

    interface SettableMapObjectInspector
    {
    }

    interface SettableListObjectInspector
    {
    }

    public class TypeEntry
    {
    }

    public class PrimitiveTypeEntry : TypeEntry
    {
        public PrimitiveCategory primitiveCategory;
        public string typeName;
    }

    public class PrimitiveObjectInspectorUtils
    {
        internal static bool isPrimitiveJavaType(Type t)
        {
            throw new NotImplementedException();
        }

        internal static bool isPrimitiveJavaClass(Type t)
        {
            throw new NotImplementedException();
        }

        internal static bool isPrimitiveWritableClass(Type t)
        {
            throw new NotImplementedException();
        }

        internal static PrimitiveTypeEntry getTypeEntryFromTypeName(string typeName)
        {
            PrimitiveTypeInfo info = TypeInfoFactory.getPrimitiveTypeInfo(typeName) as PrimitiveTypeInfo;
            if (info != null)
            {
                return new PrimitiveTypeEntry
                {
                    primitiveCategory = info.getPrimitiveCategory(),
                    typeName = typeName,
                };
            }
            return null;
        }

        internal static PrimitiveTypeEntry getTypeEntryFromPrimitiveJavaType(Type t)
        {
            throw new NotImplementedException();
        }

        internal static PrimitiveTypeEntry getTypeEntryFromPrimitiveJavaClass(Type t)
        {
            throw new NotImplementedException();
        }

        internal static PrimitiveTypeEntry getTypeEntryFromPrimitiveWritableClass(Type t)
        {
            throw new NotImplementedException();
        }

        internal static PrimitiveGrouping getPrimitiveGrouping(PrimitiveCategory from)
        {
            throw new NotImplementedException();
        }
    }

    public class ObjectInspectorUtils
    {
        internal static System.Reflection.FieldInfo[] getDeclaredNonStaticFields(Type t)
        {
            throw new NotImplementedException();
        }
    }

    public enum PrimitiveGrouping
    {
        NUMERIC_GROUP, STRING_GROUP, BOOLEAN_GROUP, DATE_GROUP, INTERVAL_GROUP, BINARY_GROUP,
        VOID_GROUP, UNKNOWN_GROUP
    }

    public class BaseCharUtils
    {
        internal static void validateVarcharParameter(int length)
        {
            throw new NotImplementedException();
        }

        internal static void validateCharParameter(int length)
        {
            throw new NotImplementedException();
        }
    }

    public class HiveDecimalUtils
    {
        internal static void validateParameter(int precision, int scale)
        {
            throw new NotImplementedException();
        }
    }

    public class HiveChar
    {
        public const int MAX_CHAR_LENGTH = 1000;
    }

    public class HiveVarchar
    {
        public const int MAX_VARCHAR_LENGTH = 1000;
    }
}
