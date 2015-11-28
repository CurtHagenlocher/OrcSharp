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
    using System.Linq;
    using System.Reflection;
    using System.Runtime.CompilerServices;

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
        private readonly PrimitiveTypeInfo typeInfo;

        protected PrimitiveObjectInspector(PrimitiveTypeInfo typeInfo)
        {
            this.typeInfo = typeInfo;
        }

        public override string getTypeName()
        {
            return typeInfo.getTypeName();
        }

        public override ObjectInspectorCategory getCategory()
        {
            return ObjectInspectorCategory.PRIMITIVE;
        }

        public virtual PrimitiveCategory getPrimitiveCategory()
        {
            return typeInfo.getPrimitiveCategory();
        }

        public virtual TypeInfo getTypeInfo()
        {
            return typeInfo;
        }
    }

    class ListObjectInspector : ObjectInspector
    {
        public override ObjectInspectorCategory getCategory()
        {
            return ObjectInspectorCategory.LIST;
        }

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

    class ArrayObjectInspector<T> : ListObjectInspector
    {
        private readonly ObjectInspector elementInspector;

        public ArrayObjectInspector(ObjectInspector elementInspector)
        {
            this.elementInspector = elementInspector;
        }

        public override ObjectInspector getListElementObjectInspector()
        {
            return elementInspector;
        }

        public override object getListElement(object list, int position)
        {
            return ((IList<T>)list)[position];
        }

        public override int getListLength(object list)
        {
            return ((IList<T>)list).Count;
        }
    }

    class MapObjectInspector : ObjectInspector
    {
        public override ObjectInspectorCategory getCategory()
        {
            return ObjectInspectorCategory.MAP;
        }

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

    class DictionaryObjectInspector<K, V> : MapObjectInspector
    {
        private readonly ObjectInspector keyInspector;
        private readonly ObjectInspector valueInspector;

        public DictionaryObjectInspector(ObjectInspector keyInspector, ObjectInspector valueInspector)
        {
            this.keyInspector = keyInspector;
            this.valueInspector = valueInspector;
        }

        public override ObjectInspector getMapKeyObjectInspector()
        {
            return keyInspector;
        }

        public override ObjectInspector getMapValueObjectInspector()
        {
            return valueInspector;
        }

        public override int getMapSize(object map)
        {
            return ((IDictionary<K, V>)map).Count;
        }

        public override object getMapValueElement(object map, object key)
        {
            return ((IDictionary<K, V>)map)[(K)key];
        }

        public override IDictionary<object, object> getMap(object map)
        {
            if (typeof(K) == typeof(object) && typeof(V) == typeof(object))
            {
                return (IDictionary<object, object>)map;
            }
            return ((IDictionary<K, V>)map).Select(pair => new KeyValuePair<object, object>(pair.Key, pair.Value)).ToDictionary(pair => pair.Key, pair => pair.Value);
        }
    }

    public class StructObjectInspector : ObjectInspector
    {
        public override ObjectInspectorCategory getCategory()
        {
            return ObjectInspectorCategory.STRUCT;
        }

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

    class FieldsObjectInspector : SettableStructObjectInspector
    {
        private FieldInfoField[] fields;

        public FieldsObjectInspector(FieldInfoField[] fields)
        {
            this.fields = fields;
        }

        public override IList<StructField> getAllStructFieldRefs()
        {
            return fields;
        }

        public override StructField getStructFieldRef(string fieldName)
        {
            return fields.Where(f => f.getFieldName() == fieldName).FirstOrDefault();
        }

        public override object getStructFieldData(object data, StructField fieldRef)
        {
            return ((FieldInfoField)fieldRef).GetFieldValue(data);
        }

        public override List<object> getStructFieldsDataAsList(object data)
        {
            throw new NotImplementedException();
        }
    }

    class UnionObjectInspector : ObjectInspector
    {
        public override ObjectInspectorCategory getCategory()
        {
            return ObjectInspectorCategory.UNION;
        }

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
        public LongObjectInspector()
            : base(TypeInfoFactory.longTypeInfo)
        {
        }

        public long get(object obj)
        {
            if (obj is IStrongBox)
            {
                obj = ((IStrongBox)obj).Value;
            }
            return (long)obj;
        }
    }

    class BooleanObjectInspector : PrimitiveObjectInspector
    {
        public BooleanObjectInspector()
            : base(TypeInfoFactory.booleanTypeInfo)
        {
        }

        internal bool get(object obj)
        {
            if (obj is IStrongBox)
            {
                obj = ((IStrongBox)obj).Value;
            }
            return (bool)obj;
        }
    }

    class ShortObjectInspector : PrimitiveObjectInspector
    {
        public ShortObjectInspector()
            : base(TypeInfoFactory.shortTypeInfo)
        {
        }

        internal short get(object obj)
        {
            if (obj is IStrongBox)
            {
                obj = ((IStrongBox)obj).Value;
            }
            return (short)obj;
        }
    }

    class IntObjectInspector : PrimitiveObjectInspector
    {
        public IntObjectInspector()
            : base(TypeInfoFactory.intTypeInfo)
        {
        }

        internal int get(object obj)
        {
            if (obj is IStrongBox)
            {
                obj = ((IStrongBox)obj).Value;
            }
            return (int)obj;
        }
    }

    class ByteObjectInspector : PrimitiveObjectInspector
    {
        public ByteObjectInspector()
            : base(TypeInfoFactory.byteTypeInfo)
        {
        }

        internal byte get(object obj)
        {
            if (obj is IStrongBox)
            {
                obj = ((IStrongBox)obj).Value;
            }
            return (byte)obj;
        }
    }

    class StringObjectInspector : PrimitiveObjectInspector
    {
        public StringObjectInspector()
            : base(TypeInfoFactory.stringTypeInfo)
        {
        }

        internal string get(object obj)
        {
            return (string)obj;
        }
    }

    class BinaryObjectInspector : PrimitiveObjectInspector
    {
        public BinaryObjectInspector()
            : base(TypeInfoFactory.binaryTypeInfo)
        {
        }

        internal byte[] get(object obj)
        {
            return (byte[])obj;
        }
    }

    class FloatObjectInspector : PrimitiveObjectInspector
    {
        public FloatObjectInspector()
            : base(TypeInfoFactory.floatTypeInfo)
        {
        }

        internal float get(object obj)
        {
            if (obj is IStrongBox)
            {
                obj = ((IStrongBox)obj).Value;
            }
            return (float)obj;
        }
    }

    class DoubleObjectInspector : PrimitiveObjectInspector
    {
        public DoubleObjectInspector()
            : base(TypeInfoFactory.doubleTypeInfo)
        {
        }

        internal double get(object obj)
        {
            if (obj is IStrongBox)
            {
                obj = ((IStrongBox)obj).Value;
            }
            return (double)obj;
        }
    }

    class HiveCharObjectInspector : PrimitiveObjectInspector
    {
        public HiveCharObjectInspector()
            : base(null)
        {
        }

        internal string get(object obj)
        {
            throw new NotImplementedException();
        }
    }

    class HiveVarcharObjectInspector : PrimitiveObjectInspector
    {
        public HiveVarcharObjectInspector()
            : base(null)
        {
        }

        internal string get()
        {
            throw new NotImplementedException();
        }
    }

    class HiveDecimalObjectInspector : PrimitiveObjectInspector
    {
        public HiveDecimalObjectInspector(int precision, int scale)
            : base(TypeInfoFactory.getDecimalTypeInfo(precision, scale))
        {
        }

        internal HiveDecimal get(object obj)
        {
            return (HiveDecimal)obj;
        }
    }

    class DateObjectInspector : PrimitiveObjectInspector
    {
        public DateObjectInspector()
            : base(TypeInfoFactory.dateTypeInfo)
        {
        }

        internal DateTime get(object obj)
        {
            if (obj is IStrongBox)
            {
                obj = ((IStrongBox)obj).Value;
            }
            return (DateTime)obj;
        }
    }

    class TimestampObjectInspector : PrimitiveObjectInspector
    {
        public TimestampObjectInspector()
            : base(TypeInfoFactory.timestampTypeInfo)
        {
        }

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

        public virtual string getTypeName()
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

        public override string getTypeName()
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
        static Dictionary<Type, ObjectInspector> cache = new Dictionary<Type, ObjectInspector>();

        internal static ObjectInspector getReflectionObjectInspector(Type type)
        {
            ObjectInspector result;
            lock (cache)
            {
                if (cache.TryGetValue(type, out result))
                {
                    return result;
                }
            }

            result = makeReflectionObjectInspector(type);

            lock (cache)
            {
                cache[type] = result;
            }
            return result;
        }

        private static ObjectInspector makeReflectionObjectInspector(Type type)
        {
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Boolean:
                    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
                case TypeCode.Byte:
                    return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
                case TypeCode.DateTime:
                    return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
                case TypeCode.Double:
                    return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
                case TypeCode.Int16:
                    return PrimitiveObjectInspectorFactory.writableShortObjectInspector;
                case TypeCode.Int32:
                    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
                case TypeCode.Int64:
                    return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
                case TypeCode.Single:
                    return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
                case TypeCode.String:
                    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
                case TypeCode.Char:
                case TypeCode.DBNull:
                case TypeCode.Decimal:
                case TypeCode.Empty:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    throw new NotSupportedException();
            }

            if (type == typeof(HiveDecimal))
            {
                return new HiveDecimalObjectInspector(HiveDecimal.MAX_PRECISION, HiveDecimal.MAX_SCALE);
            }

            if (type.IsArray && type.GetArrayRank() == 1)
            {
                Type elementType = type.GetElementType();
                ObjectInspector elementInspector = getReflectionObjectInspector(elementType);
                ConstructorInfo ctor = typeof(ArrayObjectInspector<>).MakeGenericType(elementType).GetConstructor(new[] { typeof(ObjectInspector) });
                return (ObjectInspector)ctor.Invoke(new[] { elementInspector });
            }
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
            {
                Type elementType = type.GetGenericArguments()[0];
                ObjectInspector elementInspector = getReflectionObjectInspector(elementType);
                ConstructorInfo ctor = typeof(ArrayObjectInspector<>).MakeGenericType(elementType).GetConstructor(new[] { typeof(ObjectInspector) });
                return (ObjectInspector)ctor.Invoke(new[] { elementInspector });
            }
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>))
            {
                Type[] types = type.GetGenericArguments();
                Type keyType = types[0];
                ObjectInspector keyInspector = getReflectionObjectInspector(keyType);
                Type valueType = types[0];
                ObjectInspector valueInspector = getReflectionObjectInspector(valueType);
                ConstructorInfo ctor = typeof(DictionaryObjectInspector<,>).MakeGenericType(keyType, valueType).GetConstructor(new[] { typeof(ObjectInspector), typeof(ObjectInspector) });
                return (ObjectInspector)ctor.Invoke(new[] { keyInspector, valueInspector });
            }

            FieldInfo[] objectFields = type.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            FieldInfoField[] fields = objectFields.Select(f => new FieldInfoField(f, getReflectionObjectInspector(f.FieldType))).ToArray();
            return new FieldsObjectInspector(fields);
        }

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
        public static readonly ObjectInspector writableFloatObjectInspector = new FloatObjectInspector();
        public static readonly ObjectInspector writableDoubleObjectInspector = new DoubleObjectInspector();
        public static readonly ObjectInspector writableBooleanObjectInspector = new BooleanObjectInspector();
        public static readonly ObjectInspector writableByteObjectInspector = new ByteObjectInspector();
        public static readonly ObjectInspector writableShortObjectInspector = new ShortObjectInspector();
        public static readonly ObjectInspector writableIntObjectInspector = new IntObjectInspector();
        public static readonly ObjectInspector writableLongObjectInspector = new LongObjectInspector();
        public static readonly ObjectInspector writableBinaryObjectInspector = new BinaryObjectInspector();
        public static readonly ObjectInspector writableStringObjectInspector = new StringObjectInspector();
        public static readonly ObjectInspector writableTimestampObjectInspector = new TimestampObjectInspector();
        public static readonly ObjectInspector writableDateObjectInspector = new DateObjectInspector();

        internal static ObjectInspector getPrimitiveWritableObjectInspector(PrimitiveTypeInfo primitiveTypeInfo)
        {
            switch (primitiveTypeInfo.getPrimitiveCategory())
            {
                case PrimitiveCategory.BINARY:
                    return writableBinaryObjectInspector;
                case PrimitiveCategory.BOOLEAN:
                    return writableBooleanObjectInspector;
                case PrimitiveCategory.BYTE:
                    return writableByteObjectInspector;
                case PrimitiveCategory.DATE:
                    return writableDateObjectInspector;
                case PrimitiveCategory.DECIMAL:
                    DecimalTypeInfo decimalInfo = (DecimalTypeInfo)primitiveTypeInfo;
                    return new HiveDecimalObjectInspector(decimalInfo.precision(), decimalInfo.scale());
                case PrimitiveCategory.DOUBLE:
                    return writableDoubleObjectInspector;
                case PrimitiveCategory.FLOAT:
                    return writableFloatObjectInspector;
                case PrimitiveCategory.INT:
                    return writableIntObjectInspector;
                case PrimitiveCategory.LONG:
                    return writableLongObjectInspector;
                case PrimitiveCategory.SHORT:
                    return writableShortObjectInspector;
                case PrimitiveCategory.STRING:
                    return writableStringObjectInspector;
                case PrimitiveCategory.TIMESTAMP:
                    return writableTimestampObjectInspector;
                default:
                    throw new NotImplementedException();
            }
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
