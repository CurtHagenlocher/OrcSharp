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

namespace OrcSharp.Serialization
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using OrcSharp.External;
    using OrcSharp.Types;

    /**
     * TypeInfoUtils.
     *
     */
    public static class TypeInfoUtils
    {
        public static List<PrimitiveCategory> numericTypeList = new List<PrimitiveCategory>();
        // The ordering of types here is used to determine which numeric types
        // are common/convertible to one another. Probably better to rely on the
        // ordering explicitly defined here than to assume that the enum values
        // that were arbitrarily assigned in PrimitiveCategory work for our purposes.
        public static ConcurrentDictionary<PrimitiveCategory, int> numericTypes =
            new ConcurrentDictionary<PrimitiveCategory, int>();

        static TypeInfoUtils()
        {
            registerNumericType(PrimitiveCategory.BYTE, 1);
            registerNumericType(PrimitiveCategory.SHORT, 2);
            registerNumericType(PrimitiveCategory.INT, 3);
            registerNumericType(PrimitiveCategory.LONG, 4);
            registerNumericType(PrimitiveCategory.FLOAT, 5);
            registerNumericType(PrimitiveCategory.DOUBLE, 6);
            registerNumericType(PrimitiveCategory.DECIMAL, 7);
            registerNumericType(PrimitiveCategory.STRING, 8);
        }

        /**
         * Return the extended TypeInfo from a Java type. By extended TypeInfo, we
         * allow unknownType for java.lang.Object.
         *
         * @param t
         *          The Java type.
         * @param m
         *          The method, only used for generating error messages.
         */
        private static TypeInfo getExtendedTypeInfoFromJavaType(Type t, MethodInfo m)
        {
            if (t == typeof(object))
            {
                return TypeInfoFactory.unknownTypeInfo;
            }

            if (t.IsGenericType)
            {
                Type bt = t.GetGenericTypeDefinition();
                Type[] pt = t.GenericTypeArguments;

                // List?
                if (bt == typeof(List<>))
                {
                    return TypeInfoFactory.getListTypeInfo(getExtendedTypeInfoFromJavaType(
                        pt[0], m));
                }
                // Map?
                if (bt == typeof(Dictionary<,>))
                {
                    return TypeInfoFactory.getMapTypeInfo(
                        getExtendedTypeInfoFromJavaType(pt[0], m),
                        getExtendedTypeInfoFromJavaType(pt[1], m));
                }
            }

            // Java Primitive Type?
            if (PrimitiveObjectInspectorUtils.isPrimitiveJavaType(t))
            {
                return TypeInfoUtils
                    .getTypeInfoFromObjectInspector(PrimitiveObjectInspectorFactory
                    .getPrimitiveJavaObjectInspector(PrimitiveObjectInspectorUtils
                    .getTypeEntryFromPrimitiveJavaType(t).primitiveCategory));
            }

            // Java Primitive Class?
            if (PrimitiveObjectInspectorUtils.isPrimitiveJavaClass(t))
            {
                return TypeInfoUtils
                    .getTypeInfoFromObjectInspector(PrimitiveObjectInspectorFactory
                    .getPrimitiveJavaObjectInspector(PrimitiveObjectInspectorUtils
                    .getTypeEntryFromPrimitiveJavaClass(t).primitiveCategory));
            }

            // Primitive Writable class?
            if (PrimitiveObjectInspectorUtils.isPrimitiveWritableClass(t))
            {
                return TypeInfoUtils
                    .getTypeInfoFromObjectInspector(PrimitiveObjectInspectorFactory
                    .getPrimitiveWritableObjectInspector(PrimitiveObjectInspectorUtils
                    .getTypeEntryFromPrimitiveWritableClass(t).primitiveCategory));
            }

            // Must be a struct
            FieldInfo[] fields = ObjectInspectorUtils.getDeclaredNonStaticFields(t);
            List<string> fieldNames = new List<string>(fields.Length);
            List<TypeInfo> fieldTypeInfos = new List<TypeInfo>(fields.Length);
            foreach (FieldInfo field in fields)
            {
                fieldNames.Add(field.Name);
                fieldTypeInfos.Add(getExtendedTypeInfoFromJavaType(
                    field.FieldType, m));
            }
            return TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypeInfos);
        }

        /**
         * Returns the array element type, if the Type is an array (Object[]), or
         * GenericArrayType (Map<String,String>[]). Otherwise return null.
         */
        public static Type getArrayElementType(Type t)
        {
            if (t.IsArray)
            {
                return t.GetElementType();
            }
            else if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(List<>))
            {
                return t.GenericTypeArguments[0];
            }
            return null;
        }

        /**
         * Get the parameter TypeInfo for a method.
         *
         * @param size
         *          In case the last parameter of Method is an array, we will try to
         *          return a List<TypeInfo> with the specified size by repeating the
         *          element of the array at the end. In case the size is smaller than
         *          the minimum possible number of arguments for the method, null will
         *          be returned.
         */
        public static List<TypeInfo> getParameterTypeInfos(MethodInfo m, int size)
        {
            Type[] methodParameterTypes = m.GetParameters().Select(p => p.ParameterType).ToArray();

            // Whether the method takes variable-length arguments
            // Whether the method takes an array like Object[],
            // or String[] etc in the last argument.
            Type lastParaElementType = TypeInfoUtils
                .getArrayElementType(methodParameterTypes.Length == 0 ? null
                : methodParameterTypes[methodParameterTypes.Length - 1]);
            bool isVariableLengthArgument = (lastParaElementType != null);

            List<TypeInfo> typeInfos = null;
            if (!isVariableLengthArgument)
            {
                // Normal case, no variable-length arguments
                if (size != methodParameterTypes.Length)
                {
                    return null;
                }
                typeInfos = new List<TypeInfo>(methodParameterTypes.Length);
                foreach (Type methodParameterType in methodParameterTypes)
                {
                    typeInfos.Add(getExtendedTypeInfoFromJavaType(methodParameterType, m));
                }
            }
            else
            {
                // Variable-length arguments
                if (size < methodParameterTypes.Length - 1)
                {
                    return null;
                }
                typeInfos = new List<TypeInfo>(size);
                for (int i = 0; i < methodParameterTypes.Length - 1; i++)
                {
                    typeInfos.Add(getExtendedTypeInfoFromJavaType(methodParameterTypes[i],
                        m));
                }
                for (int i = methodParameterTypes.Length - 1; i < size; i++)
                {
                    typeInfos.Add(getExtendedTypeInfoFromJavaType(lastParaElementType, m));
                }
            }
            return typeInfos;
        }

        public static bool hasParameters(string typeName)
        {
            int idx = typeName.IndexOf('(');
            if (idx == -1)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        public static string getBaseName(string typeName)
        {
            int idx = typeName.IndexOf('(');
            if (idx == -1)
            {
                return typeName;
            }
            else
            {
                return typeName.Substring(0, idx);
            }
        }

        /**
         * returns true if both TypeInfos are of primitive type, and the primitive category matches.
         * @param ti1
         * @param ti2
         * @return
         */
        public static bool doPrimitiveCategoriesMatch(TypeInfo ti1, TypeInfo ti2)
        {
            if (ti1.getCategory() == ObjectInspectorCategory.PRIMITIVE
                && ti2.getCategory() == ObjectInspectorCategory.PRIMITIVE)
            {
                if (((PrimitiveTypeInfo)ti1).getPrimitiveCategory()
                    == ((PrimitiveTypeInfo)ti2).getPrimitiveCategory())
                {
                    return true;
                }
            }
            return false;
        }

        /**
         * Parse a recursive TypeInfo list String. For example, the following inputs
         * are valid inputs:
         * "int,string,map<string,int>,list<map<int,list<string>>>,list<struct<a:int,b:string>>"
         * The separators between TypeInfos can be ",", ":", or ";".
         *
         * In order to use this class: TypeInfoParser parser = new
         * TypeInfoParser("int,string"); List<TypeInfo> typeInfos =
         * parser.parseTypeInfos();
         */
        private class TypeInfoParser
        {

            private class Token
            {
                public int position;
                public string text;
                public bool isType;

                public override string ToString()
                {
                    return "" + position + ":" + text;
                }
            };

            private static bool isTypeChar(char c)
            {
                return Char.IsLetterOrDigit(c) || c == '_' || c == '.' || c == ' ' || c == '$';
            }

            /**
             * Tokenize the typeInfoString. The rule is simple: all consecutive
             * alphadigits and '_', '.' are in one token, and all other characters are
             * one character per token.
             *
             * tokenize("map<int,string>") should return
             * ["map","<","int",",","string",">"]
             *
             * Note that we add '$' in new Calcite return path. As '$' will not appear
             * in any type in Hive, it is safe to do so.
             */
            private static List<Token> tokenize(string typeInfoString)
            {
                List<Token> tokens = new List<Token>();
                int begin = 0;
                int end = 1;
                while (end <= typeInfoString.Length)
                {
                    // last character ends a token?
                    if (end == typeInfoString.Length
                        || !isTypeChar(typeInfoString[end - 1])
                        || !isTypeChar(typeInfoString[end]))
                    {
                        Token t = new Token();
                        t.position = begin;
                        t.text = typeInfoString.Substring(begin, end - begin);
                        t.isType = isTypeChar(typeInfoString[begin]);
                        tokens.Add(t);
                        begin = end;
                    }
                    end++;
                }
                return tokens;
            }

            public TypeInfoParser(string typeInfoString)
            {
                this.typeInfoString = typeInfoString;
                typeInfoTokens = tokenize(typeInfoString);
            }

            private string typeInfoString;
            private List<Token> typeInfoTokens;
            private List<TypeInfo> typeInfos;
            private int iToken;

            public List<TypeInfo> parseTypeInfos()
            {
                typeInfos = new List<TypeInfo>();
                iToken = 0;
                while (iToken < typeInfoTokens.Count)
                {
                    typeInfos.Add(parseType());
                    if (iToken < typeInfoTokens.Count)
                    {
                        Token separator = typeInfoTokens[iToken];
                        if (",".Equals(separator.text) || ";".Equals(separator.text)
                            || ":".Equals(separator.text))
                        {
                            iToken++;
                        }
                        else
                        {
                            throw new ArgumentException(
                                "Error: ',', ':', or ';' expected at position "
                                + separator.position + " from '" + typeInfoString + "' "
                                + typeInfoTokens);
                        }
                    }
                }
                return typeInfos;
            }

            private Token peek()
            {
                if (iToken < typeInfoTokens.Count)
                {
                    return typeInfoTokens[iToken];
                }
                else
                {
                    return null;
                }
            }

            private Token expect(string item)
            {
                return expect(item, null);
            }

            private Token expect(string item, string alternative)
            {
                if (iToken >= typeInfoTokens.Count)
                {
                    throw new ArgumentException("Error: " + item
                        + " expected at the end of '" + typeInfoString + "'");
                }
                Token t = typeInfoTokens[iToken];
                if (item.Equals("type"))
                {
                    if (!serdeConstants.LIST_TYPE_NAME.Equals(t.text)
                        && !serdeConstants.MAP_TYPE_NAME.Equals(t.text)
                        && !serdeConstants.STRUCT_TYPE_NAME.Equals(t.text)
                        && !serdeConstants.UNION_TYPE_NAME.Equals(t.text)
                        && null == PrimitiveObjectInspectorUtils
                        .getTypeEntryFromTypeName(t.text)
                        && !t.text.Equals(alternative))
                    {
                        throw new ArgumentException("Error: " + item
                            + " expected at the position " + t.position + " of '"
                            + typeInfoString + "' but '" + t.text + "' is found.");
                    }
                }
                else if (item.Equals("name"))
                {
                    if (!t.isType && !t.text.Equals(alternative))
                    {
                        throw new ArgumentException("Error: " + item
                            + " expected at the position " + t.position + " of '"
                            + typeInfoString + "' but '" + t.text + "' is found.");
                    }
                }
                else
                {
                    if (!item.Equals(t.text) && !t.text.Equals(alternative))
                    {
                        throw new ArgumentException("Error: " + item
                            + " expected at the position " + t.position + " of '"
                            + typeInfoString + "' but '" + t.text + "' is found.");
                    }
                }
                iToken++;
                return t;
            }

            private string[] parseParams()
            {
                List<string> @params = new List<string>();

                Token t = peek();
                if (t != null && t.text.Equals("("))
                {
                    expect("(");

                    // checking for null in the for-loop condition prevents null-ptr exception
                    // and allows us to fail more gracefully with a parsing error.
                    for (t = peek(); (t == null) || !t.text.Equals(")"); t = expect(",", ")"))
                    {
                        @params.Add(expect("name").text);
                    }
                    if (@params.Count == 0)
                    {
                        throw new ArgumentException(
                            "type parameters expected for type string " + typeInfoString);
                    }
                }

                return @params.ToArray();
            }

            private TypeInfo parseType()
            {

                Token t = expect("type");

                // Is this a primitive type?
                PrimitiveTypeEntry typeEntry =
                    PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(t.text);
                if (typeEntry != null && typeEntry.primitiveCategory != PrimitiveCategory.UNKNOWN)
                {
                    String[] @params = parseParams();
                    switch (typeEntry.primitiveCategory)
                    {
                        case PrimitiveCategory.CHAR:
                        case PrimitiveCategory.VARCHAR:
                            if (@params == null || @params.Length == 0)
                            {
                                throw new ArgumentException(typeEntry.typeName
                                    + " type is specified without length: " + typeInfoString);
                            }

                            int length = 1;
                            if (@params.Length == 1)
                            {
                                length = Int32.Parse(@params[0]);
                                if (typeEntry.primitiveCategory == PrimitiveCategory.VARCHAR)
                                {
                                    BaseCharUtils.validateVarcharParameter(length);
                                    return TypeInfoFactory.getVarcharTypeInfo(length);
                                }
                                else
                                {
                                    BaseCharUtils.validateCharParameter(length);
                                    return TypeInfoFactory.getCharTypeInfo(length);
                                }
                            }
                            else if (@params.Length > 1)
                            {
                                throw new ArgumentException(
                                    "Type " + typeEntry.typeName + " only takes one parameter, but " +
                                    @params.Length + " is seen");
                            }
                            break;
                        case PrimitiveCategory.DECIMAL:
                            int precision = HiveDecimal.USER_DEFAULT_PRECISION;
                            int scale = HiveDecimal.USER_DEFAULT_SCALE;
                            if (@params == null || @params.Length == 0)
                            {
                                // It's possible that old metadata still refers to "decimal" as a column type w/o
                                // precision/scale. In this case, the default (10,0) is assumed. Thus, do nothing here.
                            }
                            else if (@params.Length == 2)
                            {
                                // New metadata always have two parameters.
                                precision = Int32.Parse(@params[0]);
                                scale = Int32.Parse(@params[1]);
                                HiveDecimalUtils.validateParameter(precision, scale);
                            }
                            else if (@params.Length > 2)
                            {
                                throw new ArgumentException("Type decimal only takes two parameter, but " +
                                    @params.Length + " is seen");
                            }

                            return TypeInfoFactory.getDecimalTypeInfo(precision, scale);
                        default:
                            return TypeInfoFactory.getPrimitiveTypeInfo(typeEntry.typeName);
                    }
                }

                // Is this a list type?
                if (serdeConstants.LIST_TYPE_NAME.Equals(t.text))
                {
                    expect("<");
                    TypeInfo listElementType = parseType();
                    expect(">");
                    return TypeInfoFactory.getListTypeInfo(listElementType);
                }

                // Is this a map type?
                if (serdeConstants.MAP_TYPE_NAME.Equals(t.text))
                {
                    expect("<");
                    TypeInfo mapKeyType = parseType();
                    expect(",");
                    TypeInfo mapValueType = parseType();
                    expect(">");
                    return TypeInfoFactory.getMapTypeInfo(mapKeyType, mapValueType);
                }

                // Is this a struct type?
                if (serdeConstants.STRUCT_TYPE_NAME.Equals(t.text))
                {
                    List<string> fieldNames = new List<string>();
                    List<TypeInfo> fieldTypeInfos = new List<TypeInfo>();
                    bool first = true;
                    do
                    {
                        if (first)
                        {
                            expect("<");
                            first = false;
                        }
                        else
                        {
                            Token separator = expect(">", ",");
                            if (separator.text.Equals(">"))
                            {
                                // end of struct
                                break;
                            }
                        }
                        Token name = expect("name", ">");
                        if (name.text.Equals(">"))
                        {
                            break;
                        }
                        fieldNames.Add(name.text);
                        expect(":");
                        fieldTypeInfos.Add(parseType());
                    } while (true);

                    return TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypeInfos);
                }
                // Is this a union type?
                if (serdeConstants.UNION_TYPE_NAME.Equals(t.text))
                {
                    List<TypeInfo> objectTypeInfos = new List<TypeInfo>();
                    bool first = true;
                    do
                    {
                        if (first)
                        {
                            expect("<");
                            first = false;
                        }
                        else
                        {
                            Token separator = expect(">", ",");
                            if (separator.text.Equals(">"))
                            {
                                // end of union
                                break;
                            }
                        }
                        objectTypeInfos.Add(parseType());
                    } while (true);

                    return TypeInfoFactory.getUnionTypeInfo(objectTypeInfos);
                }

                throw new ArgumentException("Internal error parsing position "
                    + t.position + " of '" + typeInfoString + "'");
            }

            public PrimitiveParts parsePrimitiveParts()
            {
                PrimitiveParts parts = new PrimitiveParts();
                Token t = expect("type");
                parts.typeName = t.text;
                parts.typeParams = parseParams();
                return parts;
            }
        }

        public class PrimitiveParts
        {
            public string typeName;
            public string[] typeParams;
        }

        /**
         * Make some of the TypeInfo parsing available as a utility.
         */
        public static PrimitiveParts parsePrimitiveParts(string typeInfoString)
        {
            TypeInfoParser parser = new TypeInfoParser(typeInfoString);
            return parser.parsePrimitiveParts();
        }

        static ConcurrentHashMap<TypeInfo, ObjectInspector> cachedStandardObjectInspector =
            new ConcurrentHashMap<TypeInfo, ObjectInspector>();

        /**
         * Returns the standard object inspector that can be used to translate an
         * object of that typeInfo to a standard object type.
         */
        public static ObjectInspector getStandardWritableObjectInspectorFromTypeInfo(
            TypeInfo typeInfo)
        {
            ObjectInspector result = cachedStandardObjectInspector.get(typeInfo);
            if (result == null)
            {
                switch (typeInfo.getCategory())
                {
                    case ObjectInspectorCategory.PRIMITIVE:
                        {
                            result = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                                (PrimitiveTypeInfo)typeInfo);
                            break;
                        }
                    case ObjectInspectorCategory.LIST:
                        {
                            ObjectInspector elementObjectInspector =
                                getStandardWritableObjectInspectorFromTypeInfo(((ListTypeInfo)typeInfo)
                                .getListElementTypeInfo());
                            result = ObjectInspectorFactory
                                .getStandardListObjectInspector(elementObjectInspector);
                            break;
                        }
                    case ObjectInspectorCategory.MAP:
                        {
                            MapTypeInfo mapTypeInfo = (MapTypeInfo)typeInfo;
                            ObjectInspector keyObjectInspector =
                                getStandardWritableObjectInspectorFromTypeInfo(mapTypeInfo.getMapKeyTypeInfo());
                            ObjectInspector valueObjectInspector =
                                getStandardWritableObjectInspectorFromTypeInfo(mapTypeInfo.getMapValueTypeInfo());
                            result = ObjectInspectorFactory.getStandardMapObjectInspector(
                                keyObjectInspector, valueObjectInspector);
                            break;
                        }
                    case ObjectInspectorCategory.STRUCT:
                        {
                            StructTypeInfo structTypeInfo = (StructTypeInfo)typeInfo;
                            List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
                            List<TypeInfo> fieldTypeInfos = structTypeInfo
                                .getAllStructFieldTypeInfos();
                            List<ObjectInspector> fieldObjectInspectors = new List<ObjectInspector>(
                                fieldTypeInfos.Count);
                            for (int i = 0; i < fieldTypeInfos.Count; i++)
                            {
                                fieldObjectInspectors
                                    .Add(getStandardWritableObjectInspectorFromTypeInfo(fieldTypeInfos[i]));
                            }
                            result = ObjectInspectorFactory.getStandardStructObjectInspector(
                                fieldNames, fieldObjectInspectors);
                            break;
                        }
                    case ObjectInspectorCategory.UNION:
                        {
                            UnionTypeInfo unionTypeInfo = (UnionTypeInfo)typeInfo;
                            List<TypeInfo> objectTypeInfos = unionTypeInfo
                                .getAllUnionObjectTypeInfos();
                            List<ObjectInspector> fieldObjectInspectors =
                              new List<ObjectInspector>(objectTypeInfos.Count);
                            for (int i = 0; i < objectTypeInfos.Count; i++)
                            {
                                fieldObjectInspectors
                                    .Add(getStandardWritableObjectInspectorFromTypeInfo(objectTypeInfos[i]));
                            }
                            result = ObjectInspectorFactory.getStandardUnionObjectInspector(
                                fieldObjectInspectors);
                            break;
                        }

                    default:
                        {
                            result = null;
                            break;
                        }
                }
                ObjectInspector prev =
                  cachedStandardObjectInspector.putIfAbsent(typeInfo, result);
                if (prev != null)
                {
                    result = prev;
                }
            }
            return result;
        }

        static ConcurrentHashMap<TypeInfo, ObjectInspector> cachedStandardJavaObjectInspector =
            new ConcurrentHashMap<TypeInfo, ObjectInspector>();

        /**
         * Returns the standard object inspector that can be used to translate an
         * object of that typeInfo to a standard object type.
         */
        public static ObjectInspector getStandardJavaObjectInspectorFromTypeInfo(
            TypeInfo typeInfo)
        {
            ObjectInspector result = cachedStandardJavaObjectInspector.get(typeInfo);
            if (result == null)
            {
                switch (typeInfo.getCategory())
                {
                    case ObjectInspectorCategory.PRIMITIVE:
                        {
                            // NOTE: we use JavaPrimitiveObjectInspector instead of
                            // StandardPrimitiveObjectInspector
                            result = PrimitiveObjectInspectorFactory
                                .getPrimitiveJavaObjectInspector((PrimitiveTypeInfo)typeInfo);
                            break;
                        }
                    case ObjectInspectorCategory.LIST:
                        {
                            ObjectInspector elementObjectInspector =
                                getStandardJavaObjectInspectorFromTypeInfo(((ListTypeInfo)typeInfo)
                                .getListElementTypeInfo());
                            result = ObjectInspectorFactory
                                .getStandardListObjectInspector(elementObjectInspector);
                            break;
                        }
                    case ObjectInspectorCategory.MAP:
                        {
                            MapTypeInfo mapTypeInfo = (MapTypeInfo)typeInfo;
                            ObjectInspector keyObjectInspector = getStandardJavaObjectInspectorFromTypeInfo(mapTypeInfo
                                .getMapKeyTypeInfo());
                            ObjectInspector valueObjectInspector =
                                getStandardJavaObjectInspectorFromTypeInfo(mapTypeInfo.getMapValueTypeInfo());
                            result = ObjectInspectorFactory.getStandardMapObjectInspector(
                                keyObjectInspector, valueObjectInspector);
                            break;
                        }
                    case ObjectInspectorCategory.STRUCT:
                        {
                            StructTypeInfo strucTypeInfo = (StructTypeInfo)typeInfo;
                            List<String> fieldNames = strucTypeInfo.getAllStructFieldNames();
                            List<TypeInfo> fieldTypeInfos = strucTypeInfo
                                .getAllStructFieldTypeInfos();
                            List<ObjectInspector> fieldObjectInspectors = new List<ObjectInspector>(
                                fieldTypeInfos.Count);
                            for (int i = 0; i < fieldTypeInfos.Count; i++)
                            {
                                fieldObjectInspectors
                                    .Add(getStandardJavaObjectInspectorFromTypeInfo(fieldTypeInfos[i]));
                            }
                            result = ObjectInspectorFactory.getStandardStructObjectInspector(
                                fieldNames, fieldObjectInspectors);
                            break;
                        }
                    case ObjectInspectorCategory.UNION:
                        {
                            UnionTypeInfo unionTypeInfo = (UnionTypeInfo)typeInfo;
                            List<TypeInfo> objectTypeInfos = unionTypeInfo
                                .getAllUnionObjectTypeInfos();
                            List<ObjectInspector> fieldObjectInspectors =
                              new List<ObjectInspector>(objectTypeInfos.Count);
                            for (int i = 0; i < objectTypeInfos.Count; i++)
                            {
                                fieldObjectInspectors
                                    .Add(getStandardJavaObjectInspectorFromTypeInfo(objectTypeInfos[i]));
                            }
                            result = ObjectInspectorFactory.getStandardUnionObjectInspector(
                                fieldObjectInspectors);
                            break;
                        }
                    default:
                        {
                            result = null;
                            break;
                        }
                }
                ObjectInspector prev =
                  cachedStandardJavaObjectInspector.putIfAbsent(typeInfo, result);
                if (prev != null)
                {
                    result = prev;
                }
            }
            return result;
        }

        /**
         * Get the TypeInfo object from the ObjectInspector object by recursively
         * going into the ObjectInspector structure.
         */
        public static TypeInfo getTypeInfoFromObjectInspector(ObjectInspector oi)
        {
            // OPTIMIZATION for later.
            // if (oi instanceof TypeInfoBasedObjectInspector) {
            // TypeInfoBasedObjectInspector typeInfoBasedObjectInspector =
            // (ObjectInspector)oi;
            // return typeInfoBasedObjectInspector.getTypeInfo();
            // }
            if (oi == null)
            {
                return null;
            }

            // Recursively going into ObjectInspector structure
            TypeInfo result = null;
            switch (oi.getCategory())
            {
                case ObjectInspectorCategory.PRIMITIVE:
                    {
                        PrimitiveObjectInspector poi = (PrimitiveObjectInspector)oi;
                        result = poi.getTypeInfo();
                        break;
                    }
                case ObjectInspectorCategory.LIST:
                    {
                        ListObjectInspector loi = (ListObjectInspector)oi;
                        result = TypeInfoFactory
                            .getListTypeInfo(getTypeInfoFromObjectInspector(loi
                            .getListElementObjectInspector()));
                        break;
                    }
                case ObjectInspectorCategory.MAP:
                    {
                        MapObjectInspector moi = (MapObjectInspector)oi;
                        result = TypeInfoFactory.getMapTypeInfo(
                            getTypeInfoFromObjectInspector(moi.getMapKeyObjectInspector()),
                            getTypeInfoFromObjectInspector(moi.getMapValueObjectInspector()));
                        break;
                    }
                case ObjectInspectorCategory.STRUCT:
                    {
                        StructObjectInspector soi = (StructObjectInspector)oi;
                        IList<StructField> fields = soi.getAllStructFieldRefs();
                        List<String> fieldNames = new List<String>(fields.Count);
                        List<TypeInfo> fieldTypeInfos = new List<TypeInfo>(fields.Count);
                        foreach (StructField f in fields)
                        {
                            fieldNames.Add(f.getFieldName());
                            fieldTypeInfos.Add(getTypeInfoFromObjectInspector(f
                                .getFieldObjectInspector()));
                        }
                        result = TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypeInfos);
                        break;
                    }
                case ObjectInspectorCategory.UNION:
                    {
                        UnionObjectInspector uoi = (UnionObjectInspector)oi;
                        List<TypeInfo> objectTypeInfos = new List<TypeInfo>();
                        foreach (ObjectInspector eoi in uoi.getObjectInspectors())
                        {
                            objectTypeInfos.Add(getTypeInfoFromObjectInspector(eoi));
                        }
                        result = TypeInfoFactory.getUnionTypeInfo(objectTypeInfos);
                        break;
                    }
                default:
                    {
                        throw new InvalidOperationException("Unknown ObjectInspector category!");
                    }
            }
            return result;
        }

        public static List<TypeInfo> typeInfosFromStructObjectInspector(
            StructObjectInspector structObjectInspector)
        {

            IList<StructField> fields = structObjectInspector.getAllStructFieldRefs();
            List<TypeInfo> typeInfoList = new List<TypeInfo>(fields.Count);

            foreach (StructField field in fields)
            {
                TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(
                    field.getFieldObjectInspector().getTypeName());
                typeInfoList.Add(typeInfo);
            }
            return typeInfoList;
        }

        public static List<TypeInfo> typeInfosFromTypeNames(List<String> typeNames)
        {

            List<TypeInfo> result = new List<TypeInfo>(typeNames.Count);

            for (int i = 0; i < typeNames.Count; i++)
            {
                TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeNames[i]);
                result.Add(typeInfo);
            }
            return result;
        }

        public static List<TypeInfo> getTypeInfosFromTypeString(string typeString)
        {
            TypeInfoParser parser = new TypeInfoParser(typeString);
            return parser.parseTypeInfos();
        }

        public static string getTypesString(List<TypeInfo> typeInfos)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < typeInfos.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(":");
                }
                sb.Append(typeInfos[i].getTypeName());
            }
            return sb.ToString();
        }

        public static TypeInfo getTypeInfoFromTypeString(string typeString)
        {
            TypeInfoParser parser = new TypeInfoParser(typeString);
            return parser.parseTypeInfos()[0];
        }

        /**
         * Given two types, determine whether conversion needs to occur to compare the two types.
         * This is needed for cases like varchar, where the TypeInfo for varchar(10) != varchar(5),
         * but there would be no need to have to convert to compare these values.
         * @param typeA
         * @param typeB
         * @return
         */
        public static bool isConversionRequiredForComparison(TypeInfo typeA, TypeInfo typeB)
        {
            if (typeA.Equals(typeB))
            {
                return false;
            }

            if (TypeInfoUtils.doPrimitiveCategoriesMatch(typeA, typeB))
            {
                return false;
            }

            return true;
        }

        public static void registerNumericType(PrimitiveCategory primitiveCategory, int level)
        {
            numericTypeList.Add(primitiveCategory);
            numericTypes.AddOrUpdate(primitiveCategory, v => level, (v, x) => x);
        }

        public static bool implicitConvertible(PrimitiveCategory from, PrimitiveCategory to)
        {
            if (from == to)
            {
                return true;
            }

            PrimitiveGrouping fromPg = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(from);
            PrimitiveGrouping toPg = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(to);

            // Allow implicit String to Double conversion
            if (fromPg == PrimitiveGrouping.STRING_GROUP && to == PrimitiveCategory.DOUBLE)
            {
                return true;
            }
            // Allow implicit String to Decimal conversion
            if (fromPg == PrimitiveGrouping.STRING_GROUP && to == PrimitiveCategory.DECIMAL)
            {
                return true;
            }
            // Void can be converted to any type
            if (from == PrimitiveCategory.VOID)
            {
                return true;
            }

            // Allow implicit String to Date conversion
            if (fromPg == PrimitiveGrouping.DATE_GROUP && toPg == PrimitiveGrouping.STRING_GROUP)
            {
                return true;
            }
            // Allow implicit Numeric to String conversion
            if (fromPg == PrimitiveGrouping.NUMERIC_GROUP && toPg == PrimitiveGrouping.STRING_GROUP)
            {
                return true;
            }
            // Allow implicit String to varchar conversion, and vice versa
            if (fromPg == PrimitiveGrouping.STRING_GROUP && toPg == PrimitiveGrouping.STRING_GROUP)
            {
                return true;
            }

            // Allow implicit conversion from Byte -> Integer -> Long -> Float -> Double
            // Decimal -> String
            int? f = null;
            int? t = null;
            int tmp;
            if (numericTypes.TryGetValue(from, out tmp))
            {
                f = tmp;
            }
            if (numericTypes.TryGetValue(to, out tmp))
            {
                t = tmp;
            }

            if (f == null || t == null)
            {
                return false;
            }
            if (f.Value > t.Value)
            {
                return false;
            }
            return true;
        }

        /**
         * Returns whether it is possible to implicitly convert an object of Class
         * from to Class to.
         */
        public static bool implicitConvertible(TypeInfo from, TypeInfo to)
        {
            if (from.Equals(to))
            {
                return true;
            }

            // Reimplemented to use PrimitiveCategory rather than TypeInfo, because
            // 2 TypeInfos from the same qualified type (varchar, decimal) should still be
            // seen as equivalent.
            if (from.getCategory() == ObjectInspectorCategory.PRIMITIVE
                && to.getCategory() == ObjectInspectorCategory.PRIMITIVE)
            {
                return implicitConvertible(
                    ((PrimitiveTypeInfo)from).getPrimitiveCategory(),
                    ((PrimitiveTypeInfo)to).getPrimitiveCategory());
            }
            return false;
        }

        class ConcurrentHashMap<K, V> : ConcurrentDictionary<K, V>
        {
            internal ObjectInspector putIfAbsent(TypeInfo typeInfo, ObjectInspector result)
            {
                throw new NotImplementedException();
            }
        }
    }
}
