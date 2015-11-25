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
    using System.Collections.Generic;
    using org.apache.hadoop.hive.ql.io.orc.external;

    public class OrcUtils
    {
        /**
         * Returns selected columns as a bool array with true value set for specified column names.
         * The result will contain number of elements equal to flattened number of columns.
         * For example:
         * selectedColumns - a,b,c
         * allColumns - a,b,c,d
         * If column c is a complex type, say list<string> and other types are primitives then result will
         * be [false, true, true, true, true, true, false]
         * Index 0 is the root element of the struct which is set to false by default, index 1,2
         * corresponds to columns a and b. Index 3,4 correspond to column c which is list<string> and
         * index 5 correspond to column d. After flattening list<string> gets 2 columns.
         *
         * @param selectedColumns - comma separated list of selected column names
         * @param schema       - object schema
         * @return - bool array with true value set for the specified column names
         */
        public static bool[] includeColumns(string selectedColumns, TypeDescription schema)
        {
            int numFlattenedCols = schema.getMaximumId();
            bool[] results = new bool[numFlattenedCols + 1];
            if ("*".Equals(selectedColumns))
            {
                for (int i = 0; i < results.Length; i++)
                {
                    results[i] = true;
                }
                return results;
            }
            if (selectedColumns != null &&
                schema.getCategory() == Category.STRUCT)
            {
                IList<string> fieldNames = schema.getFieldNames();
                IList<TypeDescription> fields = schema.getChildren();
                foreach (string column in selectedColumns.Split((',')))
                {
                    TypeDescription col = findColumn(column, fieldNames, fields);
                    if (col != null)
                    {
                        for (int i = col.getId(); i <= col.getMaximumId(); ++i)
                        {
                            results[i] = true;
                        }
                    }
                }
            }
            return results;
        }

        private static TypeDescription findColumn(
            string columnName,
            IList<string> fieldNames,
            IList<TypeDescription> fields)
        {
            int i = 0;
            foreach (string fieldName in fieldNames)
            {
                if (string.Equals(fieldName, columnName, StringComparison.OrdinalIgnoreCase))
                {
                    return fields[i];
                }
                else
                {
                    i += 1;
                }
            }
            return null;
        }

        public static TypeDescription convertTypeInfo(TypeInfo info)
        {
            switch (info.getCategory())
            {
                case ObjectInspectorCategory.PRIMITIVE:
                    {
                        PrimitiveTypeInfo pinfo = (PrimitiveTypeInfo)info;
                        switch (pinfo.getPrimitiveCategory())
                        {
                            case PrimitiveCategory.BOOLEAN:
                                return TypeDescription.createBoolean();
                            case PrimitiveCategory.BYTE:
                                return TypeDescription.createByte();
                            case PrimitiveCategory.SHORT:
                                return TypeDescription.createShort();
                            case PrimitiveCategory.INT:
                                return TypeDescription.createInt();
                            case PrimitiveCategory.LONG:
                                return TypeDescription.createLong();
                            case PrimitiveCategory.FLOAT:
                                return TypeDescription.createFloat();
                            case PrimitiveCategory.DOUBLE:
                                return TypeDescription.createDouble();
                            case PrimitiveCategory.STRING:
                                return TypeDescription.createString();
                            case PrimitiveCategory.DATE:
                                return TypeDescription.createDate();
                            case PrimitiveCategory.TIMESTAMP:
                                return TypeDescription.createTimestamp();
                            case PrimitiveCategory.BINARY:
                                return TypeDescription.createBinary();
                            case PrimitiveCategory.DECIMAL:
                                {
                                    DecimalTypeInfo dinfo = (DecimalTypeInfo)pinfo;
                                    return TypeDescription.createDecimal()
                                        .withScale(dinfo.scale())
                                        .withPrecision(dinfo.precision());
                                }
                            case PrimitiveCategory.VARCHAR:
                                {
                                    BaseCharTypeInfo cinfo = (BaseCharTypeInfo)pinfo;
                                    return TypeDescription.createVarchar()
                                        .withMaxLength(cinfo.getLength());
                                }
                            case PrimitiveCategory.CHAR:
                                {
                                    BaseCharTypeInfo cinfo = (BaseCharTypeInfo)pinfo;
                                    return TypeDescription.createChar()
                                        .withMaxLength(cinfo.getLength());
                                }
                            default:
                                throw new ArgumentException("ORC doesn't handle primitive" +
                                    " category " + pinfo.getPrimitiveCategory());
                        }
                    }
                case ObjectInspectorCategory.LIST:
                    {
                        ListTypeInfo linfo = (ListTypeInfo)info;
                        return TypeDescription.createList
                            (convertTypeInfo(linfo.getListElementTypeInfo()));
                    }
                case ObjectInspectorCategory.MAP:
                    {
                        MapTypeInfo minfo = (MapTypeInfo)info;
                        return TypeDescription.createMap
                            (convertTypeInfo(minfo.getMapKeyTypeInfo()),
                                convertTypeInfo(minfo.getMapValueTypeInfo()));
                    }
                case ObjectInspectorCategory.UNION:
                    {
                        UnionTypeInfo minfo = (UnionTypeInfo)info;
                        TypeDescription result = TypeDescription.createUnion();
                        foreach (TypeInfo child in minfo.getAllUnionObjectTypeInfos())
                        {
                            result.addUnionChild(convertTypeInfo(child));
                        }
                        return result;
                    }
                case ObjectInspectorCategory.STRUCT:
                    {
                        StructTypeInfo sinfo = (StructTypeInfo)info;
                        TypeDescription result = TypeDescription.createStruct();
                        foreach (String fieldName in sinfo.getAllStructFieldNames())
                        {
                            result.addField(fieldName,
                                convertTypeInfo(sinfo.getStructFieldTypeInfo(fieldName)));
                        }
                        return result;
                    }
                default:
                    throw new ArgumentException("ORC doesn't handle " +
                        info.getCategory());
            }
        }
    }
}
