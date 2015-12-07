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
    using OrcSharp.Serialization;
    using OrcProto = global::orc.proto;

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

        /**
         * Convert a Hive type property string that contains separated type names into a list of
         * TypeDescription objects.
         * @return the list of TypeDescription objects.
         */
        public static List<TypeDescription> typeDescriptionsFromHiveTypeProperty(string hiveTypeProperty)
        {
            // CONSDIER: We need a type name parser for TypeDescription.

            List<TypeInfo> typeInfoList = TypeInfoUtils.getTypeInfosFromTypeString(hiveTypeProperty);
            List<TypeDescription> typeDescrList = new List<TypeDescription>(typeInfoList.Count);
            foreach (TypeInfo typeInfo in typeInfoList)
            {
                typeDescrList.Add(convertTypeInfo(typeInfo));
            }
            return typeDescrList;
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
                        foreach (string fieldName in sinfo.getAllStructFieldNames())
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

        public static List<OrcProto.Type> getOrcTypes(TypeDescription typeDescr)
        {
            List<OrcProto.Type> result = new List<OrcProto.Type>();
            appendOrcTypes(result, typeDescr);
            return result;
        }

        private static void appendOrcTypes(List<OrcProto.Type> result, TypeDescription typeDescr)
        {
            OrcProto.Type.Builder type = OrcProto.Type.CreateBuilder();
            IList<TypeDescription> children = typeDescr.getChildren();
            switch (typeDescr.getCategory())
            {
                case Category.BOOLEAN:
                    type.SetKind(OrcProto.Type.Types.Kind.BOOLEAN);
                    break;
                case Category.BYTE:
                    type.SetKind(OrcProto.Type.Types.Kind.BYTE);
                    break;
                case Category.SHORT:
                    type.SetKind(OrcProto.Type.Types.Kind.SHORT);
                    break;
                case Category.INT:
                    type.SetKind(OrcProto.Type.Types.Kind.INT);
                    break;
                case Category.LONG:
                    type.SetKind(OrcProto.Type.Types.Kind.LONG);
                    break;
                case Category.FLOAT:
                    type.SetKind(OrcProto.Type.Types.Kind.FLOAT);
                    break;
                case Category.DOUBLE:
                    type.SetKind(OrcProto.Type.Types.Kind.DOUBLE);
                    break;
                case Category.STRING:
                    type.SetKind(OrcProto.Type.Types.Kind.STRING);
                    break;
                case Category.CHAR:
                    type.SetKind(OrcProto.Type.Types.Kind.CHAR);
                    type.SetMaximumLength((uint)typeDescr.getMaxLength());
                    break;
                case Category.VARCHAR:
                    type.SetKind(OrcProto.Type.Types.Kind.VARCHAR);
                    type.SetMaximumLength((uint)typeDescr.getMaxLength());
                    break;
                case Category.BINARY:
                    type.SetKind(OrcProto.Type.Types.Kind.BINARY);
                    break;
                case Category.TIMESTAMP:
                    type.SetKind(OrcProto.Type.Types.Kind.TIMESTAMP);
                    break;
                case Category.DATE:
                    type.SetKind(OrcProto.Type.Types.Kind.DATE);
                    break;
                case Category.DECIMAL:
                    type.SetKind(OrcProto.Type.Types.Kind.DECIMAL);
                    type.SetPrecision((uint)typeDescr.getPrecision());
                    type.SetScale((uint)typeDescr.getScale());
                    break;
                case Category.LIST:
                    type.SetKind(OrcProto.Type.Types.Kind.LIST);
                    type.AddSubtypes((uint)children[0].getId());
                    break;
                case Category.MAP:
                    type.SetKind(OrcProto.Type.Types.Kind.MAP);
                    foreach (TypeDescription t in children)
                    {
                        type.AddSubtypes((uint)t.getId());
                    }
                    break;
                case Category.STRUCT:
                    type.SetKind(OrcProto.Type.Types.Kind.STRUCT);
                    foreach (TypeDescription t in children)
                    {
                        type.AddSubtypes((uint)t.getId());
                    }
                    foreach (string field in typeDescr.getFieldNames())
                    {
                        type.AddFieldNames(field);
                    }
                    break;
                case Category.UNION:
                    type.SetKind(OrcProto.Type.Types.Kind.UNION);
                    foreach (TypeDescription t in children)
                    {
                        type.AddSubtypes((uint)t.getId());
                    }
                    break;
                default:
                    throw new ArgumentException("Unknown category: " + typeDescr.getCategory());
            }
            result.Add(type.Build());
            if (children != null)
            {
                foreach (TypeDescription child in children)
                {
                    appendOrcTypes(result, child);
                }
            }
        }

        /**
         * NOTE: This method ignores the subtype numbers in the TypeDescription rebuilds the subtype
         * numbers based on the length of the result list being appended.
         *
         * @param result
         * @param typeInfo
         */
        public static void appendOrcTypesRebuildSubtypes(
            IList<OrcProto.Type> result,
            TypeDescription typeDescr)
        {
            int subtype = result.Count;
            OrcProto.Type.Builder type = OrcProto.Type.CreateBuilder();
            bool needsAdd = true;
            IList<TypeDescription> children = typeDescr.getChildren();
            switch (typeDescr.getCategory())
            {
                case Category.BOOLEAN:
                    type.SetKind(OrcProto.Type.Types.Kind.BOOLEAN);
                    break;
                case Category.BYTE:
                    type.SetKind(OrcProto.Type.Types.Kind.BYTE);
                    break;
                case Category.SHORT:
                    type.SetKind(OrcProto.Type.Types.Kind.SHORT);
                    break;
                case Category.INT:
                    type.SetKind(OrcProto.Type.Types.Kind.INT);
                    break;
                case Category.LONG:
                    type.SetKind(OrcProto.Type.Types.Kind.LONG);
                    break;
                case Category.FLOAT:
                    type.SetKind(OrcProto.Type.Types.Kind.FLOAT);
                    break;
                case Category.DOUBLE:
                    type.SetKind(OrcProto.Type.Types.Kind.DOUBLE);
                    break;
                case Category.STRING:
                    type.SetKind(OrcProto.Type.Types.Kind.STRING);
                    break;
                case Category.CHAR:
                    type.SetKind(OrcProto.Type.Types.Kind.CHAR);
                    type.SetMaximumLength((uint)typeDescr.getMaxLength());
                    break;
                case Category.VARCHAR:
                    type.SetKind(OrcProto.Type.Types.Kind.VARCHAR);
                    type.SetMaximumLength((uint)typeDescr.getMaxLength());
                    break;
                case Category.BINARY:
                    type.SetKind(OrcProto.Type.Types.Kind.BINARY);
                    break;
                case Category.TIMESTAMP:
                    type.SetKind(OrcProto.Type.Types.Kind.TIMESTAMP);
                    break;
                case Category.DATE:
                    type.SetKind(OrcProto.Type.Types.Kind.DATE);
                    break;
                case Category.DECIMAL:
                    type.SetKind(OrcProto.Type.Types.Kind.DECIMAL);
                    type.SetPrecision((uint)typeDescr.getPrecision());
                    type.SetScale((uint)typeDescr.getScale());
                    break;
                case Category.LIST:
                    type.SetKind(OrcProto.Type.Types.Kind.LIST);
                    type.AddSubtypes((uint)++subtype);
                    result.Add(type.Build());
                    needsAdd = false;
                    appendOrcTypesRebuildSubtypes(result, children[0]);
                    break;
                case Category.MAP:
                    {
                        // Make room for MAP type.
                        result.Add(null);

                        // Add MAP type pair in order to determine their subtype values.
                        appendOrcTypesRebuildSubtypes(result, children[0]);
                        int subtype2 = result.Count;
                        appendOrcTypesRebuildSubtypes(result, children[1]);
                        type.SetKind(OrcProto.Type.Types.Kind.MAP);
                        type.AddSubtypes((uint)subtype + 1);
                        type.AddSubtypes((uint)subtype2);
                        result[subtype] = type.Build();
                        needsAdd = false;
                    }
                    break;
                case Category.STRUCT:
                    {
                        IList<String> fieldNames = typeDescr.getFieldNames();

                        // Make room for STRUCT type.
                        result.Add(null);

                        List<int> fieldSubtypes = new List<int>(fieldNames.Count);
                        foreach (TypeDescription child in children)
                        {
                            int fieldSubtype = result.Count;
                            fieldSubtypes.Add(fieldSubtype);
                            appendOrcTypesRebuildSubtypes(result, child);
                        }

                        type.SetKind(OrcProto.Type.Types.Kind.STRUCT);

                        for (int i = 0; i < fieldNames.Count; i++)
                        {
                            type.AddSubtypes((uint)fieldSubtypes[i]);
                            type.AddFieldNames(fieldNames[i]);
                        }
                        result[subtype] = type.Build();
                        needsAdd = false;
                    }
                    break;
                case Category.UNION:
                    {
                        // Make room for UNION type.
                        result.Add(null);

                        List<int> unionSubtypes = new List<int>(children.Count);
                        foreach (TypeDescription child in children)
                        {
                            int unionSubtype = result.Count;
                            unionSubtypes.Add(unionSubtype);
                            appendOrcTypesRebuildSubtypes(result, child);
                        }

                        type.SetKind(OrcProto.Type.Types.Kind.UNION);
                        for (int i = 0; i < children.Count; i++)
                        {
                            type.AddSubtypes((uint)unionSubtypes[i]);
                        }
                        result[subtype] = type.Build();
                        needsAdd = false;
                    }
                    break;
                default:
                    throw new ArgumentException("Unknown category: " + typeDescr.getCategory());
            }
            if (needsAdd)
            {
                result.Add(type.Build());
            }
        }

        /**
         * NOTE: This method ignores the subtype numbers in the OrcProto.Type rebuilds the subtype
         * numbers based on the length of the result list being appended.
         *
         * @param result
         * @param typeInfo
         */
        public static int appendOrcTypesRebuildSubtypes(
            IList<OrcProto.Type> result,
            IList<OrcProto.Type> types,
            int columnId)
        {
            OrcProto.Type oldType = types[columnId++];

            int subtype = result.Count;
            OrcProto.Type.Builder builder = OrcProto.Type.CreateBuilder();
            bool needsAdd = true;
            switch (oldType.Kind)
            {
                case OrcProto.Type.Types.Kind.BOOLEAN:
                    builder.SetKind(OrcProto.Type.Types.Kind.BOOLEAN);
                    break;
                case OrcProto.Type.Types.Kind.BYTE:
                    builder.SetKind(OrcProto.Type.Types.Kind.BYTE);
                    break;
                case OrcProto.Type.Types.Kind.SHORT:
                    builder.SetKind(OrcProto.Type.Types.Kind.SHORT);
                    break;
                case OrcProto.Type.Types.Kind.INT:
                    builder.SetKind(OrcProto.Type.Types.Kind.INT);
                    break;
                case OrcProto.Type.Types.Kind.LONG:
                    builder.SetKind(OrcProto.Type.Types.Kind.LONG);
                    break;
                case OrcProto.Type.Types.Kind.FLOAT:
                    builder.SetKind(OrcProto.Type.Types.Kind.FLOAT);
                    break;
                case OrcProto.Type.Types.Kind.DOUBLE:
                    builder.SetKind(OrcProto.Type.Types.Kind.DOUBLE);
                    break;
                case OrcProto.Type.Types.Kind.STRING:
                    builder.SetKind(OrcProto.Type.Types.Kind.STRING);
                    break;
                case OrcProto.Type.Types.Kind.CHAR:
                    builder.SetKind(OrcProto.Type.Types.Kind.CHAR);
                    builder.SetMaximumLength(oldType.MaximumLength);
                    break;
                case OrcProto.Type.Types.Kind.VARCHAR:
                    builder.SetKind(OrcProto.Type.Types.Kind.VARCHAR);
                    builder.SetMaximumLength(oldType.MaximumLength);
                    break;
                case OrcProto.Type.Types.Kind.BINARY:
                    builder.SetKind(OrcProto.Type.Types.Kind.BINARY);
                    break;
                case OrcProto.Type.Types.Kind.TIMESTAMP:
                    builder.SetKind(OrcProto.Type.Types.Kind.TIMESTAMP);
                    break;
                case OrcProto.Type.Types.Kind.DATE:
                    builder.SetKind(OrcProto.Type.Types.Kind.DATE);
                    break;
                case OrcProto.Type.Types.Kind.DECIMAL:
                    builder.SetKind(OrcProto.Type.Types.Kind.DECIMAL);
                    builder.SetPrecision(oldType.Precision);
                    builder.SetScale(oldType.Scale);
                    break;
                case OrcProto.Type.Types.Kind.LIST:
                    builder.SetKind(OrcProto.Type.Types.Kind.LIST);
                    builder.AddSubtypes((uint)++subtype);
                    result.Add(builder.Build());
                    needsAdd = false;
                    columnId = appendOrcTypesRebuildSubtypes(result, types, columnId);
                    break;
                case OrcProto.Type.Types.Kind.MAP:
                    {
                        // Make room for MAP type.
                        result.Add(null);

                        // Add MAP type pair in order to determine their subtype values.
                        columnId = appendOrcTypesRebuildSubtypes(result, types, columnId);
                        int subtype2 = result.Count;
                        columnId = appendOrcTypesRebuildSubtypes(result, types, columnId);
                        builder.SetKind(OrcProto.Type.Types.Kind.MAP);
                        builder.AddSubtypes((uint)subtype + 1);
                        builder.AddSubtypes((uint)subtype2);
                        result[subtype] = builder.Build();
                        needsAdd = false;
                    }
                    break;
                case OrcProto.Type.Types.Kind.STRUCT:
                    {
                        IList<string> fieldNames = oldType.FieldNamesList;

                        // Make room for STRUCT type.
                        result.Add(null);

                        List<int> fieldSubtypes = new List<int>(fieldNames.Count);
                        for (int i = 0; i < fieldNames.Count; i++)
                        {
                            int fieldSubtype = result.Count;
                            fieldSubtypes.Add(fieldSubtype);
                            columnId = appendOrcTypesRebuildSubtypes(result, types, columnId);
                        }

                        builder.SetKind(OrcProto.Type.Types.Kind.STRUCT);

                        for (int i = 0; i < fieldNames.Count; i++)
                        {
                            builder.AddSubtypes((uint)fieldSubtypes[i]);
                            builder.AddFieldNames(fieldNames[i]);
                        }
                        result[subtype] = builder.Build();
                        needsAdd = false;
                    }
                    break;
                case OrcProto.Type.Types.Kind.UNION:
                    {
                        int subtypeCount = oldType.SubtypesCount;

                        // Make room for UNION type.
                        result.Add(null);

                        List<int> unionSubtypes = new List<int>(subtypeCount);
                        for (int i = 0; i < subtypeCount; i++)
                        {
                            int unionSubtype = result.Count;
                            unionSubtypes.Add(unionSubtype);
                            columnId = appendOrcTypesRebuildSubtypes(result, types, columnId);
                        }

                        builder.SetKind(OrcProto.Type.Types.Kind.UNION);
                        for (int i = 0; i < subtypeCount; i++)
                        {
                            builder.AddSubtypes((uint)unionSubtypes[i]);
                        }
                        result[subtype] = builder.Build();
                        needsAdd = false;
                    }
                    break;
                default:
                    throw new ArgumentException("Unknown category: " + oldType.Kind);
            }
            if (needsAdd)
            {
                result.Add(builder.Build());
            }
            return columnId;
        }

#if false
        public static TypeDescription getDesiredRowTypeDescr(Configuration conf)
        {
            string columnNameProperty = null;
            string columnTypeProperty = null;

            IList<string> schemaEvolutionColumnNames = null;
            List<TypeDescription> schemaEvolutionTypeDescrs = null;

            bool haveSchemaEvolutionProperties = false;
            if (HiveConf.getBoolVar(conf, ConfVars.HIVE_SCHEMA_EVOLUTION))
            {

                columnNameProperty = conf.get(IOConstants.SCHEMA_EVOLUTION_COLUMNS);
                columnTypeProperty = conf.get(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES);

                haveSchemaEvolutionProperties =
                    (columnNameProperty != null && columnTypeProperty != null);

                if (haveSchemaEvolutionProperties)
                {
                    schemaEvolutionColumnNames = columnNameProperty.Split(',');
                    if (schemaEvolutionColumnNames.Count == 0)
                    {
                        haveSchemaEvolutionProperties = false;
                    }
                    else
                    {
                        schemaEvolutionTypeDescrs =
                            OrcUtils.typeDescriptionsFromHiveTypeProperty(columnTypeProperty);
                        if (schemaEvolutionTypeDescrs.Count != schemaEvolutionColumnNames.Count)
                        {
                            haveSchemaEvolutionProperties = false;
                        }
                    }
                }
            }

            if (!haveSchemaEvolutionProperties)
            {

                // Try regular properties;
                columnNameProperty = conf.get(serdeConstants.LIST_COLUMNS);
                columnTypeProperty = conf.get(serdeConstants.LIST_COLUMN_TYPES);
                if (columnTypeProperty == null || columnNameProperty == null)
                {
                    return null;
                }

                schemaEvolutionColumnNames = columnNameProperty.Split(',');
                if (schemaEvolutionColumnNames.Count == 0)
                {
                    return null;
                }
                schemaEvolutionTypeDescrs =
                    OrcUtils.typeDescriptionsFromHiveTypeProperty(columnTypeProperty);
                if (schemaEvolutionTypeDescrs.Count != schemaEvolutionColumnNames.Count)
                {
                    return null;
                }
            }

            // Desired schema does not include virtual columns or partition columns.
            TypeDescription result = TypeDescription.createStruct();
            for (int i = 0; i < schemaEvolutionColumnNames.Count; i++)
            {
                result.addField(schemaEvolutionColumnNames[i], schemaEvolutionTypeDescrs[i]);
            }

            return result;
        }
#endif
    }
}
