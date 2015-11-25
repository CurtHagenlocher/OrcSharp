/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
    using System.Linq;
    using org.apache.hadoop.hive.ql.io.orc.external;
    using OrcProto = global::orc.proto;

    /**
     * Factory to create ORC tree readers. It also compares file schema with schema specified on read
     * to see if type promotions are possible.
     */
    public class RecordReaderFactory
    {
        static Log LOG = LogFactory.getLog(typeof(RecordReaderFactory));
        private static bool isLogInfoEnabled = LOG.isInfoEnabled();

        public static TreeReaderFactory.TreeReader createTreeReader(int colId,
            Configuration conf,
            List<OrcProto.Type> fileSchema,
            bool[] included,
            bool skipCorrupt)
        {
            bool isAcid = checkAcidSchema(fileSchema);
            List<OrcProto.Type> originalFileSchema;
            if (isAcid)
            {
                originalFileSchema = fileSchema.subList(fileSchema[0].SubtypesCount,
                    fileSchema.Count);
            }
            else
            {
                originalFileSchema = fileSchema;
            }
            int numCols = originalFileSchema[0].SubtypesCount;
            List<OrcProto.Type> schemaOnRead = getSchemaOnRead(numCols, conf);
            List<OrcProto.Type> schemaUsed = getMatchingSchema(fileSchema, schemaOnRead);
            if (schemaUsed == null)
            {
                return TreeReaderFactory.createTreeReader(colId, fileSchema, included, skipCorrupt);
            }
            else
            {
                return ConversionTreeReaderFactory.createTreeReader(colId, schemaUsed, included, skipCorrupt);
            }
        }

        static List<string> getAcidEventFields()
        {
            return new List<string> { "operation", "originalTransaction", "bucket",
        "rowId", "currentTransaction", "row" };
        }

        private static bool checkAcidSchema(List<OrcProto.Type> fileSchema)
        {
            if (fileSchema[0].Kind == OrcProto.Type.Types.Kind.STRUCT)
            {
                List<string> acidFields = getAcidEventFields();
                IList<string> rootFields = fileSchema[0].FieldNamesList;
                if (Lists.AreEqual(acidFields, rootFields))
                {
                    return true;
                }
            }
            return false;
        }

        private static List<OrcProto.Type> getMatchingSchema(List<OrcProto.Type> fileSchema,
            List<OrcProto.Type> schemaOnRead)
        {
            if (schemaOnRead == null)
            {
                if (isLogInfoEnabled)
                {
                    LOG.info("Schema is not specified on read. Using file schema.");
                }
                return null;
            }

            if (fileSchema.Count != schemaOnRead.Count)
            {
                if (isLogInfoEnabled)
                {
                    LOG.info("Schema on read column count does not match file schema's column count." +
                        " Falling back to using file schema.");
                }
                return null;
            }
            else
            {
                List<OrcProto.Type> result = new List<OrcProto.Type>(fileSchema);
                // check type promotion. ORC can only support type promotions for integer types
                // short -> int -> bigint as same integer readers are used for the above types.
                bool canPromoteType = false;
                for (int i = 0; i < fileSchema.Count; i++)
                {
                    OrcProto.Type fColType = fileSchema[i];
                    OrcProto.Type rColType = schemaOnRead[i];
                    if (fColType.Kind != rColType.Kind)
                    {
                        if (fColType.Kind == OrcProto.Type.Types.Kind.SHORT)
                        {

                            if (rColType.Kind == OrcProto.Type.Types.Kind.INT ||
                                rColType.Kind == OrcProto.Type.Types.Kind.LONG)
                            {
                                // type promotion possible, converting SHORT to INT/LONG requested type
                                var x = result[i].ToBuilder();
                                x.Kind = rColType.Kind;
                                result[i] = x.Build();
                                canPromoteType = true;
                            }
                            else
                            {
                                canPromoteType = false;
                            }

                        }
                        else if (fColType.Kind == OrcProto.Type.Types.Kind.INT)
                        {
                            if (rColType.Kind == OrcProto.Type.Types.Kind.LONG)
                            {
                                // type promotion possible, converting INT to LONG requested type
                                var x = result[i].ToBuilder();
                                x.Kind = rColType.Kind;
                                result[i] = x.Build();
                                canPromoteType = true;
                            }
                            else
                            {
                                canPromoteType = false;
                            }

                        }
                        else
                        {
                            canPromoteType = false;
                        }
                    }
                }

                if (canPromoteType)
                {
                    if (isLogInfoEnabled)
                    {
                        LOG.info("Integer type promotion happened in ORC record reader. Using promoted schema.");
                    }
                    return result;
                }
            }

            return null;
        }

        private static List<OrcProto.Type> getSchemaOnRead(int numCols, Configuration conf)
        {
            String columnTypeProperty = conf.get(serdeConstants.LIST_COLUMN_TYPES);
            String columnNameProperty = conf.get(serdeConstants.LIST_COLUMNS);
            if (columnTypeProperty == null || columnNameProperty == null)
            {
                return null;
            }

            List<string> columnNames = columnNameProperty.Split(',').ToList();
            List<TypeInfo> fieldTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
            // Column types from conf includes virtual and partition columns at the end. We consider only
            // the actual columns in the file.
            StructTypeInfo structTypeInfo = new StructTypeInfo(
                columnNames.subList(0, numCols),
                fieldTypes.subList(0, numCols));
            ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(structTypeInfo);
            return getOrcTypes(oi);
        }

        private static List<OrcProto.Type> getOrcTypes(ObjectInspector inspector)
        {
            List<OrcProto.Type> result = new List<OrcProto.Type>();
            getOrcTypesImpl(result, inspector);
            return result;
        }

        private static void getOrcTypesImpl(List<OrcProto.Type> result, ObjectInspector inspector)
        {
            OrcProto.Type.Builder type = OrcProto.Type.CreateBuilder();
            switch (inspector.getCategory())
            {
                case ObjectInspectorCategory.PRIMITIVE:
                    switch (((PrimitiveObjectInspector)inspector).getPrimitiveCategory())
                    {
                        case PrimitiveCategory.BOOLEAN:
                            type.Kind = OrcProto.Type.Types.Kind.BOOLEAN;
                            break;
                        case PrimitiveCategory.BYTE:
                            type.Kind = OrcProto.Type.Types.Kind.BYTE;
                            break;
                        case PrimitiveCategory.SHORT:
                            type.Kind = OrcProto.Type.Types.Kind.SHORT;
                            break;
                        case PrimitiveCategory.INT:
                            type.Kind = OrcProto.Type.Types.Kind.INT;
                            break;
                        case PrimitiveCategory.LONG:
                            type.Kind = OrcProto.Type.Types.Kind.LONG;
                            break;
                        case PrimitiveCategory.FLOAT:
                            type.Kind = OrcProto.Type.Types.Kind.FLOAT;
                            break;
                        case PrimitiveCategory.DOUBLE:
                            type.Kind = OrcProto.Type.Types.Kind.DOUBLE;
                            break;
                        case PrimitiveCategory.STRING:
                            type.Kind = OrcProto.Type.Types.Kind.STRING;
                            break;
                        case PrimitiveCategory.CHAR:
                            // The char length needs to be written to file and should be available
                            // from the object inspector
                            CharTypeInfo charTypeInfo = (CharTypeInfo)((PrimitiveObjectInspector)inspector)
                                .getTypeInfo();
                            type.Kind = OrcProto.Type.Types.Kind.CHAR;
                            type.MaximumLength = (uint)charTypeInfo.getLength();
                            break;
                        case PrimitiveCategory.VARCHAR:
                            // The varchar length needs to be written to file and should be available
                            // from the object inspector
                            VarcharTypeInfo typeInfo = (VarcharTypeInfo)((PrimitiveObjectInspector)inspector)
                                .getTypeInfo();
                            type.Kind = OrcProto.Type.Types.Kind.VARCHAR;
                            type.MaximumLength = (uint)typeInfo.getLength();
                            break;
                        case PrimitiveCategory.BINARY:
                            type.Kind = OrcProto.Type.Types.Kind.BINARY;
                            break;
                        case PrimitiveCategory.TIMESTAMP:
                            type.Kind = OrcProto.Type.Types.Kind.TIMESTAMP;
                            break;
                        case PrimitiveCategory.DATE:
                            type.Kind = OrcProto.Type.Types.Kind.DATE;
                            break;
                        case PrimitiveCategory.DECIMAL:
                            DecimalTypeInfo decTypeInfo = (DecimalTypeInfo)((PrimitiveObjectInspector)inspector)
                                .getTypeInfo();
                            type.Kind = OrcProto.Type.Types.Kind.DECIMAL;
                            type.Precision = (uint)decTypeInfo.precision();
                            type.Scale = (uint)decTypeInfo.scale();
                            break;
                        default:
                            throw new ArgumentException("Unknown primitive category: " +
                                ((PrimitiveObjectInspector)inspector).getPrimitiveCategory());
                    }
                    result.Add(type.Build());
                    break;
                case ObjectInspectorCategory.LIST:
                    type.Kind = OrcProto.Type.Types.Kind.LIST;
                    result.Add(type.Build());
                    getOrcTypesImpl(result, ((ListObjectInspector)inspector).getListElementObjectInspector());
                    break;
                case ObjectInspectorCategory.MAP:
                    type.Kind = OrcProto.Type.Types.Kind.MAP;
                    result.Add(type.Build());
                    getOrcTypesImpl(result, ((MapObjectInspector)inspector).getMapKeyObjectInspector());
                    getOrcTypesImpl(result, ((MapObjectInspector)inspector).getMapValueObjectInspector());
                    break;
                case ObjectInspectorCategory.STRUCT:
                    type.Kind = OrcProto.Type.Types.Kind.STRUCT;
                    result.Add(type.Build());
                    foreach (StructField field in ((StructObjectInspector)inspector).getAllStructFieldRefs())
                    {
                        getOrcTypesImpl(result, field.getFieldObjectInspector());
                    }
                    break;
                case ObjectInspectorCategory.UNION:
                    type.Kind = OrcProto.Type.Types.Kind.UNION;
                    result.Add(type.Build());
                    foreach (ObjectInspector oi in ((UnionObjectInspector)inspector).getObjectInspectors())
                    {
                        getOrcTypesImpl(result, oi);
                    }
                    break;
                default:
                    throw new ArgumentException("Unknown category: " + inspector.getCategory());
            }
        }
    }
}
