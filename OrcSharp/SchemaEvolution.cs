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
    using System.IO;
    using OrcSharp.External;
    using OrcProto = global::orc.proto;

    /**
     * Take the file types and the (optional) configuration column names/types and see if there
     * has been schema evolution.
     */
    public class SchemaEvolution
    {
        private static readonly Logger LOG = LoggerFactory.getLog(typeof(SchemaEvolution));

        public static TreeReaderFactory.TreeReaderSchema validateAndCreate(
            IList<OrcProto.Type> fileTypes,
            IList<OrcProto.Type> schemaTypes)
        {

            // For ACID, the row is the ROW field in the outer STRUCT.
            bool isAcid = checkAcidSchema(fileTypes);
            IList<OrcProto.Type> rowSchema;
            int rowSubtype;
            if (isAcid)
            {
                rowSubtype = OrcRecordUpdater.ROW + 1;
                rowSchema = fileTypes.subList(rowSubtype, fileTypes.Count);
            }
            else
            {
                rowSubtype = 0;
                rowSchema = fileTypes;
            }

            // Do checking on the overlap.  Additional columns will be defaulted to NULL.

            int numFileColumns = rowSchema[0].SubtypesCount;
            int numDesiredColumns = schemaTypes[0].SubtypesCount;

            int numReadColumns = Math.Min(numFileColumns, numDesiredColumns);

            /**
             * Check type promotion.
             *
             * Currently, we only support integer type promotions that can be done "implicitly".
             * That is, we know that using a bigger integer tree reader on the original smaller integer
             * column will "just work".
             *
             * In the future, other type promotions might require type conversion.
             */
            // short -> int -> bigint as same integer readers are used for the above types.

            for (int i = 0; i < numReadColumns; i++)
            {
                OrcProto.Type fColType = fileTypes[rowSubtype + i];
                OrcProto.Type rColType = schemaTypes[i];
                if (fColType.Kind != rColType.Kind)
                {

                    bool ok = false;
                    if (fColType.Kind == OrcProto.Type.Types.Kind.SHORT)
                    {

                        if (rColType.Kind == OrcProto.Type.Types.Kind.INT ||
                            rColType.Kind == OrcProto.Type.Types.Kind.LONG)
                        {
                            // type promotion possible, converting SHORT to INT/LONG requested type
                            ok = true;
                        }
                    }
                    else if (fColType.Kind == OrcProto.Type.Types.Kind.INT)
                    {

                        if (rColType.Kind == OrcProto.Type.Types.Kind.LONG)
                        {
                            // type promotion possible, converting INT to LONG requested type
                            ok = true;
                        }
                    }

                    if (!ok)
                    {
                        throw new IOException("ORC does not support type conversion from " +
                            fColType.Kind.ToString() + " to " + rColType.Kind.ToString());
                    }
                }
            }

            IList<OrcProto.Type> fullSchemaTypes;

            if (isAcid)
            {
                fullSchemaTypes = new List<OrcProto.Type>();

                // This copies the ACID struct type which is subtype = 0.
                // It has field names "operation" through "row".
                // And we copy the types for all fields EXCEPT ROW (which must be last!).

                for (int i = 0; i < rowSubtype; i++)
                {
                    fullSchemaTypes.Add(fileTypes[i].ToBuilder().Build());
                }

                // Add the row struct type.
                OrcUtils.appendOrcTypesRebuildSubtypes(fullSchemaTypes, schemaTypes, 0);
            }
            else
            {
                fullSchemaTypes = schemaTypes;
            }

            int innerStructSubtype = rowSubtype;

            // LOG.info("Schema evolution: (fileTypes) " + fileTypes.toString() +
            //     " (schemaEvolutionTypes) " + schemaEvolutionTypes.toString());

            return new TreeReaderFactory.TreeReaderSchema().
                fileTypes(fileTypes).
                schemaTypes(fullSchemaTypes).
                innerStructSubtype(innerStructSubtype);
        }

        private static bool checkAcidSchema(IList<OrcProto.Type> fileSchema)
        {
            if (fileSchema[0].Kind == OrcProto.Type.Types.Kind.STRUCT)
            {
                IList<string> rootFields = fileSchema[0].FieldNamesList;
                if (acidEventFieldNames.Equals(rootFields))
                {
                    return true;
                }
            }
            return false;
        }

        /**
         * @param typeDescr
         * @return ORC types for the ACID event based on the row's type description
         */
        public static List<OrcProto.Type> createEventSchema(TypeDescription typeDescr)
        {

            List<OrcProto.Type> result = new List<OrcProto.Type>();

            OrcProto.Type.Builder type = OrcProto.Type.CreateBuilder();
            type.SetKind(OrcProto.Type.Types.Kind.STRUCT);
            type.AddRangeFieldNames(acidEventFieldNames);
            for (int i = 0; i < acidEventFieldNames.Length; i++)
            {
                type.AddSubtypes((uint)i + 1);
            }
            result.Add(type.Build());

            // Automatically add all fields except the last (ROW).
            for (int i = 0; i < acidEventOrcTypeKinds.Length - 1; i++)
            {
                type.Clear();
                type.SetKind(acidEventOrcTypeKinds[i]);
                result.Add(type.Build());
            }

            OrcUtils.appendOrcTypesRebuildSubtypes(result, typeDescr);
            return result;
        }

        public static string[] acidEventFieldNames = new string[]
        {
            "operation",
            "originalTransaction",
            "bucket",
            "rowId",
            "currentTransaction",
            "row",
        };

        public static OrcProto.Type.Types.Kind[] acidEventOrcTypeKinds = new OrcProto.Type.Types.Kind[]
        {
            OrcProto.Type.Types.Kind.INT,
            OrcProto.Type.Types.Kind.LONG,
            OrcProto.Type.Types.Kind.INT,
            OrcProto.Type.Types.Kind.LONG,
            OrcProto.Type.Types.Kind.LONG,
            OrcProto.Type.Types.Kind.STRUCT,
        };
    }
}
