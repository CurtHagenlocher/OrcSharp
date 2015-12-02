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

namespace OrcSharp
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using OrcSharp.External;
    using OrcProto = global::orc.proto;

    /**
     * File dump tool with json formatted output.
     */
    public class JsonFileDump
    {
        public static void printJsonMetaData(List<string> files, Configuration conf,
            List<int> rowIndexCols, bool prettyPrint, bool printTimeZone)
        {
            JsonWriter writer = new JsonWriter();
            bool multiFile = files.Count > 1;
            if (multiFile)
            {
                writer.array();
            }
            else
            {
                writer.newObject();
            }
            foreach (string filename in files)
            {
                if (multiFile)
                {
                    writer.newObject();
                }
                writer.key("fileName").value(Path.GetFileName(filename));
                Reader reader = OrcFile.createReader(filename, OrcFile.readerOptions(conf));
                writer.key("fileVersion").value(OrcFile.VersionHelper.getName(reader.getFileVersion()));
                writer.key("writerVersion").value(reader.getWriterVersion().ToString());
                RecordReaderImpl rows = (RecordReaderImpl)reader.rows();
                writer.key("numberOfRows").value(reader.getNumberOfRows());
                writer.key("compression").value(reader.getCompression().ToString());
                if (reader.getCompression() != CompressionKind.NONE)
                {
                    writer.key("compressionBufferSize").value(reader.getCompressionSize());
                }
                writer.key("schemaString").value(reader.getObjectInspector().getTypeName());
                writer.key("schema").array();
                writeSchema(writer, reader.getTypes());
                writer.endArray();

                writer.key("stripeStatistics").array();
                List<StripeStatistics> stripeStatistics = reader.getStripeStatistics();
                for (int n = 0; n < stripeStatistics.Count; n++)
                {
                    writer.newObject();
                    writer.key("stripeNumber").value(n + 1);
                    StripeStatistics ss = stripeStatistics[n];
                    writer.key("columnStatistics").array();
                    for (int i = 0; i < ss.getColumnStatistics().Length; i++)
                    {
                        writer.newObject();
                        writer.key("columnId").value(i);
                        writeColumnStatistics(writer, ss.getColumnStatistics()[i]);
                        writer.endObject();
                    }
                    writer.endArray();
                    writer.endObject();
                }
                writer.endArray();

                ColumnStatistics[] stats = reader.getStatistics();
                int colCount = stats.Length;
                writer.key("fileStatistics").array();
                for (int i = 0; i < stats.Length; ++i)
                {
                    writer.newObject();
                    writer.key("columnId").value(i);
                    writeColumnStatistics(writer, stats[i]);
                    writer.endObject();
                }
                writer.endArray();

                writer.key("stripes").array();
                int stripeIx = -1;
                foreach (StripeInformation stripe in reader.getStripes())
                {
                    ++stripeIx;
                    long stripeStart = stripe.getOffset();
                    OrcProto.StripeFooter footer = rows.readStripeFooter(stripe);
                    writer.newObject(); // start of stripe information
                    writer.key("stripeNumber").value(stripeIx + 1);
                    writer.key("stripeInformation");
                    writeStripeInformation(writer, stripe);
                    if (printTimeZone)
                    {
                        writer.key("writerTimezone").value(
                            footer.HasWriterTimezone ? footer.WriterTimezone : FileDump.UNKNOWN);
                    }
                    long sectionStart = stripeStart;

                    writer.key("streams").array();
                    foreach (OrcProto.Stream section in footer.StreamsList)
                    {
                        writer.newObject();
                        string kind = section.HasKind ? section.Kind.ToString() : FileDump.UNKNOWN;
                        writer.key("columnId").value(section.Column);
                        writer.key("section").value(kind);
                        writer.key("startOffset").value(sectionStart);
                        writer.key("length").value(section.Length);
                        sectionStart += (long)section.Length;
                        writer.endObject();
                    }
                    writer.endArray();

                    writer.key("encodings").array();
                    for (int i = 0; i < footer.ColumnsCount; ++i)
                    {
                        writer.newObject();
                        OrcProto.ColumnEncoding encoding = footer.ColumnsList[i];
                        writer.key("columnId").value(i);
                        writer.key("kind").value(encoding.Kind.ToString());
                        if (encoding.Kind == OrcProto.ColumnEncoding.Types.Kind.DICTIONARY ||
                            encoding.Kind == OrcProto.ColumnEncoding.Types.Kind.DICTIONARY_V2)
                        {
                            writer.key("dictionarySize").value(encoding.DictionarySize);
                        }
                        writer.endObject();
                    }
                    writer.endArray();

                    if (rowIndexCols != null && rowIndexCols.Count != 0)
                    {
                        // include the columns that are specified, only if the columns are included, bloom filter
                        // will be read
                        bool[] sargColumns = new bool[colCount];
                        foreach (int colIdx in rowIndexCols)
                        {
                            sargColumns[colIdx] = true;
                        }
                        RecordReaderImpl.Index indices = rows.readRowIndex(stripeIx, null, sargColumns);
                        writer.key("indexes").array();
                        foreach (int col in rowIndexCols)
                        {
                            writer.newObject();
                            writer.key("columnId").value(col);
                            writeRowGroupIndexes(writer, col, indices.getRowGroupIndex());
                            writeBloomFilterIndexes(writer, col, indices.getBloomFilterIndex());
                            writer.endObject();
                        }
                        writer.endArray();
                    }
                    writer.endObject(); // end of stripe information
                }
                writer.endArray();

                long fileLen = new FileInfo(filename).Length;
                long paddedBytes = FileDump.getTotalPaddingSize(reader);
                // empty ORC file is ~45 bytes. Assumption here is file length always >0
                double percentPadding = ((double)paddedBytes / (double)fileLen) * 100;
                writer.key("fileLength").value(fileLen);
                writer.key("paddingLength").value(paddedBytes);
                writer.key("paddingRatio").value(percentPadding);
                rows.close();

                writer.endObject();
            }
            if (multiFile)
            {
                writer.endArray();
            }

            if (prettyPrint)
            {
#if false
                string prettyJson;
                if (multiFile)
                {
                    JSONArray jsonArray = new JSONArray(writer.toString());
                    prettyJson = jsonArray.toString(2);
                }
                else
                {
                    JSONObject jsonObject = new JSONObject(writer.toString());
                    prettyJson = jsonObject.toString(2);
                }
#else
                string prettyJson = writer.ToString();
#endif
                System.Console.WriteLine(prettyJson);
            }
            else
            {
                System.Console.WriteLine(writer.ToString());
            }
        }

        private static void writeSchema(JsonWriter writer, IList<OrcProto.Type> types)
        {
            int i = 0;
            foreach (OrcProto.Type type in types)
            {
                writer.newObject();
                writer.key("columnId").value(i++);
                writer.key("columnType").value(type.Kind.ToString());
                if (type.FieldNamesCount > 0)
                {
                    writer.key("childColumnNames").array();
                    foreach (string field in type.FieldNamesList)
                    {
                        writer.value(field);
                    }
                    writer.endArray();
                    writer.key("childColumnIds").array();
                    foreach (int colId in type.SubtypesList)
                    {
                        writer.value(colId);
                    }
                    writer.endArray();
                }
                if (type.HasPrecision)
                {
                    writer.key("precision").value(type.Precision);
                }

                if (type.HasScale)
                {
                    writer.key("scale").value(type.Scale);
                }

                if (type.HasMaximumLength)
                {
                    writer.key("maxLength").value(type.MaximumLength);
                }
                writer.endObject();
            }
        }

        private static void writeStripeInformation(JsonWriter writer, StripeInformation stripe)
        {
            writer.newObject();
            writer.key("offset").value(stripe.getOffset());
            writer.key("indexLength").value(stripe.getIndexLength());
            writer.key("dataLength").value(stripe.getDataLength());
            writer.key("footerLength").value(stripe.getFooterLength());
            writer.key("rowCount").value(stripe.getNumberOfRows());
            writer.endObject();
        }

        private static void writeColumnStatistics(JsonWriter writer, ColumnStatistics cs)
        {
            if (cs != null)
            {
                writer.key("count").value(cs.getNumberOfValues());
                writer.key("hasNull").value(cs.hasNull());
                if (cs is BinaryColumnStatistics) {
                    writer.key("totalLength").value(((BinaryColumnStatistics)cs).getSum());
                    writer.key("type").value(OrcProto.Type.Types.Kind.BINARY.ToString());
                } else if (cs is BooleanColumnStatistics) {
                    writer.key("trueCount").value(((BooleanColumnStatistics)cs).getTrueCount());
                    writer.key("falseCount").value(((BooleanColumnStatistics)cs).getFalseCount());
                    writer.key("type").value(OrcProto.Type.Types.Kind.BOOLEAN.ToString());
                } else if (cs is IntegerColumnStatistics) {
                    writer.key("min").value(((IntegerColumnStatistics)cs).getMinimum());
                    writer.key("max").value(((IntegerColumnStatistics)cs).getMaximum());
                    if (((IntegerColumnStatistics)cs).isSumDefined())
                    {
                        writer.key("sum").value(((IntegerColumnStatistics)cs).getSum());
                    }
                    writer.key("type").value(OrcProto.Type.Types.Kind.LONG.ToString());
                } else if (cs is DoubleColumnStatistics) {
                    writer.key("min").value(((DoubleColumnStatistics)cs).getMinimum());
                    writer.key("max").value(((DoubleColumnStatistics)cs).getMaximum());
                    writer.key("sum").value(((DoubleColumnStatistics)cs).getSum());
                    writer.key("type").value(OrcProto.Type.Types.Kind.DOUBLE.ToString());
                } else if (cs is StringColumnStatistics) {
                    writer.key("min").value(((StringColumnStatistics)cs).getMinimum());
                    writer.key("max").value(((StringColumnStatistics)cs).getMaximum());
                    writer.key("totalLength").value(((StringColumnStatistics)cs).getSum());
                    writer.key("type").value(OrcProto.Type.Types.Kind.STRING.ToString());
                } else if (cs is DateColumnStatistics) {
                    if (((DateColumnStatistics)cs).getMaximum() != null)
                    {
#if false
                        writer.key("min").value(((DateColumnStatistics)cs).getMinimum());
                        writer.key("max").value(((DateColumnStatistics)cs).getMaximum());
#endif
                    }
                    writer.key("type").value(OrcProto.Type.Types.Kind.DATE.ToString());
                } else if (cs is TimestampColumnStatistics) {
                    if (((TimestampColumnStatistics)cs).getMaximum() != null)
                    {
#if false
                        writer.key("min").value(((TimestampColumnStatistics)cs).getMinimum());
                        writer.key("max").value(((TimestampColumnStatistics)cs).getMaximum());
#endif
                    }
                    writer.key("type").value(OrcProto.Type.Types.Kind.TIMESTAMP.ToString());
                } else if (cs is DecimalColumnStatistics) {
                    if (((DecimalColumnStatistics)cs).getMaximum() != null)
                    {
#if false
                        writer.key("min").value(((DecimalColumnStatistics)cs).getMinimum());
                        writer.key("max").value(((DecimalColumnStatistics)cs).getMaximum());
                        writer.key("sum").value(((DecimalColumnStatistics)cs).getSum());
#endif
                    }
                    writer.key("type").value(OrcProto.Type.Types.Kind.DECIMAL.ToString());
                }
            }
        }

        private static void writeBloomFilterIndexes(JsonWriter writer, int col,
            OrcProto.BloomFilterIndex[] bloomFilterIndex)
        {
            BloomFilter stripeLevelBF = null;
            if (bloomFilterIndex != null && bloomFilterIndex[col] != null)
            {
                int entryIx = 0;
                writer.key("bloomFilterIndexes").array();
                foreach (OrcProto.BloomFilter bf in bloomFilterIndex[col].BloomFilterList)
                {
                    writer.newObject();
                    writer.key("entryId").value(entryIx++);
                    BloomFilter toMerge = BloomFilterIO.Create(bf);
                    writeBloomFilterStats(writer, toMerge);
                    if (stripeLevelBF == null)
                    {
                        stripeLevelBF = toMerge;
                    }
                    else
                    {
                        stripeLevelBF.merge(toMerge);
                    }
                    writer.endObject();
                }
                writer.endArray();
            }
            if (stripeLevelBF != null)
            {
                writer.key("stripeLevelBloomFilter");
                writer.newObject();
                writeBloomFilterStats(writer, stripeLevelBF);
                writer.endObject();
            }
        }

        private static void writeBloomFilterStats(JsonWriter writer, BloomFilter bf)
        {
            int bitCount = bf.getBitSize();
            int popCount = 0;
            foreach (long l in bf.getBitSet())
            {
                popCount += Long.NumberOfOnes(l);
            }
            int k = bf.getNumHashFunctions();
            float loadFactor = (float)popCount / (float)bitCount;
            float expectedFpp = (float)Math.Pow(loadFactor, k);
            writer.key("numHashFunctions").value(k);
            writer.key("bitCount").value(bitCount);
            writer.key("popCount").value(popCount);
            writer.key("loadFactor").value(loadFactor);
            writer.key("expectedFpp").value(expectedFpp);
        }

        private static void writeRowGroupIndexes(JsonWriter writer, int col,
            OrcProto.RowIndex[] rowGroupIndex)
        {

            OrcProto.RowIndex index;
            if (rowGroupIndex == null || (col >= rowGroupIndex.Length) ||
                ((index = rowGroupIndex[col]) == null))
            {
                return;
            }

            writer.key("rowGroupIndexes").array();
            for (int entryIx = 0; entryIx < index.EntryCount; ++entryIx)
            {
                writer.newObject();
                writer.key("entryId").value(entryIx);
                OrcProto.RowIndexEntry entry = index.EntryList[entryIx];
                if (entry == null)
                {
                    continue;
                }
                OrcProto.ColumnStatistics colStats = entry.Statistics;
                writeColumnStatistics(writer, ColumnStatisticsImpl.deserialize(colStats));
                writer.key("positions").array();
                for (int posIx = 0; posIx < entry.PositionsCount; ++posIx)
                {
                    writer.value(entry.PositionsList[posIx]);
                }
                writer.endArray();
                writer.endObject();
            }
            writer.endArray();
        }
    }
}
