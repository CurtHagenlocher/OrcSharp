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
    using System.IO;
    using System.Text;
    using org.apache.hadoop.hive.ql.io.orc.external;
    using OrcProto = global::orc.proto;
    using Path = org.apache.hadoop.hive.ql.io.orc.external.Path;

    /**
     * A tool for printing out the file structure of ORC files.
     */
    public static class FileDump
    {
        public static readonly string UNKNOWN = "UNKNOWN";

        public static void Main(string[] args)
        {
            Configuration conf = new Configuration();

            List<int> rowIndexCols = null;
            Options opts = createOptions();
            CommandLine cli = new GnuParser().parse(opts, args);

            if (cli.hasOption('h'))
            {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("orcfiledump", opts);
                return;
            }

            bool dumpData = cli.hasOption('d');
            if (cli.hasOption("r"))
            {
                string[] colStrs = cli.getOptionValue("r").split(",");
                rowIndexCols = new List<int>(colStrs.Length);
                foreach (string colStr in colStrs)
                {
                    rowIndexCols.Add(Int32.Parse(colStr));
                }
            }

            bool printTimeZone = cli.hasOption('t');
            bool jsonFormat = cli.hasOption('j');
            string[] files = cli.getArgs();
            if (files.Length == 0)
            {
                System.Console.Error.WriteLine("Error : ORC files are not specified");
                return;
            }

            // if the specified path is directory, iterate through all files and print the file dump
            List<string> filesInPath = new List<string>();
            foreach (string filename in files)
            {
                Path path = new Path(filename);
                filesInPath.AddRange(getAllFilesInPath(path, conf));
            }

            if (dumpData)
            {
                printData(filesInPath, conf);
            }
            else
            {
                if (jsonFormat)
                {
                    bool prettyPrint = cli.hasOption('p');
                    JsonFileDump.printJsonMetaData(filesInPath, conf, rowIndexCols, prettyPrint,
                        printTimeZone);
                }
                else
                {
                    printMetaData(filesInPath, conf, rowIndexCols, printTimeZone);
                }
            }
        }

        private static List<string> getAllFilesInPath(Path path, Configuration conf)
        {
            List<string> filesInPath = new List<string>();
            FileSystem fs = path.getFileSystem(conf);
            FileStatus fileStatus = fs.getFileStatus(path);
            if (fileStatus.isDir())
            {
                FileStatus[] fileStatuses = fs.listStatus(path, AcidUtils.hiddenFileFilter);
                foreach (FileStatus fileInPath in fileStatuses)
                {
                    if (fileInPath.isDir())
                    {
                        filesInPath.AddRange(getAllFilesInPath(fileInPath.getPath(), conf));
                    }
                    else
                    {
                        filesInPath.Add(fileInPath.getPath().ToString());
                    }
                }
            }
            else
            {
                filesInPath.Add(path.ToString());
            }

            return filesInPath;
        }

        private static void printData(List<string> files, Configuration conf)
        {
            foreach (string file in files)
            {
                printJsonData(conf, file);
                if (files.Count > 1)
                {
                    System.Console.WriteLine(new string('=', 80) + "\n");
                }
            }
        }

        private static void printMetaData(List<string> files, Configuration conf,
            List<int> rowIndexCols, bool printTimeZone)
        {
            foreach (string filename in files)
            {
                System.Console.WriteLine("Structure for " + filename);
                Path path = new Path(filename);
                Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
                System.Console.WriteLine("File Version: " + reader.getFileVersion().getName() +
                    " with " + reader.getWriterVersion());
                RecordReaderImpl rows = (RecordReaderImpl)reader.rows();
                System.Console.WriteLine("Rows: " + reader.getNumberOfRows());
                System.Console.WriteLine("Compression: " + reader.getCompression());
                if (reader.getCompression() != CompressionKind.NONE)
                {
                    System.Console.WriteLine("Compression size: " + reader.getCompressionSize());
                }
                System.Console.WriteLine("Type: " + reader.getObjectInspector().getTypeName());
                System.Console.WriteLine("\nStripe Statistics:");
                List<StripeStatistics> stripeStats = reader.getStripeStatistics();
                for (int n = 0; n < stripeStats.Count; n++)
                {
                    System.Console.WriteLine("  Stripe " + (n + 1) + ":");
                    StripeStatistics ss = stripeStats[n];
                    for (int i = 0; i < ss.getColumnStatistics().Length; ++i)
                    {
                        System.Console.WriteLine("    Column " + i + ": " +
                            ss.getColumnStatistics()[i].ToString());
                    }
                }
                ColumnStatistics[] stats = reader.getStatistics();
                int colCount = stats.Length;
                System.Console.WriteLine("\nFile Statistics:");
                for (int i = 0; i < stats.Length; ++i)
                {
                    System.Console.WriteLine("  Column " + i + ": " + stats[i].ToString());
                }
                System.Console.WriteLine("\nStripes:");
                int stripeIx = -1;
                foreach (StripeInformation stripe in reader.getStripes())
                {
                    ++stripeIx;
                    long stripeStart = stripe.getOffset();
                    OrcProto.StripeFooter footer = rows.readStripeFooter(stripe);
                    if (printTimeZone)
                    {
                        string tz = footer.WriterTimezone;
                        if (string.IsNullOrEmpty(tz))
                        {
                            tz = UNKNOWN;
                        }
                        System.Console.WriteLine("  Stripe: " + stripe.ToString() + " timezone: " + tz);
                    }
                    else
                    {
                        System.Console.WriteLine("  Stripe: " + stripe.ToString());
                    }
                    long sectionStart = stripeStart;
                    foreach (OrcProto.Stream section in footer.StreamsList)
                    {
                        string kind = section.HasKind ? section.Kind.ToString() : UNKNOWN;
                        System.Console.WriteLine("    Stream: column " + section.Column +
                            " section " + kind + " start: " + sectionStart +
                            " length " + section.Length);
                        sectionStart += (long)section.Length;
                    }
                    for (int i = 0; i < footer.ColumnsCount; ++i)
                    {
                        OrcProto.ColumnEncoding encoding = footer.ColumnsList[i];
                        StringBuilder buf = new StringBuilder();
                        buf.Append("    Encoding column ");
                        buf.Append(i);
                        buf.Append(": ");
                        buf.Append(encoding.Kind);
                        if (encoding.Kind == OrcProto.ColumnEncoding.Types.Kind.DICTIONARY ||
                            encoding.Kind == OrcProto.ColumnEncoding.Types.Kind.DICTIONARY_V2)
                        {
                            buf.Append("[");
                            buf.Append(encoding.DictionarySize);
                            buf.Append("]");
                        }
                        System.Console.WriteLine(buf);
                    }
                    if (rowIndexCols != null && rowIndexCols.Count != 0)
                    {
                        // include the columns that are specified, only if the columns are included, bloom filter
                        // will be read
                        bool[] sargColumns = new bool[colCount];
                        foreach (int colIdx in rowIndexCols)
                        {
                            sargColumns[colIdx] = true;
                        }
                        RecordReaderImpl.Index indices = rows.readRowIndex(stripeIx, null, null, null, sargColumns);
                        foreach (int col in rowIndexCols)
                        {
                            StringBuilder buf = new StringBuilder();
                            string rowIdxString = getFormattedRowIndices(col, indices.getRowGroupIndex());
                            buf.Append(rowIdxString);
                            string bloomFilString = getFormattedBloomFilters(col, indices.getBloomFilterIndex());
                            buf.Append(bloomFilString);
                            System.Console.WriteLine(buf);
                        }
                    }
                }

                FileSystem fs = path.getFileSystem(conf);
                long fileLen = fs.getContentSummary(path).getLength();
                long paddedBytes = getTotalPaddingSize(reader);
                // empty ORC file is ~45 bytes. Assumption here is file length always >0
                double percentPadding = ((double)paddedBytes / (double)fileLen) * 100;
                DecimalFormat format = new DecimalFormat("##.##");
                System.Console.WriteLine("\nFile length: " + fileLen + " bytes");
                System.Console.WriteLine("Padding length: " + paddedBytes + " bytes");
                System.Console.WriteLine("Padding ratio: " + format.format(percentPadding) + "%");
                rows.close();
                if (files.Count > 1)
                {
                    System.Console.WriteLine(new string("=", 80) + "\n");
                }
            }
        }

        private static string getFormattedBloomFilters(int col, OrcProto.BloomFilterIndex[] bloomFilterIndex)
        {
            StringBuilder buf = new StringBuilder();
            BloomFilter stripeLevelBF = null;
            if (bloomFilterIndex != null && bloomFilterIndex[col] != null)
            {
                int idx = 0;
                buf.Append("\n    Bloom filters for column ").Append(col).Append(":");
                foreach (OrcProto.BloomFilter bf in bloomFilterIndex[col].BloomFilterList)
                {
                    BloomFilter toMerge = BloomFilterIO.Create(bf);
                    buf.Append("\n      Entry ").Append(idx++).Append(":").Append(getBloomFilterStats(toMerge));
                    if (stripeLevelBF == null)
                    {
                        stripeLevelBF = toMerge;
                    }
                    else
                    {
                        stripeLevelBF.merge(toMerge);
                    }
                }
                string bloomFilterStats = getBloomFilterStats(stripeLevelBF);
                buf.Append("\n      Stripe level merge:").Append(bloomFilterStats);
            }
            return buf.ToString();
        }

        private static string getBloomFilterStats(BloomFilter bf)
        {
            StringBuilder sb = new StringBuilder();
            int bitCount = bf.getBitSize();
            int popCount = 0;
            foreach (long l in bf.getBitSet())
            {
                popCount += Long.bitCount(l);
            }
            int k = bf.getNumHashFunctions();
            float loadFactor = (float)popCount / (float)bitCount;
            float expectedFpp = (float)Math.Pow(loadFactor, k);
            DecimalFormat df = new DecimalFormat("###.####");
            sb.Append(" numHashFunctions: ").Append(k);
            sb.Append(" bitCount: ").Append(bitCount);
            sb.Append(" popCount: ").Append(popCount);
            sb.Append(" loadFactor: ").Append(df.format(loadFactor));
            sb.Append(" expectedFpp: ").Append(expectedFpp);
            return sb.ToString();
        }

        private static string getFormattedRowIndices(int col, OrcProto.RowIndex[] rowGroupIndex)
        {
            StringBuilder buf = new StringBuilder();
            OrcProto.RowIndex index;
            buf.Append("    Row group indices for column ").Append(col).Append(":");
            if (rowGroupIndex == null || (col >= rowGroupIndex.Length) ||
                ((index = rowGroupIndex[col]) == null))
            {
                buf.Append(" not found\n");
                return buf.ToString();
            }

            for (int entryIx = 0; entryIx < index.EntryCount; ++entryIx)
            {
                buf.Append("\n      Entry ").Append(entryIx).Append(": ");
                OrcProto.RowIndexEntry entry = index.EntryList[entryIx];
                if (entry == null)
                {
                    buf.Append("unknown\n");
                    continue;
                }
                OrcProto.ColumnStatistics colStats = entry.Statistics;
                if (colStats == null)
                {
                    buf.Append("no stats at ");
                }
                else
                {
                    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(colStats);
                    buf.Append(cs.ToString());
                }
                buf.Append(" positions: ");
                for (int posIx = 0; posIx < entry.PositionsCount; ++posIx)
                {
                    if (posIx != 0)
                    {
                        buf.Append(",");
                    }
                    buf.Append(entry.PositionsList[posIx]);
                }
            }
            return buf.ToString();
        }

        public static long getTotalPaddingSize(Reader reader)
        {
            long paddedBytes = 0;
            List<org.apache.hadoop.hive.ql.io.orc.StripeInformation> stripes = reader.getStripes();
            for (int i = 1; i < stripes.Count; i++)
            {
                long prevStripeOffset = stripes[i - 1].getOffset();
                long prevStripeLen = stripes[i - 1].getLength();
                paddedBytes += stripes[i].getOffset() - (prevStripeOffset + prevStripeLen);
            }
            return paddedBytes;
        }

        static Options createOptions()
        {
            Options result = new Options();

            // add -d and --data to print the rows
            result.addOption(OptionBuilder
                .withLongOpt("data")
                .withDescription("Should the data be printed")
                .create('d'));

            // to avoid breaking unit tests (when run in different time zones) for file dump, printing
            // of timezone is made optional
            result.addOption(OptionBuilder
                .withLongOpt("timezone")
                .withDescription("Print writer's time zone")
                .create('t'));

            result.addOption(OptionBuilder
                .withLongOpt("help")
                .withDescription("print help message")
                .create('h'));

            result.addOption(OptionBuilder
                .withLongOpt("rowindex")
                .withArgName("comma separated list of column ids for which row index should be printed")
                .withDescription("Dump stats for column number(s)")
                .hasArg()
                .create('r'));

            result.addOption(OptionBuilder
                .withLongOpt("json")
                .withDescription("Print metadata in JSON format")
                .create('j'));

            result.addOption(OptionBuilder
                    .withLongOpt("pretty")
                    .withDescription("Pretty print json metadata output")
                    .create('p'));

            return result;
        }

        private static void printMap(JSONWriter writer,
            Dictionary<object, object> obj,
            IList<OrcProto.Type> types,
            OrcProto.Type type
        )
        {
            writer.array();
            int keyType = (int)type.SubtypesList[0];
            int valueType = (int)type.SubtypesList[1];
            foreach (KeyValuePair<object, object> item in obj)
            {
                writer.newObject();
                writer.key("_key");
                printObject(writer, item.Key, types, keyType);
                writer.key("_value");
                printObject(writer, item.Value, types, valueType);
                writer.endObject();
            }
            writer.endArray();
        }

        private static void printList(JSONWriter writer,
            List<object> obj,
            IList<OrcProto.Type> types,
            OrcProto.Type type
        )
        {
            int subtype = (int)type.SubtypesList[0];
            writer.array();
            foreach (object item in obj)
            {
                printObject(writer, item, types, subtype);
            }
            writer.endArray();
        }

        private static void printUnion(JSONWriter writer,
            OrcUnion obj,
            IList<OrcProto.Type> types,
            OrcProto.Type type
        )
        {
            int subtype = (int)type.SubtypesList[obj.getTag()];
            printObject(writer, obj.getObject(), types, subtype);
        }

        static void printStruct(JSONWriter writer,
            OrcStruct obj,
            IList<OrcProto.Type> types,
            OrcProto.Type type)
        {
            writer.newObject();
            IList<uint> fieldTypes = type.SubtypesList;
            for (int i = 0; i < fieldTypes.Count; ++i)
            {
                writer.key(type.FieldNamesList[i]);
                printObject(writer, obj.getFieldValue(i), types, (int)fieldTypes[i]);
            }
            writer.endObject();
        }

        static void printObject(JSONWriter writer, object obj, IList<OrcProto.Type> types, int typeId)
        {
            OrcProto.Type type = types[typeId];
            if (obj == null)
            {
                writer.value(null);
            }
            else
            {
                switch (type.Kind)
                {
                    case OrcProto.Type.Types.Kind.STRUCT:
                        printStruct(writer, (OrcStruct)obj, types, type);
                        break;
                    case OrcProto.Type.Types.Kind.UNION:
                        printUnion(writer, (OrcUnion)obj, types, type);
                        break;
                    case OrcProto.Type.Types.Kind.LIST:
                        printList(writer, (List<Object>)obj, types, type);
                        break;
                    case OrcProto.Type.Types.Kind.MAP:
                        printMap(writer, (Dictionary<Object, Object>)obj, types, type);
                        break;
                    case OrcProto.Type.Types.Kind.BYTE:
                        writer.value(((ByteWritable)obj).get());
                        break;
                    case OrcProto.Type.Types.Kind.SHORT:
                        writer.value(((ShortWritable)obj).get());
                        break;
                    case OrcProto.Type.Types.Kind.INT:
                        writer.value(((IntWritable)obj).get());
                        break;
                    case OrcProto.Type.Types.Kind.LONG:
                        writer.value(((LongWritable)obj).get());
                        break;
                    case OrcProto.Type.Types.Kind.FLOAT:
                        writer.value(((FloatWritable)obj).get());
                        break;
                    case OrcProto.Type.Types.Kind.DOUBLE:
                        writer.value(((DoubleWritable)obj).get());
                        break;
                    case OrcProto.Type.Types.Kind.BOOLEAN:
                        writer.value(((BooleanWritable)obj).get());
                        break;
                    default:
                        writer.value(obj.ToString());
                        break;
                }
            }
        }

        static void printJsonData(Configuration conf, string filename)
        {
            Path path = new Path(filename);
            Reader reader = OrcFile.createReader(path.getFileSystem(conf), path);
            TextWriter @out = System.Console.Out;
            RecordReader rows = reader.rows(null);
            object row = null;
            List<OrcProto.Type> types = reader.getTypes();
            while (rows.hasNext())
            {
                row = rows.next(row);
                JSONWriter writer = new JSONWriter(@out);
                printObject(writer, row, types, 0);
                @out.Write("\n");
                @out.Flush();
            }
        }
    }
}