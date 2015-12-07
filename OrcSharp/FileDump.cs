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
    using System.Text;
    using OrcSharp.External;
    using OrcProto = global::orc.proto;

    /**
     * A tool for printing out the file structure of ORC files.
     */
    public static class FileDump
    {
        public const string UNKNOWN = "UNKNOWN";
        public static readonly string SEPARATOR = new string('_', 120) + Environment.NewLine;
        public const int DEFAULT_BLOCK_SIZE = 256 * 1024 * 1024;
        public static readonly string DEFAULT_BACKUP_PATH = "/tmp";

#if false
        public static readonly PathFilter HIDDEN_AND_SIDE_FILE_FILTER = new PathFilter()
        {
            public bool accept(Path p)
            {
                String name = p.getName();
                return !name.startsWith("_") && !name.startsWith(".") && !name.endsWith(
                    AcidUtils.DELTA_SIDE_FILE_SUFFIX);
            }
        };
#endif

        public static void Main(string[] args)
        {
            Configuration conf = new Configuration();

            List<int> rowIndexCols = null;
            CommandLine.Options opts = createOptions();
            CommandLine cli = CommandLine.parse(opts, args);

            if (cli.hasOption('h'))
            {
                CommandLine.printHelp("orcfiledump", opts);
                return;
            }

            bool dumpData = cli.hasOption('d');
            bool recover = cli.hasOption("recover");
            bool skipDump = cli.hasOption("skip-dump");
            string backupPath = DEFAULT_BACKUP_PATH;
            if (cli.hasOption("backup-path"))
            {
                backupPath = cli.getOptionValue("backup-path");
            }

            if (cli.hasOption('r'))
            {
                string[] colStrs = cli.getOptionValue('r').Split(',');
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

#if STORAGE
            // if the specified path is directory, iterate through all files and print the file dump
            List<string> filesInPath = new List<string>();
            foreach (string filename in files)
            {
                Path path = new Path(filename);
                filesInPath.AddRange(getAllFilesInPath(path, conf));
            }
#else
            List<string> filesInPath = new List<string>(files);
#endif

            if (dumpData)
            {
                printData(filesInPath, conf);
            }
            else if (recover && skipDump)
            {
                // recoverFiles(filesInPath, conf, backupPath);
                throw new NotImplementedException();
            }
            else
            {
                if (jsonFormat)
                {
                    bool prettyPrint = cli.hasOption('p');
                    JsonFileDump.printJsonMetaData(filesInPath, conf, rowIndexCols, prettyPrint, printTimeZone);
                }
                else
                {
                    // printMetaData(filesInPath, conf, rowIndexCols, printTimeZone, recover, backupPath);
                    throw new NotImplementedException();
                }
            }
        }

#if STORAGE
        private static List<string> getAllFilesInPath(string path, Configuration conf)
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
#endif

        private static void printData(List<string> files, Configuration conf)
        {
            foreach (string file in files)
            {
                printJsonData(conf, file);
                if (files.Count > 1)
                {
                    System.Console.WriteLine(SEPARATOR);
                }
            }
        }

        private static void printMetaData(List<string> files, Configuration conf, List<int> rowIndexCols, bool printTimeZone)
        {
            foreach (string path in files)
            {
                System.Console.WriteLine("Structure for " + Path.GetFileName(path));
                Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
                System.Console.WriteLine("File Version: " + reader.getFileVersion().ToString() +
                    " with " + reader.getWriterVersion());
                using (RecordReaderImpl rows = (RecordReaderImpl)reader.rows())
                {
                    System.Console.WriteLine("Rows: " + reader.getNumberOfRows());
                    System.Console.WriteLine("Compression: " + reader.getCompression());
                    if (reader.getCompression() != CompressionKind.NONE)
                    {
                        System.Console.WriteLine("Compression size: " + reader.getCompressionSize());
                    }
                    System.Console.WriteLine("Type: " + reader.getObjectInspector().getTypeName());
                    System.Console.WriteLine();
                    System.Console.WriteLine("Stripe Statistics:");
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
                    System.Console.WriteLine();
                    System.Console.WriteLine("File Statistics:");
                    for (int i = 0; i < stats.Length; ++i)
                    {
                        System.Console.WriteLine("  Column " + i + ": " + stats[i].ToString());
                    }
                    System.Console.WriteLine();
                    System.Console.WriteLine("Stripes:");
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

                    // TODO: Storage
                    long fileLen = new FileInfo(path).Length;
                    long paddedBytes = getTotalPaddingSize(reader);
                    // empty ORC file is ~45 bytes. Assumption here is file length always >0
                    double percentPadding = ((double)paddedBytes / (double)fileLen) * 100;
                    System.Console.WriteLine();
                    System.Console.WriteLine("File length: {0} bytes", fileLen);
                    System.Console.WriteLine("Padding length: {0} bytes", paddedBytes);
                    System.Console.WriteLine("Padding ratio: {0:00.00}%", percentPadding);
                }
                if (files.Count > 1)
                {
                    System.Console.WriteLine(SEPARATOR);
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
                buf.AppendLine();
                buf.AppendFormat("    Bloom filters for column {0}:", col);
                foreach (OrcProto.BloomFilter bf in bloomFilterIndex[col].BloomFilterList)
                {
                    BloomFilter toMerge = BloomFilterIO.Create(bf);
                    buf.AppendLine();
                    buf.AppendFormat("      Entry {0}:{1}", idx++, getBloomFilterStats(toMerge));
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
                buf.AppendLine();
                buf.Append("      Stripe level merge:").Append(bloomFilterStats);
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
                popCount += Long.NumberOfOnes(l);
            }
            int k = bf.getNumHashFunctions();
            float loadFactor = (float)popCount / (float)bitCount;
            float expectedFpp = (float)Math.Pow(loadFactor, k);
            sb.AppendFormat(" numHashFunctions: {0}", k);
            sb.AppendFormat(" bitCount: {0}", bitCount);
            sb.AppendFormat(" popCount: {0}", popCount);
            sb.AppendFormat(" loadFactor: {0:00.0000}", loadFactor);
            sb.AppendFormat(" expectedFpp: {0}", expectedFpp);
            return sb.ToString();
        }

        private static string getFormattedRowIndices(int col, OrcProto.RowIndex[] rowGroupIndex)
        {
            StringBuilder buf = new StringBuilder();
            OrcProto.RowIndex index;
            buf.AppendLine();
            buf.AppendFormat("    Row group indices for column {0}:", col);
            if (rowGroupIndex == null || (col >= rowGroupIndex.Length) ||
                ((index = rowGroupIndex[col]) == null))
            {
                buf.AppendLine(" not found");
                return buf.ToString();
            }

            for (int entryIx = 0; entryIx < index.EntryCount; ++entryIx)
            {
                buf.AppendLine();
                buf.AppendFormat("      Entry {0}:", entryIx);
                OrcProto.RowIndexEntry entry = index.EntryList[entryIx];
                if (entry == null)
                {
                    buf.AppendLine("unknown");
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
            IList<StripeInformation> stripes = reader.getStripes();
            for (int i = 1; i < stripes.Count; i++)
            {
                long prevStripeOffset = stripes[i - 1].getOffset();
                long prevStripeLen = stripes[i - 1].getLength();
                paddedBytes += stripes[i].getOffset() - (prevStripeOffset + prevStripeLen);
            }
            return paddedBytes;
        }

        static CommandLine.Options createOptions()
        {
            CommandLine.Options result = new CommandLine.Options();

            // add -d and --data to print the rows
            result.addOption(CommandLine.OptionBuilder
                .withLongOpt("data")
                .withDescription("Should the data be printed")
                .create('d'));

            // to avoid breaking unit tests (when run in different time zones) for file dump, printing
            // of timezone is made optional
            result.addOption(CommandLine.OptionBuilder
                .withLongOpt("timezone")
                .withDescription("Print writer's time zone")
                .create('t'));

            result.addOption(CommandLine.OptionBuilder
                .withLongOpt("help")
                .withDescription("print help message")
                .create('h'));

            result.addOption(CommandLine.OptionBuilder
                .withLongOpt("rowindex")
                .withArgName("comma separated list of column ids for which row index should be printed")
                .withDescription("Dump stats for column number(s)")
                .hasArg()
                .create('r'));

            result.addOption(CommandLine.OptionBuilder
                .withLongOpt("json")
                .withDescription("Print metadata in JSON format")
                .create('j'));

            result.addOption(CommandLine.OptionBuilder
                .withLongOpt("pretty")
                .withDescription("Pretty print json metadata output")
                .create('p'));

            result.addOption(CommandLine.OptionBuilder
                .withLongOpt("recover")
                .withDescription("recover corrupted orc files generated by streaming")
                .create());

            result.addOption(CommandLine.OptionBuilder
                .withLongOpt("skip-dump")
                .withDescription("used along with --recover to directly recover files without dumping")
                .create());

            result.addOption(CommandLine.OptionBuilder
                .withLongOpt("backup-path")
                .withDescription("specify a backup path to store the corrupted files (default: /tmp)")
                .hasArg()
                .create());

            return result;
        }

        private static void printMap(JsonWriter writer,
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

        private static void printList(JsonWriter writer,
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

        private static void printUnion(JsonWriter writer,
            OrcUnion obj,
            IList<OrcProto.Type> types,
            OrcProto.Type type
        )
        {
            int subtype = (int)type.SubtypesList[obj.getTag()];
            printObject(writer, obj.getObject(), types, subtype);
        }

        static void printStruct(JsonWriter writer,
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

        static void printObject(JsonWriter writer, object obj, IList<OrcProto.Type> types, int typeId)
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
                        printList(writer, (List<object>)obj, types, type);
                        break;
                    case OrcProto.Type.Types.Kind.MAP:
                        printMap(writer, (Dictionary<object, object>)obj, types, type);
                        break;
                    case OrcProto.Type.Types.Kind.BYTE:
                        writer.value((byte)obj);
                        break;
                    case OrcProto.Type.Types.Kind.SHORT:
                        writer.value((short)obj);
                        break;
                    case OrcProto.Type.Types.Kind.INT:
                        writer.value((int)obj);
                        break;
                    case OrcProto.Type.Types.Kind.LONG:
                        writer.value((long)obj);
                        break;
                    case OrcProto.Type.Types.Kind.FLOAT:
                        writer.value((float)obj);
                        break;
                    case OrcProto.Type.Types.Kind.DOUBLE:
                        writer.value((double)obj);
                        break;
                    case OrcProto.Type.Types.Kind.BOOLEAN:
                        writer.value((bool)obj);
                        break;
                    default:
                        writer.value(obj.ToString());
                        break;
                }
            }
        }

        static void printJsonData(Configuration conf, string path)
        {
            Reader reader = OrcFile.createReader(() => File.OpenRead(path), path);
            TextWriter @out = System.Console.Out;
            using (RecordReader rows = reader.rows(null))
            {
                IList<OrcProto.Type> types = reader.getTypes();
                while (rows.hasNext())
                {
                    object row = rows.next();
                    JsonWriter writer = new JsonWriter(@out);
                    printObject(writer, row, types, 0);
                    @out.WriteLine();
                    @out.Flush();
                }
            }
        }
    }
}
