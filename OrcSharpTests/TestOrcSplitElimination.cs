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

namespace OrcSharpTests
{
    using System.Collections.Generic;
    using OrcSharp;
    using OrcSharp.External;
    using OrcSharp.Query;
    using Xunit;

#if HADOOP

    public class TestOrcSplitElimination : WithLocalDirectory
    {
        public class AllTypesRow
        {
            long userid;
            Text string1;
            double subtype;
            HiveDecimal decimal1;
            Timestamp ts;

            public AllTypesRow(long uid, string s1, double d1, HiveDecimal @decimal, Timestamp ts)
            {
                this.userid = uid;
                this.string1 = new Text(s1);
                this.subtype = d1;
                this.decimal1 = @decimal;
                this.ts = ts;
            }
        }

        // Before
        public void openFileSystem()
        {
            conf = new JobConf();
            // all columns
            conf.set("columns", "userid,string1,subtype,decimal1,ts");
            conf.set("columns.types", "bigint,string,double,decimal,timestamp");
            // needed columns
            conf.set(ColumnProjectionUtils.READ_ALL_COLUMNS, "false");
            conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "0,2");
            conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "userid,subtype");
            fs = FileSystem.getLocal(conf);
            testFilePath = new Path(workDir, "TestOrcFile." +
                testCaseName.getMethodName() + ".orc");
            fs.delete(testFilePath, false);
        }

        [Fact]
        public void testSplitEliminationSmallMaxSplit()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(AllTypesRow));

            Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
                100000, CompressionKind.NONE, 10000, 10000);
            writeData(writer);
            writer.close();
            conf.set(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDMINSPLITSIZE"), "1000");
            conf.set(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDMAXSPLITSIZE"), "5000");
            InputFormat @in = new OrcInputFormat();
            FileInputFormat.setInputPaths(conf, testFilePath.ToString());

            GenericUDF udf = new GenericUDFOPEqualOrLessThan();
            List<ExprNodeDesc> childExpr = new List<ExprNodeDesc>();
            ExprNodeColumnDesc col = new ExprNodeColumnDesc(typeof(long), "userid", "T", false);
            ExprNodeConstantDesc con = new ExprNodeConstantDesc(100);
            childExpr.Add(col);
            childExpr.Add(con);
            ExprNodeGenericFuncDesc en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
            string sargStr = Utilities.serializeExpression(en);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            InputSplit[] splits = @in.getSplits(conf, 1);
            Assert.Equal(5, splits.Length);

            con = new ExprNodeConstantDesc(1);
            childExpr[1] = con;
            en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
            sargStr = Utilities.serializeExpression(en);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            splits = @in.getSplits(conf, 1);
            Assert.Equal(0, splits.Length);

            con = new ExprNodeConstantDesc(2);
            childExpr[1] = con;
            en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
            sargStr = Utilities.serializeExpression(en);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            splits = @in.getSplits(conf, 1);
            Assert.Equal(1, splits.Length);

            con = new ExprNodeConstantDesc(5);
            childExpr[1] = con;
            en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
            sargStr = Utilities.serializeExpression(en);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            splits = @in.getSplits(conf, 1);
            Assert.Equal(2, splits.Length);

            con = new ExprNodeConstantDesc(13);
            childExpr[1] = con;
            en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
            sargStr = Utilities.serializeExpression(en);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            splits = @in.getSplits(conf, 1);
            Assert.Equal(3, splits.Length);

            con = new ExprNodeConstantDesc(29);
            childExpr[1] = con;
            en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
            sargStr = Utilities.serializeExpression(en);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            splits = @in.getSplits(conf, 1);
            Assert.Equal(4, splits.Length);

            con = new ExprNodeConstantDesc(70);
            childExpr[1] = con;
            en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
            sargStr = Utilities.serializeExpression(en);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            splits = @in.getSplits(conf, 1);
            Assert.Equal(5, splits.Length);
        }

        [Fact]
        public void testSplitEliminationLargeMaxSplit()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(AllTypesRow));

            Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
                100000, CompressionKind.NONE, 10000, 10000);
            writeData(writer);
            writer.close();
            conf.set(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDMINSPLITSIZE"), "1000");
            conf.set(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDMAXSPLITSIZE"), "150000");
            InputFormat @in = new OrcInputFormat();
            FileInputFormat.setInputPaths(conf, testFilePath.ToString());

            GenericUDF udf = new GenericUDFOPEqualOrLessThan();
            List<ExprNodeDesc> childExpr = new List<ExprNodeDesc>();
            ExprNodeColumnDesc col = new ExprNodeColumnDesc(typeof(long), "userid", "T", false);
            ExprNodeConstantDesc con = new ExprNodeConstantDesc(100);
            childExpr.Add(col);
            childExpr.Add(con);
            ExprNodeGenericFuncDesc en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
            string sargStr = Utilities.serializeExpression(en);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            InputSplit[] splits = @in.getSplits(conf, 1);
            Assert.Equal(2, splits.Length);

            con = new ExprNodeConstantDesc(0);
            childExpr[1] = con;
            en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
            sargStr = Utilities.serializeExpression(en);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            splits = @in.getSplits(conf, 1);
            // no stripes satisfies the condition
            Assert.Equal(0, splits.Length);

            con = new ExprNodeConstantDesc(2);
            childExpr[1] = con;
            en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
            sargStr = Utilities.serializeExpression(en);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            splits = @in.getSplits(conf, 1);
            // only first stripe will satisfy condition and hence single split
            Assert.Equal(1, splits.Length);

            con = new ExprNodeConstantDesc(5);
            childExpr[1] = con;
            en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
            sargStr = Utilities.serializeExpression(en);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            splits = @in.getSplits(conf, 1);
            // first stripe will satisfy the predicate and will be a single split, last stripe will be a
            // separate split
            Assert.Equal(2, splits.Length);

            con = new ExprNodeConstantDesc(13);
            childExpr[1] = con;
            en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
            sargStr = Utilities.serializeExpression(en);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            splits = @in.getSplits(conf, 1);
            // first 2 stripes will satisfy the predicate and merged to single split, last stripe will be a
            // separate split
            Assert.Equal(2, splits.Length);

            con = new ExprNodeConstantDesc(29);
            childExpr[1] = con;
            en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
            sargStr = Utilities.serializeExpression(en);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            splits = @in.getSplits(conf, 1);
            // first 3 stripes will satisfy the predicate and merged to single split, last stripe will be a
            // separate split
            Assert.Equal(2, splits.Length);

            con = new ExprNodeConstantDesc(70);
            childExpr[1] = con;
            en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);
            sargStr = Utilities.serializeExpression(en);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            splits = @in.getSplits(conf, 1);
            // first 2 stripes will satisfy the predicate and merged to single split, last two stripe will
            // be a separate split
            Assert.Equal(2, splits.Length);
        }


        [Fact]
        public void testSplitEliminationComplexExpr()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(AllTypesRow));

            Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
                100000, CompressionKind.NONE, 10000, 10000);
            writeData(writer);
            writer.close();
            conf.set(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDMINSPLITSIZE"), "1000");
            conf.set(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDMAXSPLITSIZE"), "150000");
            InputFormat @in = new OrcInputFormat();
            FileInputFormat.setInputPaths(conf, testFilePath.ToString());

            // predicate expression: userid <= 100 and subtype <= 1000.0
            GenericUDF udf = new GenericUDFOPEqualOrLessThan();
            List<ExprNodeDesc> childExpr = new List<ExprNodeDesc>();
            ExprNodeColumnDesc col = new ExprNodeColumnDesc(typeof(long), "userid", "T", false);
            ExprNodeConstantDesc con = new ExprNodeConstantDesc(100);
            childExpr.Add(col);
            childExpr.Add(con);
            ExprNodeGenericFuncDesc en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);

            GenericUDF udf1 = new GenericUDFOPEqualOrLessThan();
            List<ExprNodeDesc> childExpr1 = new List<ExprNodeDesc>();
            ExprNodeColumnDesc col1 = new ExprNodeColumnDesc(typeof(double), "subtype", "T", false);
            ExprNodeConstantDesc con1 = new ExprNodeConstantDesc(1000.0);
            childExpr1.Add(col1);
            childExpr1.Add(con1);
            ExprNodeGenericFuncDesc en1 = new ExprNodeGenericFuncDesc(inspector, udf1, childExpr1);

            GenericUDF udf2 = new GenericUDFOPAnd();
            List<ExprNodeDesc> childExpr2 = new List<ExprNodeDesc>();
            childExpr2.Add(en);
            childExpr2.Add(en1);
            ExprNodeGenericFuncDesc en2 = new ExprNodeGenericFuncDesc(inspector, udf2, childExpr2);

            string sargStr = Utilities.serializeExpression(en2);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            InputSplit[] splits = @in.getSplits(conf, 1);
            Assert.Equal(2, splits.Length);

            con = new ExprNodeConstantDesc(2);
            childExpr[1] = con;
            en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);

            con1 = new ExprNodeConstantDesc(0.0);
            childExpr1[1] = con1;
            en1 = new ExprNodeGenericFuncDesc(inspector, udf1, childExpr1);

            childExpr2[0] = en;
            childExpr2[1] = en1;
            en2 = new ExprNodeGenericFuncDesc(inspector, udf2, childExpr2);

            sargStr = Utilities.serializeExpression(en2);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            splits = @in.getSplits(conf, 1);
            // no stripe will satisfy the predicate
            Assert.Equal(0, splits.Length);

            con = new ExprNodeConstantDesc(2);
            childExpr[1] = con;
            en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);

            con1 = new ExprNodeConstantDesc(1.0);
            childExpr1[1] = con1;
            en1 = new ExprNodeGenericFuncDesc(inspector, udf1, childExpr1);

            childExpr2[0] = en;
            childExpr2[1] = en1;
            en2 = new ExprNodeGenericFuncDesc(inspector, udf2, childExpr2);

            sargStr = Utilities.serializeExpression(en2);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            splits = @in.getSplits(conf, 1);
            // only first stripe will satisfy condition and hence single split
            Assert.Equal(1, splits.Length);

            udf = new GenericUDFOPEqual();
            con = new ExprNodeConstantDesc(13);
            childExpr[1] = con;
            en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);

            con1 = new ExprNodeConstantDesc(80.0);
            childExpr1[1] = con1;
            en1 = new ExprNodeGenericFuncDesc(inspector, udf1, childExpr1);

            childExpr2[0] = en;
            childExpr2[1] = en1;
            en2 = new ExprNodeGenericFuncDesc(inspector, udf2, childExpr2);

            sargStr = Utilities.serializeExpression(en2);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            splits = @in.getSplits(conf, 1);
            // first two stripes will satisfy condition and hence single split
            Assert.Equal(2, splits.Length);

            udf = new GenericUDFOPEqual();
            con = new ExprNodeConstantDesc(13);
            childExpr[1] = con;
            en = new ExprNodeGenericFuncDesc(inspector, udf, childExpr);

            udf1 = new GenericUDFOPEqual();
            con1 = new ExprNodeConstantDesc(80.0);
            childExpr1[1] = con1;
            en1 = new ExprNodeGenericFuncDesc(inspector, udf1, childExpr1);

            childExpr2[0] = en;
            childExpr2[1] = en1;
            en2 = new ExprNodeGenericFuncDesc(inspector, udf2, childExpr2);

            sargStr = Utilities.serializeExpression(en2);
            conf.set("hive.io.filter.expr.serialized", sargStr);
            splits = @in.getSplits(conf, 1);
            // only second stripes will satisfy condition and hence single split
            Assert.Equal(1, splits.Length);
        }

        private void writeData(Writer writer)
        {
            for (int i = 0; i < 25000; i++)
            {
                if (i == 0)
                {
                    writer.addRow(new AllTypesRow(2L, "foo", 0.8, HiveDecimal.Parse("1.2"), new Timestamp(0)));
                }
                else if (i == 5000)
                {
                    writer.addRow(new AllTypesRow(13L, "bar", 80.0, HiveDecimal.Parse("2.2"), new Timestamp(5000)));
                }
                else if (i == 10000)
                {
                    writer.addRow(new AllTypesRow(29L, "cat", 8.0, HiveDecimal.Parse("3.3"), new Timestamp(10000)));
                }
                else if (i == 15000)
                {
                    writer.addRow(new AllTypesRow(70L, "dog", 1.8, HiveDecimal.Parse("4.4"), new Timestamp(15000)));
                }
                else if (i == 20000)
                {
                    writer.addRow(new AllTypesRow(5L, "eat", 0.8, HiveDecimal.Parse("5.5"), new Timestamp(20000)));
                }
                else
                {
                    writer.addRow(new AllTypesRow(100L, "zebra", 8.0, HiveDecimal.Parse("0.0"), new Timestamp(250000)));
                }
            }
        }
    }

#endif
}
