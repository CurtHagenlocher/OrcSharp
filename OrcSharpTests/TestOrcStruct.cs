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
    using OrcSharp.Serialization;
    using Xunit;

    public class TestOrcStruct
    {
        [Fact]
        public void testStruct()
        {
            OrcStruct st1 = new OrcStruct(4);
            OrcStruct st2 = new OrcStruct(4);
            OrcStruct st3 = new OrcStruct(3);
            st1.setFieldValue(0, "hop");
            st1.setFieldValue(1, "on");
            st1.setFieldValue(2, "pop");
            st1.setFieldValue(3, 42);
            Assert.Equal(false, st1.Equals(null));
            st2.setFieldValue(0, "hop");
            st2.setFieldValue(1, "on");
            st2.setFieldValue(2, "pop");
            st2.setFieldValue(3, 42);
            Assert.Equal(st1, st2);
            st3.setFieldValue(0, "hop");
            st3.setFieldValue(1, "on");
            st3.setFieldValue(2, "pop");
            Assert.Equal(false, st1.Equals(st3));
#if PREDICTABLE_STRING_HASH
            Assert.Equal(11241, st1.GetHashCode());
#endif
            Assert.Equal(st1.GetHashCode(), st2.GetHashCode());
#if PREDICTABLE_STRING_HASH
            Assert.Equal(11204, st3.GetHashCode());
#endif
            Assert.Equal("{hop, on, pop, 42}", st1.ToString());
            st1.setFieldValue(3, null);
            Assert.Equal(false, st1.Equals(st2));
            Assert.Equal(false, st2.Equals(st1));
            st2.setFieldValue(3, null);
            Assert.Equal(st1, st2);
        }

        [Fact]
        public void testInspectorFromTypeInfo()
        {
            TypeInfo typeInfo =
                TypeInfoUtils.getTypeInfoFromTypeString("struct<c1:boolean,c2:tinyint" +
                    ",c3:smallint,c4:int,c5:bigint,c6:float,c7:double,c8:binary," +
                    "c9:string,c10:struct<c1:int>,c11:map<int,int>,c12:uniontype<int>" +
                    ",c13:array<timestamp>>");
            StructObjectInspector inspector = (StructObjectInspector)
                OrcStruct.createObjectInspector(typeInfo);
            Assert.Equal("struct<c1:boolean,c2:tinyint,c3:smallint,c4:int,c5:" +
                "bigint,c6:float,c7:double,c8:binary,c9:string,c10:struct<" +
                "c1:int>,c11:map<int,int>,c12:uniontype<int>,c13:array<timestamp>>",
                inspector.getTypeName());
            Assert.Equal(null,
                inspector.getAllStructFieldRefs()[0].getFieldComment());
            Assert.Equal(null, inspector.getStructFieldRef("UNKNOWN"));
            OrcStruct s1 = new OrcStruct(13);
            for (int i = 0; i < 13; ++i)
            {
                s1.setFieldValue(i, i);
            }

            List<object> list = new List<object> { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
            Assert.Equal(list, inspector.getStructFieldsDataAsList(s1));
            ListObjectInspector listOI = (ListObjectInspector)
                inspector.getAllStructFieldRefs()[12].getFieldObjectInspector();
            Assert.Equal(ObjectInspectorCategory.LIST, listOI.getCategory());
            Assert.Equal(10, listOI.getListElement(list, 10));
            Assert.Equal(null, listOI.getListElement(list, -1));
            Assert.Equal(null, listOI.getListElement(list, 13));
            Assert.Equal(13, listOI.getListLength(list));

            Dictionary<object, object> map = new Dictionary<object, object>()
            {
                {1, 2},
                {2, 4},
                {3, 6},
            };
            MapObjectInspector mapOI = (MapObjectInspector)
                inspector.getAllStructFieldRefs()[10].getFieldObjectInspector();
            Assert.Equal(3, mapOI.getMapSize(map));
            Assert.Equal(4, mapOI.getMapValueElement(map, 2));
        }

        [Fact]
        public void testUnion()
        {
            OrcUnion un1 = new OrcUnion();
            OrcUnion un2 = new OrcUnion();
            un1.set((byte)0, "hi");
            un2.set((byte)0, "hi");
            Assert.Equal(un1, un2);
            Assert.Equal(un1.GetHashCode(), un2.GetHashCode());
            un2.set((byte)0, null);
            Assert.Equal(false, un1.Equals(un2));
            Assert.Equal(false, un2.Equals(un1));
            un1.set((byte)0, null);
            Assert.Equal(un1, un2);
            un2.set((byte)0, "hi");
            un1.set((byte)1, "hi");
            Assert.Equal(false, un1.Equals(un2));
            Assert.Equal(false, un1.GetHashCode() == un2.GetHashCode());
            un2.set((byte)1, "byte");
            Assert.Equal(false, un1.Equals(un2));
            Assert.Equal("union(1, hi)", un1.ToString());
            Assert.Equal(false, un1.Equals(null));
        }
    }
}
