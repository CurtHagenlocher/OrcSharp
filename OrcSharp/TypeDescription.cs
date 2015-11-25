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
    using System.Collections.ObjectModel;
    using System.Text;

    /// <summary>
    /// This is the description of the types in an ORC file.
    /// </summary>
    public class TypeDescription
    {
        private const int MAX_PRECISION = 38;
        private const int MAX_SCALE = 38;
        private const int DEFAULT_PRECISION = 38;
        private const int DEFAULT_SCALE = 10;
        private const int DEFAULT_LENGTH = 256;

        public static TypeDescription createBoolean()
        {
            return new TypeDescription(Category.BOOLEAN);
        }

        public static TypeDescription createByte()
        {
            return new TypeDescription(Category.BYTE);
        }

        public static TypeDescription createShort()
        {
            return new TypeDescription(Category.SHORT);
        }

        public static TypeDescription createInt()
        {
            return new TypeDescription(Category.INT);
        }

        public static TypeDescription createLong()
        {
            return new TypeDescription(Category.LONG);
        }

        public static TypeDescription createFloat()
        {
            return new TypeDescription(Category.FLOAT);
        }

        public static TypeDescription createDouble()
        {
            return new TypeDescription(Category.DOUBLE);
        }

        public static TypeDescription createString()
        {
            return new TypeDescription(Category.STRING);
        }

        public static TypeDescription createDate()
        {
            return new TypeDescription(Category.DATE);
        }

        public static TypeDescription createTimestamp()
        {
            return new TypeDescription(Category.TIMESTAMP);
        }

        public static TypeDescription createBinary()
        {
            return new TypeDescription(Category.BINARY);
        }

        public static TypeDescription createDecimal()
        {
            return new TypeDescription(Category.DECIMAL);
        }

        /**
         * For decimal types, set the precision.
         * @param precision the new precision
         * @return this
         */
        public TypeDescription withPrecision(int precision)
        {
            if (category != Category.DECIMAL)
            {
                throw new ArgumentException("precision is only allowed on decimal" +
                   " and not " + category.getName());
            }
            else if (precision < 1 || precision > MAX_PRECISION || scale > precision)
            {
                throw new ArgumentException("precision " + precision +
                    " is out of range 1 .. " + scale);
            }
            this.precision = precision;
            return this;
        }

        /**
         * For decimal types, set the scale.
         * @param scale the new scale
         * @return this
         */
        public TypeDescription withScale(int scale)
        {
            if (category != Category.DECIMAL)
            {
                throw new ArgumentException("scale is only allowed on decimal" +
                    " and not " + category.getName());
            }
            else if (scale < 0 || scale > MAX_SCALE || scale > precision)
            {
                throw new ArgumentException("scale is out of range at " + scale);
            }
            this.scale = scale;
            return this;
        }

        public static TypeDescription createVarchar()
        {
            return new TypeDescription(Category.VARCHAR);
        }

        public static TypeDescription createChar()
        {
            return new TypeDescription(Category.CHAR);
        }

        /**
         * Set the maximum length for char and varchar types.
         * @param maxLength the maximum value
         * @return this
         */
        public TypeDescription withMaxLength(int maxLength)
        {
            if (category != Category.VARCHAR && category != Category.CHAR)
            {
                throw new ArgumentException("maxLength is only allowed on char and varchar and not " + category.getName());
            }
            this.maxLength = maxLength;
            return this;
        }

        public static TypeDescription createList(TypeDescription childType)
        {
            TypeDescription result = new TypeDescription(Category.LIST);
            result.children.Add(childType);
            childType.parent = result;
            return result;
        }

        public static TypeDescription createMap(TypeDescription keyType, TypeDescription valueType)
        {
            TypeDescription result = new TypeDescription(Category.MAP);
            result.children.Add(keyType);
            result.children.Add(valueType);
            keyType.parent = result;
            valueType.parent = result;
            return result;
        }

        public static TypeDescription createUnion()
        {
            return new TypeDescription(Category.UNION);
        }

        public static TypeDescription createStruct()
        {
            return new TypeDescription(Category.STRUCT);
        }

        /**
         * Add a child to a union type.
         * @param child a new child type to add
         * @return the union type.
         */
        public TypeDescription addUnionChild(TypeDescription child)
        {
            if (category != Category.UNION)
            {
                throw new ArgumentException("Can only add types to union type" +
                    " and not " + category);
            }
            children.Add(child);
            child.parent = this;
            return this;
        }

        /**
         * Add a field to a struct type as it is built.
         * @param field the field name
         * @param fieldType the type of the field
         * @return the struct type
         */
        public TypeDescription addField(string field, TypeDescription fieldType)
        {
            if (category != Category.STRUCT)
            {
                throw new ArgumentException("Can only add fields to struct type and not " + category);
            }
            fieldNames.Add(field);
            children.Add(fieldType);
            fieldType.parent = this;
            return this;
        }

        /**
         * Get the id for this type.
         * The first call will cause all of the the ids in tree to be assigned, so
         * it should not be called before the type is completely built.
         * @return the sequential id
         */
        public int getId()
        {
            // if the id hasn't been assigned, assign all of the ids from the root
            if (id == -1)
            {
                TypeDescription root = this;
                while (root.parent != null)
                {
                    root = root.parent;
                }
                root.assignIds(0);
            }
            return id;
        }

        /**
         * Get the maximum id assigned to this type or its children.
         * The first call will cause all of the the ids in tree to be assigned, so
         * it should not be called before the type is completely built.
         * @return the maximum id assigned under this type
         */
        public int getMaximumId()
        {
            // if the id hasn't been assigned, assign all of the ids from the root
            if (maxId == -1)
            {
                TypeDescription root = this;
                while (root.parent != null)
                {
                    root = root.parent;
                }
                root.assignIds(0);
            }
            return maxId;
        }

        /**
         * Get the kind of this type.
         * @return get the category for this type.
         */
        public Category getCategory()
        {
            return category;
        }

        /**
         * Get the maximum length of the type. Only used for char and varchar types.
         * @return the maximum length of the string type
         */
        public int getMaxLength()
        {
            return maxLength;
        }

        /**
         * Get the precision of the decimal type.
         * @return the number of digits for the precision.
         */
        public int getPrecision()
        {
            return precision;
        }

        /**
         * Get the scale of the decimal type.
         * @return the number of digits for the scale.
         */
        public int getScale()
        {
            return scale;
        }

        /**
         * For struct types, get the list of field names.
         * @return the list of field names.
         */
        public IList<string> getFieldNames()
        {
            return new ReadOnlyCollection<string>(fieldNames);
        }

        /**
         * Get the subtypes of this type.
         * @return the list of children types
         */
        public IList<TypeDescription> getChildren()
        {
            return children == null ? null : new ReadOnlyCollection<TypeDescription>(children);
        }

        /**
         * Assign ids to all of the nodes under this one.
         * @param startId the lowest id to assign
         * @return the next available id
         */
        private int assignIds(int startId)
        {
            id = startId++;
            if (children != null)
            {
                foreach (TypeDescription child in children)
                {
                    startId = child.assignIds(startId);
                }
            }
            maxId = startId - 1;
            return startId;
        }

        private TypeDescription(Category category)
        {
            this.category = category;
            if (category.isPrimitive())
            {
                children = null;
            }
            else
            {
                children = new List<TypeDescription>();
            }
            if (category == Category.STRUCT)
            {
                fieldNames = new List<string>();
            }
            else
            {
                fieldNames = null;
            }
        }

        private int id = -1;
        private int maxId = -1;
        private TypeDescription parent;
        private Category category;
        private List<TypeDescription> children;
        private List<string> fieldNames;
        private int maxLength = DEFAULT_LENGTH;
        private int precision = DEFAULT_PRECISION;
        private int scale = DEFAULT_SCALE;

        public void printToBuffer(StringBuilder buffer)
        {
            buffer.Append(category.getName());
            switch (category)
            {
                case Category.DECIMAL:
                    buffer.Append('(');
                    buffer.Append(precision);
                    buffer.Append(',');
                    buffer.Append(scale);
                    buffer.Append(')');
                    break;
                case Category.CHAR:
                case Category.VARCHAR:
                    buffer.Append('(');
                    buffer.Append(maxLength);
                    buffer.Append(')');
                    break;
                case Category.LIST:
                case Category.MAP:
                case Category.UNION:
                    buffer.Append('<');
                    for (int i = 0; i < children.Count; ++i)
                    {
                        if (i != 0)
                        {
                            buffer.Append(',');
                        }
                        children[i].printToBuffer(buffer);
                    }
                    buffer.Append('>');
                    break;
                case Category.STRUCT:
                    buffer.Append('<');
                    for (int i = 0; i < children.Count; ++i)
                    {
                        if (i != 0)
                        {
                            buffer.Append(',');
                        }
                        buffer.Append(fieldNames[i]);
                        buffer.Append(':');
                        children[i].printToBuffer(buffer);
                    }
                    buffer.Append('>');
                    break;
                default:
                    break;
            }
        }

        public override string ToString()
        {
            StringBuilder buffer = new StringBuilder();
            printToBuffer(buffer);
            return buffer.ToString();
        }

        private void printJsonToBuffer(string prefix, StringBuilder buffer, int indent)
        {
            for (int i = 0; i < indent; ++i)
            {
                buffer.Append(' ');
            }
            buffer.Append(prefix);
            buffer.Append("{\"category\": \"");
            buffer.Append(category.getName());
            buffer.Append("\", \"id\": ");
            buffer.Append(getId());
            buffer.Append(", \"max\": ");
            buffer.Append(maxId);
            switch (category)
            {
                case Category.DECIMAL:
                    buffer.Append(", \"precision\": ");
                    buffer.Append(precision);
                    buffer.Append(", \"scale\": ");
                    buffer.Append(scale);
                    break;
                case Category.CHAR:
                case Category.VARCHAR:
                    buffer.Append(", \"length\": ");
                    buffer.Append(maxLength);
                    break;
                case Category.LIST:
                case Category.MAP:
                case Category.UNION:
                    buffer.Append(", \"children\": [");
                    for (int i = 0; i < children.Count; ++i)
                    {
                        buffer.Append('\n');
                        children[i].printJsonToBuffer("", buffer, indent + 2);
                        if (i != children.Count - 1)
                        {
                            buffer.Append(',');
                        }
                    }
                    buffer.Append("]");
                    break;
                case Category.STRUCT:
                    buffer.Append(", \"fields\": [");
                    for (int i = 0; i < children.Count; ++i)
                    {
                        buffer.Append('\n');
                        children[i].printJsonToBuffer("\"" + fieldNames[i] + "\": ",
                            buffer, indent + 2);
                        if (i != children.Count - 1)
                        {
                            buffer.Append(',');
                        }
                    }
                    buffer.Append(']');
                    break;
                default:
                    break;
            }
            buffer.Append('}');
        }

        public string toJson()
        {
            StringBuilder buffer = new StringBuilder();
            printJsonToBuffer("", buffer, 0);
            return buffer.ToString();
        }
    }

    public enum Category
    {
        BOOLEAN,
        BYTE,
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        STRING,
        DATE,
        TIMESTAMP,
        BINARY,
        DECIMAL,
        VARCHAR,
        CHAR,
        LIST,
        MAP,
        STRUCT,
        UNION,
    }

    public static class CategoryMethods
    {
        public static bool isPrimitive(this Category category)
        {
            switch (category)
            {
                case Category.LIST:
                case Category.MAP:
                case Category.STRUCT:
                case Category.UNION:
                    return false;
                default:
                    return true;
            }
        }

        public static string getName(this Category category)
        {
            switch (category)
            {
                case Category.BOOLEAN: return "boolean";
                case Category.BYTE: return "tinyint";
                case Category.SHORT: return "smallint";
                case Category.INT: return "int";
                case Category.LONG: return "bigint";
                case Category.FLOAT: return "float";
                case Category.DOUBLE: return "double";
                case Category.STRING: return "string";
                case Category.DATE: return "date";
                case Category.TIMESTAMP: return "timestamp";
                case Category.BINARY: return "binary";
                case Category.DECIMAL: return "decimal";
                case Category.VARCHAR: return "varchar";
                case Category.CHAR: return "char";
                case Category.LIST: return "array";
                case Category.MAP: return "map";
                case Category.STRUCT: return "struct";
                case Category.UNION: return "union";
                default: throw new InvalidOperationException();
            }
        }
    }
}
