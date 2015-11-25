﻿/**
 * Ported from code that was
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */

namespace org.apache.hadoop.hive.ql.io.orc.external
{
    using System.Collections.Generic;

    public class serdeConstants
    {
        public const string SERIALIZATION_LIB = "serialization.lib";

        public const string SERIALIZATION_CLASS = "serialization.class";

        public const string SERIALIZATION_FORMAT = "serialization.format";

        public const string SERIALIZATION_DDL = "serialization.ddl";

        public const string SERIALIZATION_NULL_FORMAT = "serialization.null.format";

        public const string SERIALIZATION_ESCAPE_CRLF = "serialization.escape.crlf";

        public const string SERIALIZATION_LAST_COLUMN_TAKES_REST = "serialization.last.column.takes.rest";

        public const string SERIALIZATION_SORT_ORDER = "serialization.sort.order";

        public const string SERIALIZATION_USE_JSON_OBJECTS = "serialization.use.json.object";

        public const string SERIALIZATION_ENCODING = "serialization.encoding";

        public const string FIELD_DELIM = "field.delim";

        public const string COLLECTION_DELIM = "colelction.delim";

        public const string LINE_DELIM = "line.delim";

        public const string MAPKEY_DELIM = "mapkey.delim";

        public const string QUOTE_CHAR = "quote.delim";

        public const string ESCAPE_CHAR = "escape.delim";

        public const string HEADER_COUNT = "skip.header.line.count";

        public const string FOOTER_COUNT = "skip.footer.line.count";

        public const string VOID_TYPE_NAME = "void";

        public const string BOOLEAN_TYPE_NAME = "boolean";

        public const string TINYINT_TYPE_NAME = "tinyint";

        public const string SMALLINT_TYPE_NAME = "smallint";

        public const string INT_TYPE_NAME = "int";

        public const string BIGINT_TYPE_NAME = "bigint";

        public const string FLOAT_TYPE_NAME = "float";

        public const string DOUBLE_TYPE_NAME = "double";

        public const string STRING_TYPE_NAME = "string";

        public const string CHAR_TYPE_NAME = "char";

        public const string VARCHAR_TYPE_NAME = "varchar";

        public const string DATE_TYPE_NAME = "date";

        public const string DATETIME_TYPE_NAME = "datetime";

        public const string TIMESTAMP_TYPE_NAME = "timestamp";

        public const string DECIMAL_TYPE_NAME = "decimal";

        public const string BINARY_TYPE_NAME = "binary";

        public const string INTERVAL_YEAR_MONTH_TYPE_NAME = "interval_year_month";

        public const string INTERVAL_DAY_TIME_TYPE_NAME = "interval_day_time";

        public const string LIST_TYPE_NAME = "array";

        public const string MAP_TYPE_NAME = "map";

        public const string STRUCT_TYPE_NAME = "struct";

        public const string UNION_TYPE_NAME = "uniontype";

        public const string LIST_COLUMNS = "columns";

        public const string LIST_COLUMN_TYPES = "columns.types";

        public const string TIMESTAMP_FORMATS = "timestamp.formats";

        public static readonly HashSet<string> PrimitiveTypes = new HashSet<string>()
        {
            "void",
            "boolean",
            "tinyint",
            "smallint",
            "int",
            "bigint",
            "float",
            "double",
            "string",
            "varchar",
            "char",
            "date",
            "datetime",
            "timestamp",
            "interval_year_month",
            "interval_day_time",
            "decimal",
            "binary",
        };

        public static readonly HashSet<string> CollectionTypes = new HashSet<string>()
        {
            "array",
            "map",
        };

        public static readonly HashSet<string> IntegralTypes = new HashSet<string>()
        {
            "tinyint",
            "smallint",
            "int",
            "bigint",
        };
    }
}