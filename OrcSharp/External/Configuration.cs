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

namespace org.apache.hadoop.hive.ql.io.orc.external
{
    using System;

    public class Configuration
    {
        public bool? getBoolean(string key)
        {
            return null;
        }

        public string get(string key)
        {
            return null;
        }

        public long? getLong(string key, long defaultValue = 0)
        {
            return null;
        }

        internal void set(string v, string sargStr)
        {
            throw new NotImplementedException();
        }
    }

    public class Properties
    {
        internal string getProperty(string attribute)
        {
            return null;
        }
    }
}
