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

namespace OrcSharp.External
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    public class LogFactory
    {
        public static Log getLog(Type type)
        {
            return new Log();
        }
    }

    public class Log
    {
        public void debug(string message)
        {
        }

        public void error(string p)
        {
        }

        public void info(string message)
        {
        }

        public bool isDebugEnabled()
        {
            return false;
        }

        public bool isInfoEnabled()
        {
            return false;
        }

        public bool isTraceEnabled()
        {
            return false;
        }

        public bool isWarnEnabled()
        {
            return false;
        }

        public void trace(string message)
        {
        }

        public void warn(string message)
        {
        }
    }
}
