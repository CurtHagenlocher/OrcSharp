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

    class CommandLine
    {
        internal static CommandLine parse(Options opts, string[] args)
        {
            throw new NotImplementedException();
        }

        internal bool hasOption(char v)
        {
            throw new NotImplementedException();
        }

        internal static void printHelp(string v, Options opts)
        {
            throw new NotImplementedException();
        }

        internal string[] getArgs()
        {
            throw new NotImplementedException();
        }

        internal class OptionBuilder
        {
            internal Option create(char v)
            {
                throw new NotImplementedException();
            }

            internal static OptionBuilder withLongOpt(string v)
            {
                throw new NotImplementedException();
            }

            internal OptionBuilder withDescription(string v)
            {
                throw new NotImplementedException();
            }

            internal OptionBuilder withArgName(string v)
            {
                throw new NotImplementedException();
            }

            internal OptionBuilder hasArg()
            {
                throw new NotImplementedException();
            }
        }

        internal string getOptionValue(char v)
        {
            throw new NotImplementedException();
        }

        internal class Option
        {
        }

        internal class Options
        {
            internal void addOption(object p)
            {
                throw new NotImplementedException();
            }
        }
    }
}
