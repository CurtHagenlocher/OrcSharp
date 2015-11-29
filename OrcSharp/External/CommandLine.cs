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

namespace OrcSharp.External
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    class CommandLine
    {
        private readonly string[] args;
        private readonly Dictionary<char, string> options;

        private CommandLine(string[] args, Dictionary<char, string> options)
        {
            this.args = args;
            this.options = options;
        }

        internal static CommandLine parse(Options opts, string[] args)
        {
            List<string> newArgs = new List<string>(args.Length);
            Dictionary<char, string> options = new Dictionary<char, string>(args.Length);
            for (int i = 0; i < args.Length; i++)
            {
                char option;
                string value;
                if (args[i].StartsWith("-") && opts.TryGetOption(args[i], out option, out value))
                {
                    options[option] = value;
                }
                else
                {
                    newArgs.Add(args[i]);
                }
            }

            return new CommandLine(newArgs.ToArray(), options);
        }

        internal bool hasOption(char v)
        {
            return options.ContainsKey(v);
        }

        internal static void printHelp(string v, Options opts)
        {
            throw new NotImplementedException();
        }

        internal string[] getArgs()
        {
            return args;
        }

        internal class OptionBuilder : Option
        {
            internal Option create(char shortOption)
            {
                this.shortOption = shortOption;
                return this;
            }

            internal static OptionBuilder withLongOpt(string longOption)
            {
                return new OptionBuilder { longOption = longOption };
            }

            internal OptionBuilder withDescription(string description)
            {
                this.description = description;
                return this;
            }

            internal OptionBuilder withArgName(string argumentName)
            {
                this.argumentName = argumentName;
                return this;
            }

            internal OptionBuilder hasArg()
            {
                hasArgument = true;
                return this;
            }
        }

        internal string getOptionValue(char v)
        {
            return options[v];
        }

        internal class Option
        {
            protected char shortOption;
            protected string longOption;
            protected string description;
            protected string argumentName;
            protected bool hasArgument;

            public char ShortOption { get { return shortOption; } }
            public string LongOption { get { return longOption; } }
            public bool HasArgument { get { return hasArgument; } }
        }

        internal class Options : List<Option>
        {
            internal void addOption(Option option)
            {
                Add(option);
            }

            public bool TryGetOption(string arg, out char option, out string value)
            {
                string[] args = arg.Split('=');
                arg = args[0];

                Option opt;
                if (arg.Length == 2 && arg[0] == '-')
                {
                    opt = this.Where(o => o.ShortOption == arg[1]).FirstOrDefault();
                }
                else if (arg.Length > 2 && arg[0] == '-' && arg[1] == '-')
                {
                    arg = arg.Substring(2);
                    opt = this.Where(o => o.LongOption == arg).FirstOrDefault();
                }
                else
                {
                    opt = null;
                }

                if (opt == null)
                {
                    option = (char)0;
                    value = null;
                    return false;
                }
                else
                {
                    option = opt.ShortOption;
                    value = opt.HasArgument ? args[1] : null;
                    return true;
                }
            }
        }
    }
}
