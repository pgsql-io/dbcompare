using System;
using System.Collections.Generic;
using CommandLine;
using OSCGP.DBCompare;

namespace DBCompare
{
    class Program
    {
        static List<string> schemaNames = new List<string>();
        static Boolean verbose = false;
        static String database = null;

        class Options
        {
            [Option('s', "schemas", Required = true, HelpText = "the schemas information e.g. -s schema1 schema2")]
            public string[] InputSchemas { get; set; }

            [Option('v', "verbose", Default = false, Required = false, HelpText = "print the background activities")]
            public bool Verbose { get; set; }

            [Option('d', "database", Default = null, Required = false, HelpText = "select the database (PG or ORA)")]
            public string InputDatabase { get; set; }
        }

        static void RunOptions(Options opts)
        {
            //handle options
            if (opts.Verbose)
            {
                Console.WriteLine("verbose option passed");
                verbose = true;
            }
            if (opts.InputSchemas != null)
            {
                Console.WriteLine("input schemas : " + opts.InputSchemas);
                schemaNames.Add((String)opts.InputSchemas.GetValue(0));
            }
            if (opts.InputDatabase != null)
            {
                Console.WriteLine("input database : " + opts.InputDatabase);
                database = opts.InputDatabase;
                if (!(String.Compare(database, "ORA", StringComparison.OrdinalIgnoreCase) == 0 ||
                    String.Compare(database, "ORA", StringComparison.OrdinalIgnoreCase) == 0))
                {
                    Console.WriteLine("invalid value '" + database + "' provided for option -d");
                    Environment.Exit(1);
                }
            }
        }

        static void HandleParseError(IEnumerable<Error> errs)
        {
            //handle errors
            Console.WriteLine("Error occured while parsing the input " + errs.ToString());
        }

        static void Main(string[] args)
        {
            CommandLine.Parser.Default.ParseArguments<Options>(args)
                 .WithParsed(RunOptions)
                 .WithNotParsed(HandleParseError);
            PGConnect pgCon = new PGConnect(verbose);
            if (database == null || String.Compare(database, "PG", StringComparison.OrdinalIgnoreCase) == 0)
            {
                pgCon.setSchemaNames(schemaNames);
                pgCon.run();
            }

            OraConnect oraCon = new OraConnect(verbose);
            if (database == null || String.Compare(database, "Ora", StringComparison.OrdinalIgnoreCase) == 0)
            {
                oraCon.setSchemaNames(schemaNames);
                oraCon.run();
            }
        }
    }
}
