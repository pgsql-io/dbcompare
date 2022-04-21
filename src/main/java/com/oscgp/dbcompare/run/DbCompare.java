/*
 * OSCG-Partners
 * 2022 All rights reserved 
 */
package com.oscgp.dbcompare.run;

import com.oscgp.dbcompare.common.OraConnect;
import com.oscgp.dbcompare.common.PGConnect;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 *
 * @author Muhammad Asif Naeem
 */
public class DbCompare {
    public static void main(String[] args) {
        String[] schemaNames = null;
        String configFile = "dbcompare.conf";
        Boolean verbose = false;
        Boolean mandatoryOptionProvided = false;
        // create the command line parser
        CommandLineParser cmdParser = new DefaultParser();
        Options osOptions = new Options();
        Option oSchemas = Option.builder("s")
                .argName("schemas")
                .hasArgs()
                .desc("the schemas information e.g. -s schema1 schema2")
                .build();
        osOptions.addOption(oSchemas);

        Option oVerbose = Option.builder("v")
                .argName("verbose")
                .hasArg(false)
                .desc("print the background activities")
                .build();
        osOptions.addOption(oVerbose);

        try {
            // parse the command line arguments
            CommandLine clOptions = cmdParser.parse(osOptions, args);

            /*FIXME:            if (clOptions.getArgList().isEmpty()) {
               System.out.println("no option provided");
               System.exit(1);
            }*/
            if (clOptions.hasOption("s")) {
                mandatoryOptionProvided = true;
                schemaNames = clOptions.getOptionValues("s");
                System.out.println("Comparison is being performed for schema " + Arrays.toString(schemaNames) + "");
            }

            if (clOptions.hasOption("v")) {
                System.out.println("The background activities will be printed");
                verbose = true;
            }

            if (!mandatoryOptionProvided) {
                System.err.println("no suitable option provided");
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("cmd", osOptions);
                System.exit(1);
            }
        } catch (ParseException exp) {
            System.err.println("Unexpected exception:" + exp.getMessage());
        }

        Properties prop = new Properties();
        try {
            FileInputStream fis = new FileInputStream(configFile);
            prop.load(fis);
        } catch (FileNotFoundException ex) {
            System.err.println("Unable to find config file '" + configFile + "'");
                System.exit(1);            
        } catch (IOException ex) {        
            System.err.println("Unable to read config file '" + configFile + "'");
                System.exit(1);
        }
    
        System.out.println("***************************************************");
        System.out.println("PostgreSQL ...");
        System.out.println("***************************************************");
        PGConnect pgCon = new PGConnect(verbose);
        pgCon.connectPG(prop.getProperty("pg.connect"), prop.getProperty("pg.user"),
                prop.getProperty("pg.password"));
        pgCon.writeTablesInfo(schemaNames);
        System.out.println("***************************************************");
        System.out.println("Oracle ...");
        System.out.println("***************************************************");
        OraConnect oraCon = new OraConnect(verbose);
        oraCon.connectOracle(prop.getProperty("ora.connect"), prop.getProperty("ora.user"),
                prop.getProperty("ora.password"));
        oraCon.writeTablesInfo(schemaNames);
        System.out.println("***************************************************");
    }
}
