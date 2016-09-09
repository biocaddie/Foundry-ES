package org.neuinfo.foundry.common.ingestion;

import org.apache.commons.cli.*;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.util.JSONUtils;

/**
 * Created by bozyurt on 5/27/14.
 */
public class SourceIngestorCLI {

    final static String HOME_DIR = System.getProperty("user.home");

    public static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("SourceIngestorCLI", options);
        System.exit(1);
    }

    public static void ingestOBP(String[] args) throws Exception {
        Option help = new Option("h", "print this message");
        Option jsonOption = Option.builder("j").argName("source-json-file").hasArg().
                desc("harvest source description file").build();
        Option delOption = new Option("d", "delete the source given by the source-json-file");
        Option updateOption = new Option("u", "update the source given by the source-json-file");
        Option configFileOption = Option.builder("c").argName("config-file")
                .hasArg().desc("config-file e.g. common-cfg.xml (default)").build();

        jsonOption.setRequired(true);

        Options options = new Options();
        options.addOption(help);
        options.addOption(jsonOption);
        options.addOption(delOption);
        options.addOption(configFileOption);
        options.addOption(updateOption);
        CommandLineParser cli = new DefaultParser();
        CommandLine line = null;
        try {
            line = cli.parse(options, args);
        } catch (Exception x) {
            System.err.println(x.getMessage());
            usage(options);
        }
        if (line == null || line.hasOption("h")) {
            usage(options);
        }

        String jsonFilePath = line.getOptionValue('j');
        boolean delSource = line.hasOption('d');
        boolean updateSource = line.hasOption('u');

        JSONObject js = JSONUtils.loadFromFile(jsonFilePath);

        Source source = Source.fromJSON(js);

        String configFile = "common-cfg.xml";
        if (line.hasOption('c')) {
            configFile = line.getOptionValue('c');
        }

        Configuration conf = Configuration.fromXML(configFile);
        System.out.println(conf.toString());
        SourceIngestionService sis = new SourceIngestionService();
        try {
            sis.start(conf);
            if (delSource) {
                System.out.println("Deleting source " + source.getResourceID() + " [" + source.getName() + "]...");
                sis.deleteSource(source);
            } else if (updateSource) {
                System.out.println("Updating source " + source.getResourceID() + " [" + source.getName() + "]...");
                sis.updateSource(source);
            } else {
                System.out.println("Saving source " + source.getResourceID() + " [" + source.getName() + "]...");
                sis.saveSource(source);
            }
        } finally {
            sis.shutdown();
        }
    }


    public static void main(String[] args) throws Exception {
        SourceIngestorCLI.ingestOBP(args);
    }
}
