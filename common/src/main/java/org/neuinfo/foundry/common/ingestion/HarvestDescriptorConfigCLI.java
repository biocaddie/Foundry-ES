package org.neuinfo.foundry.common.ingestion;

import org.apache.commons.cli.*;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.IngestConfig;
import org.neuinfo.foundry.common.util.Utils;

import java.io.File;
import java.util.List;

/**
 * Created by bozyurt on 10/22/15.
 */
public class HarvestDescriptorConfigCLI {
    public static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("HarvestDescriptorConfigCLI", options);
        System.exit(1);
    }

    static File findMatching(List<File> files, String name) {
        for (File f : files) {
            if (f.getName().indexOf(name) != -1) {
                return f;
            }
        }
        return null;
    }


    public static void main(String[] args) throws Exception {
        Option help = new Option("h", "print this message");
        Option cmdOption = Option.builder("c").argName("command").hasArg()
                .desc("one of [(f)ull,(l)ist,(a)dd,(d)el]").build();
        Option descNameOption = Option.builder("i").argName("descriptor config filename").hasArg().build();
        cmdOption.setRequired(true);
        Options options = new Options();
        options.addOption(help);
        options.addOption(cmdOption);
        options.addOption(descNameOption);
        CommandLineParser cli = new DefaultParser();
        CommandLine line = null;
        try {
            line = cli.parse(options, args);
        } catch (Exception x) {
            System.err.println(x.getMessage());
            usage(options);
        }
        if (line.hasOption("h")) {
            usage(options);
        }
        String cmd = line.getOptionValue('c');
        Configuration conf = Configuration.fromXML("common-cfg.xml");

        HarvestDescriptorConfigService service = new HarvestDescriptorConfigService();
        try {
            String homeDir = System.getProperty("user.home");
            service.start(conf);
            File dir = new File(homeDir + "/dev/java/Foundry-ES/consumers/etc/ingestor-configs");
            //File dir = new File(homeDir + "/work/Foundry-ES/consumers/etc/ingestor-configs");
            List<File> files = Utils.findAllFilesMatching(dir, new Utils.RegexFileNameFilter("\\.json$"));
            if (cmd.equals("list") || cmd.equals("l")) {
                for (File f : files) {
                    String name = f.getName().replaceFirst("\\.json$", "");
                    System.out.println("\t" + name);
                }
            } else if (cmd.equals("f") || cmd.equals("full")) {
                for (File jsonFile : files) {
                    String jsonStr = Utils.loadAsString(jsonFile.getAbsolutePath());
                    JSONObject json = new JSONObject(jsonStr);
                    IngestConfig ic = IngestConfig.fromJSON(json);
                    service.saveIngestConfig(ic);
                }
            } else if (cmd.equals("a") || cmd.equals("add")) {
                String name = line.getOptionValue('i');
                if (name == null) {
                    usage(options);
                }
                File matching = findMatching(files, name);
                if (matching != null) {
                    String jsonStr = Utils.loadAsString(matching.getAbsolutePath());
                    JSONObject json = new JSONObject(jsonStr);
                    IngestConfig ic = IngestConfig.fromJSON(json);
                    service.saveIngestConfig(ic);
                } else {
                    System.err.println("No matching harvest descriptor with name:" + name);
                }
            } else if (cmd.equals("d") || cmd.equals("del")) {
                String name = line.getOptionValue('i');
                if (name == null) {
                    usage(options);
                }
                File matching = findMatching(files, name);
                if (matching != null) {
                    String jsonStr = Utils.loadAsString(matching.getAbsolutePath());
                    JSONObject json = new JSONObject(jsonStr);
                    IngestConfig ic = IngestConfig.fromJSON(json);
                    service.deleteIngestConfig(ic);
                } else {
                    System.err.println("No matching harvest descriptor with name:" + name);
                }
            }

        } finally {
            service.shutdown();
        }
    }
}

