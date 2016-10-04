package org.neuinfo.foundry.common.transform;

import org.apache.commons.cli.*;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Utils;

import java.io.File;
import java.io.IOException;

/**
 * Created by bozyurt on 9/29/16.
 */
public class TransformationChecker {
    public static String HOME = System.getProperty("user.home");

    public static void doTransform(String transformationScript, File sampleFile) throws Exception {
        String jsonStr = Utils.loadAsString(sampleFile.getAbsolutePath());
        TransformationEngine trEngine = new TransformationEngine(transformationScript);
        JSONObject transformedJson = new JSONObject();
        JSONObject json = new JSONObject(jsonStr);
        trEngine.transform(json, transformedJson);

        System.out.println(transformedJson.toString(2));
    }

    public static File findMatchingSampleDir(File samplesRootDir, String trName) {
        File[] files = samplesRootDir.listFiles();
        for (File f : files) {
            if (f.isDirectory()) {
                String name = f.getName();
                if (name.equalsIgnoreCase(trName)) {
                    return f;
                }
            }

        }
        return null;
    }

    public static File findMatchingTransformationFile(File trRootDir, String trName) {
        File[] files = trRootDir.listFiles();
        for (File f : files) {
            if (f.getName().startsWith(trName)) {
                return f;
            }
        }
        return null;
    }

    public static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("TransformationChecker", options);
        System.exit(1);
    }


    public static void main(String[] args) throws Exception {
        Option help = new Option("h", "print this message");
        Option commandOption = Option.builder("c").argName("command").hasArg()
                .desc("one of [list,test]").required().build();
        Option rootDirOption = Option.builder("f").argName("data-pipeline-root-dir").hasArg().build();
        Option trOption = Option.builder("n").argName("name of the transformation file").hasArg().build();
        Options options = new Options();
        options.addOption(help);
        options.addOption(commandOption);
        options.addOption(rootDirOption);
        options.addOption(trOption);

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
        String rootDir = HOME + "/dev/biocaddie/data-pipeline";

        if (line.hasOption('f')) {
            rootDir = line.getOptionValue('f');
        }
        if (!new File(rootDir).isDirectory()) {
            System.err.println("Please provide the root dir for data-pipeline (transformation files location) via -f option!");
            System.exit(1);
        }
        String cmd = line.getOptionValue('c');
        File trDir = new File(rootDir, "transformations");
        if (cmd.equals("list") || cmd.equals("ls")) {
            if (trDir.isDirectory()) {
                File[] files = trDir.listFiles();
                for (File f : files) {
                    if (f.getName().endsWith(".trs")) {
                        System.out.println(f.getName().replace(".trs", ""));
                    }
                }
            }
        } else if (cmd.equals("test")) {
            File samplesRootDir = new File(rootDir, "SampleData");
            String trName = line.getOptionValue('n');
            File matchingDir = findMatchingSampleDir(samplesRootDir, trName);
            System.out.println( matchingDir);
            File trFile = findMatchingTransformationFile(trDir, trName);
            if (matchingDir != null) {
                String trScript = Utils.loadAsString(trFile.getAbsolutePath());
                for(File f: matchingDir.listFiles()) {
                    if (f.getName().endsWith(".json")) {
                        doTransform(trScript, f);
                        System.out.println("============================");
                    }
                }
            }

        }
    }
}
