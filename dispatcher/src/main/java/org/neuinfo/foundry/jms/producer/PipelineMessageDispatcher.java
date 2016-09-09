package org.neuinfo.foundry.jms.producer;

import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.neuinfo.foundry.common.config.ConfigLoader;
import org.neuinfo.foundry.common.config.Configuration;
import org.neuinfo.foundry.jms.common.SourceProcessStatusReplier;

/**
 * Created by bozyurt on 7/9/15.
 */
public class PipelineMessageDispatcher {
    private Configuration config;
    private PipelineMessageListener messageListener;
    SourceProcessStatusReplier spsReplier;
    private final static Logger logger = Logger.getLogger(PipelineMessageDispatcher.class);

    public void startup(String configFile) throws Exception {
        if (configFile != null) {
            this.config = ConfigLoader.load(configFile);
        } else {
            this.config = ConfigLoader.load("dispatcher-cfg.xml");
        }
        System.out.println(this.config);

        this.messageListener = new PipelineMessageListener(config);

        this.messageListener.startup();
        spsReplier = new SourceProcessStatusReplier(config);
        spsReplier.startup();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdown();
            }
        });
    }

    private void shutdown() {
        if (spsReplier != null) {
            spsReplier.shutdown();
        }
        if (messageListener != null) {
            logger.info("shutting down PipelineMessageListener...");
            messageListener.shutdown();
        }
    }

    public static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Dispatcher", options);
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        Option help = new Option("h", "print this message");
        Option configFileOption = Option.builder("c").argName("config-file")
                .hasArg().desc("config-file e.g. dispatcher-cfg.xml (default)").build();

        Options options = new Options();
        options.addOption(help);
        options.addOption(configFileOption);
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

        String configFile = null;
        if (line.hasOption('c')) {
            configFile = line.getOptionValue('c');
        }

        PipelineMessageDispatcher dispatcher = new PipelineMessageDispatcher();
        dispatcher.startup(configFile);
    }


}
