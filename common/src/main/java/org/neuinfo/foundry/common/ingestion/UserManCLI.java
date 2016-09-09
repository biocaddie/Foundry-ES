package org.neuinfo.foundry.common.ingestion;

import org.apache.commons.cli.*;
import org.neuinfo.foundry.common.model.User;

import java.util.List;

/**
 * Created by bozyurt on 5/18/16.
 */
public class UserManCLI {
    private String configFile = "common-cfg.xml";

    void createUsers(String[] users) throws Exception {
        Configuration conf = Configuration.fromXML(configFile);
        System.out.println(conf.toString());
        UserIngestionService uis = new UserIngestionService();
        try {
            uis.start(conf);
            for (String userStr : users) {
                String username = userStr;
                String password = userStr;
                int idx = userStr.indexOf(':');
                if (idx != -1) {
                    username = userStr.substring(0, idx);
                    password = userStr.substring(idx + 1);
                }
                System.out.println("creating user:" + username);
                uis.saveUser(username, password);
            }
        } finally {
            uis.shutdown();
        }
    }

    void deleteUser(String username) throws Exception {
        Configuration conf = Configuration.fromXML(configFile);
        UserIngestionService uis = new UserIngestionService();
        try {
            uis.start(conf);
            uis.deleteUser(username);
        } finally {
            uis.shutdown();
        }
    }

    void listUsers() throws Exception {
        Configuration conf = Configuration.fromXML(configFile);
        UserIngestionService uis = new UserIngestionService();
        try {
            uis.start(conf);
            List<User> users = uis.listUsers();
            for (User u : users) {
                System.out.println("\t" + u.getUsername() + " role:" + u.getRole());
            }
        } finally {
            uis.shutdown();
        }
    }

    public static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("UserManCLI", options);
        System.exit(1);
    }


    public static void main(String[] args) throws Exception {
        Option help = new Option("h", "print this message");
        Option configFileOption = Option.builder("c").argName("config-file")
                .hasArg().desc("config-file e.g. common-cfg.xml").build();
        Option usersOption = Option.builder("u").argName("user(s)")
                .hasArg()
                .desc("A comma separated list of users (username[:pwd]").build();
        Option listOption = Option.builder("l").desc("list all existing usernames").build();
        Option delOption = Option.builder("d").argName("user")
                .hasArg().desc("username to delete").build();
        Options options = new Options();
        options.addOption(help);
        options.addOption(configFileOption);
        options.addOption(usersOption);
        options.addOption(listOption);
        options.addOption(delOption);
        CommandLineParser cli = new DefaultParser();
        CommandLine line = null;
        try {
            line = cli.parse(options, args);
        } catch (Exception x) {
            System.err.println(x.getMessage());
            usage(options);
        }
        if (line.hasOption("h") || (!line.hasOption("u") && !line.hasOption("l") && !line.hasOption('d'))) {
            usage(options);
        }
        UserManCLI umc = new UserManCLI();

        if (line.hasOption('c')) {
            String configFile = line.getOptionValue('c');
            umc.configFile = configFile;
        }
        if (line.hasOption('u')) {
            String usersStr = line.getOptionValue('u');
            String[] users = usersStr.split("\\s*,\\s*");

            umc.createUsers(users);
        } else if (line.hasOption('l')) {
            umc.listUsers();
        } else if (line.hasOption('d')) {
            String username = line.getOptionValue('d');
            umc.deleteUser(username);
        }
    }
}
