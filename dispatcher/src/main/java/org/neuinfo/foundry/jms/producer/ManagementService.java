package org.neuinfo.foundry.jms.producer;

import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.DocProcessingStatsService.SourceStats;
import org.neuinfo.foundry.common.ingestion.DocProcessingStatsService.WFStatusInfo;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.ElasticSearchUtils;
import org.neuinfo.foundry.common.util.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created by bozyurt on 10/30/14.
 */
public class ManagementService {
    private PipelineTriggerHelper helper;


    public ManagementService(String queueName) {
        this.helper = new PipelineTriggerHelper(queueName);
    }

    public void startup(String configFile) throws Exception {
        this.helper.startup(configFile);
    }

    public void shutdown() {
        helper.shutdown();
    }


    void deleteDocuments(String sourceID) {
        this.helper.getDocService().deleteDocuments4Resource(this.helper.getCollectionName(), sourceID, null);
    }

    void cleanupGridFSDuplicates(String sourceID) {
        this.helper.getDocService().cleanupGridFSDuplicates(this.helper.getCollectionName(), sourceID, null);
    }

    boolean deleteIndex(String url) throws Exception {
        //HttpClient client = new DefaultHttpClient();
        HttpClient client = HttpClientBuilder.create().build();
        URIBuilder builder = new URIBuilder(url);
        URI uri = builder.build();
        System.out.println("uri:" + uri);
        return ElasticSearchUtils.sendDeleteRequest(client, uri);
    }

    void showWorkflows() {
        this.helper.showWS();
    }

    void showProcessingStats(String sourceID, boolean showTime) {
        List<SourceStats> processingStats = this.helper.getProcessingStats(sourceID);
        Map<String, WFStatusInfo> wfsiMap = this.helper.getWorkflowStatusInfo(sourceID, processingStats);
        for (SourceStats ss : processingStats) {
            WFStatusInfo wfsi = wfsiMap.get(ss.getSourceID());
            showSourceStats(ss, wfsi, showTime);
        }
    }

    void showSourceStats(SourceStats ss, WFStatusInfo wfStatusInfo, boolean showTime) {
        StringBuilder sb = new StringBuilder(128);
        sb.append(StringUtils.rightPad(ss.getSourceID(), 15)).append(" ");
        if (wfStatusInfo != null) {
            sb.append(StringUtils.leftPad(wfStatusInfo.getStatus(), 12)).append(" ");
        } else {
            sb.append(StringUtils.leftPad("unknown", 12)).append(" ");
        }

        Map<String, Integer> statusCountMap = ss.getStatusCountMap();
        int totCount = 0;
        if (ss.getStatusCountMap().containsKey("ingested")) {
            totCount = ss.getStatusCountMap().get("ingested");
        } else {
            for (Integer count : statusCountMap.values()) {
                totCount += count;
            }
        }
        sb.append("total:").append(StringUtils.leftPad(String.valueOf(totCount), 10)).append(" ");
        Integer finishedCount = statusCountMap.get("finished");
        Integer errorCount = statusCountMap.get("error");
        finishedCount = finishedCount == null ? 0 : finishedCount;
        errorCount = errorCount == null ? 0 : errorCount;
        sb.append("finished:").append(StringUtils.leftPad(finishedCount.toString(), 10)).append(" ");
        sb.append("error:").append(StringUtils.leftPad(errorCount.toString(), 8)).append("  ");
        for (String status : statusCountMap.keySet()) {
            if (status.equals("finished") || status.equals("error")) {
                continue;
            }
            Integer statusCount = statusCountMap.get(status);
            String s = StringUtils.leftPad(status + ":", 15) + StringUtils.leftPad(statusCount.toString(), 10);
            sb.append(s).append(" ");
        }
        if (wfStatusInfo != null && showTime) {
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
            if (wfStatusInfo.getStartDate() != null) {
                sb.append('[').append(sdf.format(wfStatusInfo.getStartDate())).append(" - ");
                if (wfStatusInfo.getEndDate() != null) {
                    sb.append(sdf.format(wfStatusInfo.getEndDate()));
                }
                sb.append(']');
            }
        }
        System.out.println(sb.toString().trim());
    }

    public static void showHelp() {
        System.out.println("Available commands");
        System.out.println("\thelp - shows this message.");
        System.out.println("\tingest <sourceID>");
        System.out.println("\th - show all command history");
        System.out.println("\tdelete <url> - [e.g. http://52.32.231.227:9200/geo_20151106]");
        System.out.println("\tdd <sourceID>  - delete docs for a sourceID");
        System.out.println("\tcdup <sourceID>  - clean duplicate files from GridFS for a sourceID");
        System.out.println("\ttrigger <sourceID> <status-2-match> <queue-2-send> [<new-status> [<new-out-status>]] (e.g. trigger nif-0000-00135 new.1 foundry.uuid.1)");
        System.out.println("\trun <sourceID> status:<status-2-match> step:<step-name> [on|to_end] (e.g. run nif-0000-00135 status:new.1 step:transform)");
        System.out.println("\tindex <sourceID> <status-2-match> <url> [-filter <filter-jsonpath-exp>](e.g. index biocaddie-0006 transformed.1 http://52.32.231.227:9200/geo_20151106/dataset)");
        System.out.println("\tlist - lists all of the existing sources.");
        System.out.println("\tstatus [<sourceID>] [-time <on-off>]- show processing status of data source(s)");
        System.out.println("\tws - show configured workflow(s)");
        System.out.println("\tsource <script-path> - run a list of commands from a file [currently only index and ingest commands]");
        ;
        System.out.println("\texit - exits the management client.");
    }

    public static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ManagementService", options);
        System.exit(1);
    }

    static boolean confirm(BufferedReader in, String message) throws IOException {
        System.out.print(message + " (y/[n])? ");
        String ans = in.readLine();
        ans = ans.trim();
        if (ans.equalsIgnoreCase("y")) {
            return true;
        }
        return false;
    }


    static void handleRun(String ans, ManagementService ms, Options options) throws Exception {
        String[] tokens = ans.split("\\s+");
        int numTokens = tokens.length;
        if (numTokens < 4) {
            usage(options);
        }

        String srcNifId = tokens[1];
        String status2Match = null;
        boolean run2TheEnd = false;
        String stepName = null;
        for (int i = 2; i < numTokens; i++) {
            String token = tokens[i];
            if (token.startsWith("status:")) {
                status2Match = token.substring(token.indexOf(':') + 1);
            } else if (token.startsWith("step:")) {
                stepName = token.substring(token.indexOf(':') + 1);
            } else if (token.equalsIgnoreCase("on") || token.equalsIgnoreCase("to_end")) {
                run2TheEnd = true;
            }
        }
        Source source = ms.helper.findSource(srcNifId);
        Assertion.assertNotNull(source);
        System.out.println("status2Match:" + status2Match + " stepName:" + stepName + " run2TheEnd:" + run2TheEnd);
        ms.helper.runPipelineSteps(source, status2Match, stepName, run2TheEnd);
    }

    public static class IndexCommand implements ICommand {
        ManagementService ms;

        public IndexCommand(ManagementService ms) {
            this.ms = ms;
        }

        @Override
        public void execute(Utils.OptParser optParser) throws Exception {
            int noPP = optParser.getNumOfPositionalParams();
            String srcNifId = optParser.getParam(1);
            String status2Match = optParser.getParam(2);
            String urlStr = optParser.getParam(3);
            String apiKey = null;
            if (noPP == 5) {
                apiKey = optParser.getParam(4);
            }
            String filter = optParser.getOptValue("filter");
            if (filter != null) {
                String[] tokens = filter.split("=");
                if (tokens.length != 2) {
                    throw new RuntimeException("Filter string must be of type <json-path>=<value>");
                }
            }
            Source source = ms.helper.findSource(srcNifId);
            Assertion.assertNotNull(source);
            String[] urlParts = Utils.splitServerURLAndPath(urlStr);
            ms.helper.index2ElasticSearchBulk(source, status2Match,
                    urlParts[1], urlParts[0], apiKey, filter);

        }
    }

    public static class IngestCommand implements ICommand {
        ManagementService ms;

        public IngestCommand(ManagementService ms) {
            this.ms = ms;
        }

        @Override
        public void execute(Utils.OptParser optParser) throws Exception {
            String srcNifId = optParser.getParam(1);
            Source source = ms.helper.findSource(srcNifId);
            if (source != null) {
                JSONObject json = ms.helper.prepareMessageBody("ingest", source);
                ms.helper.saveStatus(source);
                ms.helper.sendMessage(json);
            } else {
                System.err.println("Source not found:" + srcNifId);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Option help = new Option("h", "print this message");
        Option configFileOption = Option.builder("c").argName("config-file")
                .hasArg().desc("config-file e.g. dispatcher-cfg.xml (default)").build();
        Option verboseOpt = new Option("v", "if set show detailed logs");
        Options options = new Options();
        options.addOption(help);
        options.addOption(configFileOption);
        options.addOption(verboseOpt);
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
        boolean verbose = line.hasOption('v');
        if (verbose) {
            Logger.getRootLogger().setLevel(Level.DEBUG);

        }

        ManagementService ms = new ManagementService("foundry.consumer.head");
        Map<String, ICommand> commandMap = new HashMap<String, ICommand>(7);
        commandMap.put("index", new IndexCommand(ms));
        commandMap.put("ingest", new IngestCommand(ms));

        Set<String> history = new LinkedHashSet<String>();
        String lastCommand = null;
        try {
            ms.startup(configFile);
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            boolean finished = false;
            while (!finished) {
                System.out.print("Foundry:>> ");
                String ans = in.readLine();
                ans = ans.trim();
                if (ans.equals("!!") && lastCommand != null) {
                    ans = lastCommand;
                    System.out.println("running command:" + ans);
                }
                if (ans.equals("help")) {
                    showHelp();
                }
                if (ans.equals("ws")) {
                    ms.showWorkflows();
                } else if (ans.startsWith("ingest")) {
                    Utils.OptParser optParser = new Utils.OptParser(ans);
                    int noPP = optParser.getNumOfPositionalParams();
                    if (noPP == 2) {
                        history.add(ans);
                        String srcNifId = optParser.getParam(1);
                        Source source = ms.helper.findSource(srcNifId);
                        JSONObject json = ms.helper.prepareMessageBody("ingest", source);
                        // ms.helper.saveStatus(source);
                        // ms.helper.sendIngestStartMessage(source.getResourceID(), source.getDataSource());
                        ms.helper.sendMessage(json);
                        lastCommand = ans;
                    }
                } else if (ans.startsWith("run")) {
                    handleRun(ans, ms, options);

                } else if (ans.startsWith("trigger")) {
                    String[] toks = ans.split("\\s+");
                    if (toks.length == 4 || toks.length == 5 || toks.length == 6) {
                        String srcNifId = toks[1];
                        String status2Match = toks[2];
                        String toQueue = toks[3];
                        String newStatus = null;
                        if (toks.length >= 5) {
                            newStatus = toks[4];
                        }
                        String newOutStatus = null;
                        if (toks.length == 6) {
                            newOutStatus = toks[5];
                        }
                        Source source = ms.helper.findSource(srcNifId);
                        Assertion.assertNotNull(source);
                        ms.helper.triggerPipeline(source, status2Match, toQueue, newStatus, newOutStatus);
                        history.add(ans);
                        lastCommand = ans;
                    }
                } else if (ans.startsWith("index")) {
                    Utils.OptParser optParser = new Utils.OptParser(ans);
                    int noPP = optParser.getNumOfPositionalParams();
                    if (noPP == 4 || noPP == 5) {
                        String srcNifId = optParser.getParam(1);// toks[1];
                        String status2Match = optParser.getParam(2); //  toks[2];
                        String urlStr = optParser.getParam(3); // toks[3];
                        String apiKey = null;
                        if (noPP == 5) {
                            apiKey = optParser.getParam(4); // toks[4];
                        }
                        String filter = optParser.getOptValue("filter");
                        if (filter != null) {
                            String[] tokens = filter.split("=");
                            if (tokens.length != 2) {
                                throw new RuntimeException("Filter string must be of type <json-path>=<value>");
                            }
                        }
                        Source source = ms.helper.findSource(srcNifId);
                        Assertion.assertNotNull(source);
                        String[] urlParts = Utils.splitServerURLAndPath(urlStr);
                        ms.helper.index2ElasticSearchBulk(source, status2Match,
                                urlParts[1], urlParts[0], apiKey, filter);
                        history.add(ans);
                        lastCommand = ans;
                    }
                } else if (ans.startsWith("status")) {
                    Utils.OptParser optParser = new Utils.OptParser(ans);
                    int noPP = optParser.getNumOfPositionalParams();
                    String[] toks = ans.split("\\s+");
                    if (noPP == 1 || noPP == 2) {
                        boolean showTime = false;
                        if (optParser.getOptValue("time") != null &&
                                optParser.getOptValue("time").equalsIgnoreCase("on")) {
                            showTime = true;
                        }
                        if (noPP == 2) {
                            String srcNifId = toks[1];
                            ms.showProcessingStats(srcNifId, showTime);
                        } else {
                            ms.showProcessingStats(null, showTime);
                        }
                        history.add(ans);
                        lastCommand = ans;
                    }
                } else if (ans.equals("history") || ans.equals("h")) {
                    for (String h : history) {
                        System.out.println(h);
                    }
                } else if (ans.startsWith("delete")) {
                    String[] toks = ans.split("\\s+");
                    if (toks.length == 2) {
                        String url = toks[1];
                        ms.deleteIndex(url);
                    }
                } else if (ans.startsWith("dd")) {
                    String[] toks = ans.split("\\s+");
                    if (toks.length == 2) {
                        String sourceID = toks[1];
                        if (confirm(in, "Do you want to delete docs for " + sourceID + "?")) {
                            ms.deleteDocuments(sourceID);
                        }
                    }
                } else if (ans.startsWith("cdup")) {
                    String[] toks = ans.split("\\s+");
                    if (toks.length == 2) {
                        String sourceID = toks[1];
                        if (confirm(in, "Do you want to remove duplicate files for " + sourceID + " from GridFS?")) {
                            ms.cleanupGridFSDuplicates(sourceID);
                        }
                    }
                } else if (ans.startsWith("list")) {
                    List<Source> sources = ms.helper.findSources();
                    for (Source source : sources) {
                        StringBuilder sb = new StringBuilder(128);
                        sb.append(StringUtils.rightPad(source.getResourceID(), 16)).append(" - ");
                        sb.append(source.getName());
                        System.out.println(sb.toString());
                        // System.out.println(String.format("%s - (%s)", source.getResourceID(), source.getName()));
                    }
                    lastCommand = ans;
                } else if (ans.startsWith("source")) {
                    Utils.OptParser optParser = new Utils.OptParser(ans);
                    int noPP = optParser.getNumOfPositionalParams();
                    if (noPP == 2) {
                        String scriptPath = optParser.getParam(1);
                        if (!new File(scriptPath).isFile()) {
                            throw new RuntimeException("Not a valid script file path:" + scriptPath);
                        }
                        String[] lines = Utils.loadAsString(scriptPath).split("\\n");
                        for (String aLine : lines) {
                            Utils.OptParser op = new Utils.OptParser(aLine);
                            if (op.getNumOfPositionalParams() > 1) {
                                ICommand command = commandMap.get(op.getParam(0));
                                if (command != null) {
                                    command.execute(op);
                                }
                            }
                        }
                    }
                } else if (ans.equals("exit")) {
                    break;
                }
            }

        } finally {
            ms.shutdown();
        }

    }
}
