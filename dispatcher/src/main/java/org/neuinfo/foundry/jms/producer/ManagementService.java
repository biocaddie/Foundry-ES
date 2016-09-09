package org.neuinfo.foundry.jms.producer;

import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.DocProcessingStatsService.SourceStats;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.ElasticSearchUtils;
import org.neuinfo.foundry.common.util.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        HttpClient client = new DefaultHttpClient();
        URIBuilder builder = new URIBuilder(url);
        URI uri = builder.build();
        System.out.println("uri:" + uri);
        return ElasticSearchUtils.sendDeleteRequest(client, uri);
    }

    void showProcessingStats(String sourceID) {
        List<SourceStats> processingStats = this.helper.getProcessingStats();
        if (sourceID == null) {
            for (SourceStats ss : processingStats) {
                showSourceStats(ss);
            }
        } else {
            for (SourceStats ss : processingStats) {
                if (ss.getSourceID().equals(sourceID)) {
                    showSourceStats(ss);
                    break;
                }
            }
        }
    }

    void showSourceStats(SourceStats ss) {
        StringBuilder sb = new StringBuilder(128);
        sb.append(ss.getSourceID()).append("\t");
        Map<String, Integer> statusCountMap = ss.getStatusCountMap();
        int totCount = 0;
        for (Integer count : statusCountMap.values()) {
            totCount += count;
        }
        sb.append("total:").append(StringUtils.leftPad(String.valueOf(totCount), 10)).append("\t");
        Integer finishedCount = statusCountMap.get("finished");
        Integer errorCount = statusCountMap.get("error");
        finishedCount = finishedCount == null ? 0 : finishedCount;
        errorCount = errorCount == null ? 0 : errorCount;
        sb.append("finished:").append(StringUtils.leftPad(finishedCount.toString(), 10)).append("\t");
        sb.append("error:").append(StringUtils.leftPad(errorCount.toString(), 10)).append("\t");
        for (String status : statusCountMap.keySet()) {
            if (status.equals("finished") || status.equals("error")) {
                continue;
            }
            Integer statusCount = statusCountMap.get(status);
            sb.append(status).append(':').append(StringUtils.leftPad(statusCount.toString(), 10)).append("\t");
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
        System.out.println("\tindex <sourceID> <status-2-match> <url> (e.g. index biocaddie-0006 transformed.1 http://52.32.231.227:9200/geo_20151106/dataset)");
        System.out.println("\tlist - lists all of the existing sources.");
        System.out.println("\tstatus [<sourceID>] - show processing status of data source(s)");
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

        ManagementService ms = new ManagementService("foundry.consumer.head");
        Set<String> history = new LinkedHashSet<String>();
        try {
            ms.startup(configFile);
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            boolean finished = false;
            while (!finished) {
                System.out.print("Foundry:>> ");
                String ans = in.readLine();
                ans = ans.trim();
                if (ans.equals("help")) {
                    showHelp();
                } else if (ans.startsWith("ingest")) {
                    String[] toks = ans.split("\\s+");
                    if (toks.length == 2) {
                        history.add(ans);
                        String srcNifId = toks[1];
                        Source source = ms.helper.findSource(srcNifId);
                        JSONObject json = ms.helper.prepareMessageBody("ingest", source);
                        ms.helper.sendMessage(json);

                    }
                } else if (ans.startsWith("trigger")) {
                    String[] toks = ans.split("\\s+");
                    if (toks.length == 4 || toks.length == 5 || toks.length == 6) {
                        String srcNifId = toks[1];
                        String status2Match = toks[2];
                        String toQueue = toks[3];
                        String newStatus = null;
                        if (toks.length == 5) {
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
                    }
                } else if (ans.startsWith("index")) {
                    String[] toks = ans.split("\\s+");
                    if (toks.length == 4 || toks.length == 5) {
                        String srcNifId = toks[1];
                        String status2Match = toks[2];
                        String urlStr = toks[3];
                        String apiKey = null;
                        if (toks.length == 5) {
                            apiKey = toks[4];
                        }
                        Source source = ms.helper.findSource(srcNifId);
                        Assertion.assertNotNull(source);
                        String[] urlParts = Utils.splitServerURLAndPath(urlStr);
                        ms.helper.index2ElasticSearchBulk(source, status2Match, urlParts[1], urlParts[0], apiKey);
                        history.add(ans);
                    }
                } else if (ans.startsWith("status")) {
                    String[] toks = ans.split("\\s+");
                    if (toks.length == 1 || toks.length == 2) {
                        if (toks.length == 2) {
                            String srcNifId = toks[1];
                            ms.showProcessingStats(srcNifId);
                        } else {
                            ms.showProcessingStats(null);
                        }
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
                        System.out.println(String.format("%s - (%s)", source.getResourceID(), source.getName()));
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
