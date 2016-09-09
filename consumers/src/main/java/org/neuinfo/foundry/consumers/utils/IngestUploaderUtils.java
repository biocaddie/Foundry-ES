package org.neuinfo.foundry.consumers.utils;

import org.apache.commons.cli.*;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.WebIngestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by bozyurt on 2/26/16.
 */
public class IngestUploaderUtils {


    public static void testIngestClinicalTrials(boolean useCache) throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ingestURL", "https://clinicaltrials.gov/search?term=&resultsxml=true");
        options.put("documentElement", "clinical_study");
        options.put("cacheFilename", "clinicaltrials_gov");
        options.put("filenamePattern", "\\w+\\.xml");
        options.put("useCache", String.valueOf(useCache));
        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);

        ingestor.startup();
        boolean first = true;
        int count = 0;
        while (ingestor.hasNext()) {
            Result result = ingestor.prepPayload();
            Assertion.assertNotNull(result);
            Assertion.assertTrue(result.getStatus() != Result.Status.ERROR);
            if (first) {
                Utils.saveText(result.getPayload().toString(2), "/tmp/clinicaltrials_gov_record.json");
                first = false;
            }
            count++;
            if (count > 10) {
                break;
            }
        }
        Assertion.assertTrue(count > 0);
        System.out.println("# of xml records:" + count);
    }

    public static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("IngestUploaderUtils", options);
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        Option help = new Option("h", "print this message");
        Option useCacheOption = Option.builder("uc").desc("use cached data if set (default false)").build();
        Options options = new Options();
        options.addOption(help);
        options.addOption(useCacheOption);
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
        boolean useCache = false;
        if (line.hasOption("uc")) {
            useCache = true;
        }
        IngestUploaderUtils.testIngestClinicalTrials(useCache);

    }
}
