package org.neuinfo.foundry.consumers.utils;

import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.ServiceFactory;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.ResourceIngestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bozyurt on 9/14/15.
 */
public class DataSampler {

    public static void sampleFromLincsDataSets() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("sourceURL", "ds:biocaddie-0004::HMS Dataset ID");
        options.put("urlTemplate", "http://lincs.hms.harvard.edu/db/datasets/${HMS Dataset ID}/results?search=&output_type=.csv");
        options.put("fileType", "csv");
        options.put("ignoreLines", "1");
        options.put("headerLine", "1");
        options.put("delimiter", ",");
        options.put("textQuote", "&#034;");
        options.put("escapeCharacter", "&#092;");
        options.put("fieldsToAdd", "HMS Dataset ID:hmsDatasetID");
        options.put("sampleMode", "true");

        ServiceFactory.getInstance("consumers-cfg.xml");
        ResourceIngestor ingestor = new ResourceIngestor();

        ingestor.initialize(options);
        ingestor.startup();
        int count = 0;
        File dir = new File("/tmp/lincs_ds_result");
        dir.mkdir();
        while (ingestor.hasNext()) {
            count++;
            Result result = ingestor.prepPayload();

            Utils.saveText(result.getPayload().toString(2),
                    "/tmp/lincs_ds_result/lincs_ds_result_" + count + ".json");
        }
    }


    public static void main(String[] args) throws Exception {
        sampleFromLincsDataSets();
    }
}
