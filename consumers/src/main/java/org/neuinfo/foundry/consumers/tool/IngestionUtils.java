package org.neuinfo.foundry.consumers.tool;

import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.IOException;

/**
 * Created by bozyurt on 9/21/16.
 */
public class IngestionUtils {
    public static void ingest(Ingestor ingestor, String outFile, int sampleSize) throws Exception {
        try {
            ingestor.startup();
            int count = 0;
            while (ingestor.hasNext()) {
                String jsonFile = outFile.replaceFirst("\\.json$", "_" + (count + 1) + ".json");
                processPayload(ingestor, jsonFile);
                count++;
                if (count >= sampleSize) {
                    break;
                }
            }
        } finally {
            ingestor.shutdown();
        }
    }

    public static void processPayload(Ingestor ingestor, String outFile) throws IOException {
        Result result = ingestor.prepPayload();
        Utils.saveText(result.getPayload().toString(2), outFile);
        System.out.println("saved file:" + outFile);
    }
}
