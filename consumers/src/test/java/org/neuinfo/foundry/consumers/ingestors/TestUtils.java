package org.neuinfo.foundry.consumers.ingestors;

import junit.framework.TestCase;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by bozyurt on 9/15/16.
 */
public class TestUtils  {

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
        assertNotNull(result);
        assertTrue(result.getStatus() != Result.Status.ERROR);
        Utils.saveText(result.getPayload().toString(2), outFile);
        System.out.println("saved file:" + outFile);
    }
}
