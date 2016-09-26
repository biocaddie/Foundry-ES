package org.neuinfo.foundry.consumers.ingestors;

import org.junit.Test;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.WebIngestor;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by bozyurt on 9/15/16.
 */
public class ITWebIngestor {

    @Test
    public void testDataCite() throws Exception {
        Map<String,String> options = new HashMap<String, String>();
        options.put("ingestURL", "http://api.datacite.org/dats?publisher-id=cdl.tcia");
        options.put("parserType", "json");
        options.put("documentElement","data");
        options.put("offsetParam", "offset");
        options.put("limitParam", "rows");
        options.put("limitValue", "100");
        options.put("useCache","false");

        WebIngestor ingestor = new WebIngestor();
        ingestor.initialize(options);
        TestUtils.ingest(ingestor, "/tmp/cia_sample_record.json", 5);
    }


}
