package org.neuinfo.foundry.consumers.ingestors;

import org.junit.Test;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.IngestorHelper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by bozyurt on 8/4/15.
 */
public class IngestorHelperTest {

    @Test
    public void testExtractTemplateVariable() throws Exception {
        String url = "http://lincs.hms.harvard.edu/db/datasets/${HMS Dataset ID}/results?search=&output_type=.csv";

        List<String> templateVariables = IngestorHelper.extractTemplateVariables(url);
        assertNotNull(templateVariables);
        assertTrue(templateVariables.size() == 1);
        System.out.println(templateVariables);
    }

    public void testCreateURL() throws Exception {
        String urlTemplate = "http://lincs.hms.harvard.edu/db/datasets/${HMS Dataset ID}/results?search=&output_type=.csv";
        Map<String, String> map = new HashMap<String, String>(7);
        map.put("HMS Dataset ID", "2000");

        String url = IngestorHelper.createURL(urlTemplate, map);
        assertFalse(url.isEmpty());
        assertEquals(url, "http://lincs.hms.harvard.edu/db/datasets/2000/results?search=&output_type=.csv");
        System.out.println(url);

    }

}
