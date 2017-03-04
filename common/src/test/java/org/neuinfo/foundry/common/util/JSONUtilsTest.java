package org.neuinfo.foundry.common.util;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by bozyurt on 3/2/17.
 */
public class JSONUtilsTest {

    @Test
    public void testNormalize() throws Exception {
        BufferedReader in = null;
        try {
            in = new BufferedReader(new InputStreamReader(
                    JSONUtils.class.getClassLoader().getResourceAsStream("testdata/nature_record_15.json")));
            String jsonStr = Utils.loadAsString(in);
            JSONObject json = new JSONObject(jsonStr);

            JSONUtils.normalize(json);

            // System.out.println(json.toString(2));

            JSONObject metadata = json.getJSONObject("studies").getJSONArray("nodes").getJSONObject(0).getJSONObject("metadata");
            assertNotNull(metadata);
            assertTrue(metadata.has("Comment[strain]"));
            assertTrue(metadata.get("Comment[strain]") instanceof JSONArray);
            assertTrue(metadata.getJSONArray("Comment[strain]").length() == 1);
            assertTrue(metadata.getJSONArray("Comment[strain]").get(0) instanceof String);
            assertEquals("ISO1 ", metadata.getJSONArray("Comment[strain]").getString(0));

        } finally {
            Utils.close(in);
        }
    }
}
