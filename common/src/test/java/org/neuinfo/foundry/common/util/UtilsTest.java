package org.neuinfo.foundry.common.util;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import junit.framework.TestCase;
import org.jdom2.Element;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neuinfo.foundry.common.util.Assertion.assertNotNull;

/**
 * Created by bozyurt on 5/14/15.
 */
public class UtilsTest {

    @Test
    public void testFromURL2FileName() {
        String urlStr = "http://www.chibi.ubc.ca/Gemma/datasetdownload/8.19.2013/DatasetDiffEx.view.txt.gz";
        String filename = Utils.fromURL2FileName(urlStr);
        assertEquals(filename, "www_chibi_ubc_ca_Gemma_datasetdownload_8_19_2013_DatasetDiffEx_view_txt_gz");
        System.out.println(filename);
    }

    public void testLoadGzippedXML() throws Exception {
        String filePath = "/tmp/100d.xml.gz";

        Element rootEl = Utils.loadGzippedXML(filePath);
        assertNotNull(rootEl);
        assertEquals(rootEl.getName(), "datablock");
    }

    public void testExtractOrigDocSec() throws Exception {
        String HOME_DIR = System.getProperty("user.home");
        String s = Utils.loadAsString(HOME_DIR + "/dev/java/Foundry-ES/nature_data_test.json");
        JSONObject json = new JSONObject(s);
        JSONObject orig = json.getJSONObject("OriginalDoc");
        Utils.saveText(orig.toString(2), "/tmp/nature_data_test.json");
    }


    public void testConditionNatureData() throws Exception {
        String s = Utils.loadAsString("/tmp/nature_data_test.json");
        JSONObject json = new JSONObject(s);
        normalize(json);

        Utils.saveText(json.toString(2), "/tmp/nature_data_test_mod.json");
    }
    
    public void testConditionNatureDataAll() throws Exception {
        String s = Utils.loadAsString("/var/data/foundry-es/cache/data/all_data_nature.json");
        JSONObject json = new JSONObject(s);
        s = null;
        JSONArray data = json.getJSONArray("data");
        for(int i = 0; i < data.length(); i++) {
            JSONObject record = data.getJSONObject(i);
            normalize(record);
            System.out.println("handling " + i);
        }
         Utils.saveText(json.toString(2), "/tmp/all_data_nature.json");
    }

    private void normalize(JSONObject json) {
        if (json.has("studies")) {
            JSONObject studies = json.getJSONObject("studies");
            if (studies.has("nodes")) {
                JSONArray nodes = studies.getJSONArray("nodes");
                for (int i = 0; i < nodes.length(); i++) {
                    JSONObject node = nodes.getJSONObject(i);
                    JSONObject metadata = node.getJSONObject("metadata");
                    if (metadata.has("Characteristics[organism part]")) {
                        JSONArray parts = metadata.getJSONArray("Characteristics[organism part]");
                        if (hasArrays(parts)) {
                            JSONArray newArr = new JSONArray();
                            for (int j = 0; j < parts.length(); j++) {
                                JSONArray arr = parts.getJSONArray(j);
                                JSONObject a = new JSONObject();
                                a.put("name", arr.get(0).toString());
                                a.put("ontology", arr.get(1).toString());
                                a.put("id", arr.get(2).toString());
                                newArr.put(a);
                            }
                            metadata.put("Characteristics[organism part]", newArr);
                        } else {
                            JSONObject a = new JSONObject();
                            a.put("name", parts.get(0).toString());
                            a.put("ontology", parts.get(1).toString());
                            a.put("id", parts.get(2).toString());
                            JSONArray arr = new JSONArray();
                            arr.put(a);
                            metadata.put("Characteristics[organism part]", arr);
                        }
                    }

                }
            }
        }
    }

    public static boolean hasArrays(JSONArray arr) {
        if (arr.length() == 0) {
            return false;
        }
        return arr.get(0) instanceof JSONArray;
    }

    @Test
    public void testSplitServerURLAndPath() throws Exception {
        String urlStr = "http://172.21.51.125:8080/biocaddie/pdb";

        String[] toks = Utils.splitServerURLAndPath(urlStr);
        assertNotNull(toks);
        assertEquals(2, toks.length);
        for (String tok : toks) {
            System.out.println(tok);
        }
    }

    @Test
    public void testHandleLike() {
        assertTrue(Utils.handleLike("abab", "%ba%"));
        assertTrue(Utils.handleLike("abab", "ab%"));
        assertTrue(Utils.handleLike("abab", "%ab"));
        assertFalse(Utils.handleLike("abab", "%c"));
        assertTrue(Utils.handleLike("ab%ab", "%%%ab"));
    }


    @Test
    public void testNaturalLanguageDateParsing() {
        Parser parser = new Parser();
        List<DateGroup> groups = parser.parse("May 19 2016");
        assertFalse(groups.isEmpty());
        List<Date> dates = groups.get(0).getDates();
        assertTrue(dates != null && !dates.isEmpty());
        assertEquals(1, dates.size());
        System.out.println(dates.get(0));
    }

    @Test
    public void testOptParser() {
        String cmdLine = "index biocaddie-0065 finished http://biocaddie.scicrunch.io/eu_clinicaltrials_20161215/dataset " +
                "-filter dataset.available=True";
        Utils.OptParser optParser = new Utils.OptParser(cmdLine);
        assertEquals(4, optParser.getNumOfPositionalParams());
        assertNotNull(optParser.getOptValue("filter"));
        assertEquals("dataset.available=True", optParser.getOptValue("filter"));
    }

}
