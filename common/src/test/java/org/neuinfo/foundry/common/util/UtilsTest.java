package org.neuinfo.foundry.common.util;

import junit.framework.TestCase;
import org.jdom2.Element;
import org.junit.Test;

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
}
