package org.neuinfo.foundry.common.util;

import junit.framework.TestCase;
import org.jdom2.Element;

/**
 * Created by bozyurt on 5/14/15.
 */
public class UtilsTest extends TestCase {
    public UtilsTest(String name) {
        super(name);
    }

    public void testFromURL2FileName() {
        String urlStr = "http://www.chibi.ubc.ca/Gemma/datasetdownload/8.19.2013/DatasetDiffEx.view.txt.gz";
        System.out.println( Utils.fromURL2FileName(urlStr));
    }
    public void testLoadGzippedXML() throws Exception {
        String filePath = "/tmp/100d.xml.gz";

        Element rootEl = Utils.loadGzippedXML(filePath);
        assertNotNull(rootEl);
        assertEquals(rootEl.getName(),"datablock");
    }

    public void testSplitServerURLAndPath() throws Exception {
        String urlStr = "http://172.21.51.125:8080/biocaddie/pdb";

        String[] toks = Utils.splitServerURLAndPath(urlStr);
        assertNotNull(toks);
        assertEquals(2, toks.length);
        for(String tok : toks) {
            System.out.println(tok);
        }
    }
}
