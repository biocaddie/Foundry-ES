package org.neuinfo.foundry.consumers;

import junit.framework.TestCase;
import org.jdom2.Element;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.common.util.XML2JSONConverter;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.RemoteFileIterator;
import org.neuinfo.foundry.consumers.common.XMLFileIterator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by bozyurt on 10/7/15.
 */
public class XmlFileIteratorTests extends TestCase {
    public XmlFileIteratorTests(String name) {
        super(name);
    }

    public void testPDBIteration() throws Exception {
        File pdbXmlFile = new File("/var/temp/pdb_rsync/00/200d-noatom.xml");
        XMLFileIterator it = new XMLFileIterator(
                new RemoteFileIterator(Arrays.asList(pdbXmlFile)), null, "datablock");
        int count = 0;
        while (it.hasNext()) {
            Element docEl = it.next();
            System.out.println(docEl);
            XML2JSONConverter converter = new XML2JSONConverter();
            JSONObject json = converter.toJSON(docEl);
            Utils.saveText(json.toString(2), "/tmp/pdb_sample_1.json");
            ++count;
        }
        assertEquals(1, count);
    }


    public void testMedlineRecIteration() throws Exception {
        File rootPath = new File("/var/data/foundry-es/test");
        String[] medlineFiles = {"medline15n0579.xml", "medline15n0001.xml", "medline15n0002.xml", "medline15n0003.xml"};
        List<File> xmlFiles = new ArrayList<File>(3);
        for (String mf : medlineFiles) {
            xmlFiles.add(new File(rootPath, mf));
            // for test
            break;
        }
        XMLFileIterator it = new XMLFileIterator(
                new RemoteFileIterator(xmlFiles), "MedlineCitationSet", "MedlineCitation");
        int count = handleIteration(it, "/tmp/medline_sample_1.json");
        System.out.println("count:" + count);
        // assertEquals(90000, count);
    }

    private int handleIteration(XMLFileIterator it, String jsonFile) throws IOException {
        int count = 0;
        while (it.hasNext()) {
            Element docEl = it.next();
            System.out.println(docEl);
            if (count == 1) {
                XML2JSONConverter converter = new XML2JSONConverter();
                JSONObject json = converter.toJSON(docEl);
                Utils.saveText(json.toString(2), jsonFile);
            }
            ++count;
        }
        return count;
    }

    public void testArrayExpressRecIteration() throws Exception {
        File rootPath = new File("/var/data/foundry-es/test/array_express");
        List<File> xmlFiles = Arrays.asList(new File(rootPath, "experiments.xml"));
        XMLFileIterator it = new XMLFileIterator(new RemoteFileIterator(xmlFiles), "experiments", "experiment");
        int count = handleIteration(it, "/tmp/array_express_sample_1.json");

        System.out.println("count:" + count);
        assertTrue(count > 0);
    }
}
