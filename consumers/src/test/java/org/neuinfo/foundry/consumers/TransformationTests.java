package org.neuinfo.foundry.consumers;

import junit.framework.TestCase;
import org.jdom2.Element;
import org.json.JSONObject;
import org.neuinfo.foundry.common.transform.TransformMappingUtils;
import org.neuinfo.foundry.common.transform.TransformationEngine;
import org.neuinfo.foundry.common.util.XML2JSONConverter;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.RemoteFileIterator;
import org.neuinfo.foundry.consumers.common.XMLFileIterator;

import java.io.File;
import java.util.Arrays;

/**
 * Created by bozyurt on 11/4/15.
 */
public class TransformationTests extends TestCase {
    final static String HOME_DIR = System.getProperty("user.home");

    public TransformationTests(String name) {
        super(name);
    }


    public void testPDB() throws Exception {
        String testXmlFile = HOME_DIR + "/4ejx-noatom.xml";

        XMLFileIterator xfIter = new XMLFileIterator(
                new RemoteFileIterator(Arrays.asList(new File(testXmlFile))), null, "datablock");

        if (xfIter.hasNext()) {
            Element el = xfIter.next();
            XML2JSONConverter converter = new XML2JSONConverter();
            JSONObject json = converter.toJSON(el);
            String transformScript = TransformMappingUtils.loadTransformMappingScript(HOME_DIR +
                    "/dev/java/Foundry-Data/transformations/pdb.trs");
            TransformationEngine trEngine = new TransformationEngine(transformScript);
            JSONObject transformedJson = new JSONObject();
            trEngine.transform(json, transformedJson);
            System.out.println(transformedJson.toString(2));
        }
    }
}
