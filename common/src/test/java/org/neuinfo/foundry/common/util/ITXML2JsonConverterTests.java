package org.neuinfo.foundry.common.util;

import junit.framework.TestCase;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.json.JSONObject;
import org.junit.Test;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.builder.Input;
import org.xmlunit.diff.*;

import javax.xml.transform.Source;
import java.io.BufferedReader;
import java.io.BufferedWriter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Created by bozyurt on 3/3/15.
 */
public class ITXML2JsonConverterTests {


    @Test
    public void testAustraliaMetaData() throws Exception {
        String xmlSource = "http://hydro10.sdsc.edu/metadata/geoscience_australia/043dbb8c-66de-6897-e054-00144fdd4fa6.xml";
        SAXBuilder builder = new SAXBuilder();
        Document doc = builder.build(xmlSource);
        Element rootEl = doc.getRootElement();


        XML2JSONConverter converter = new XML2JSONConverter();
        JSONObject json = converter.toJSON(rootEl);

        System.out.println(json.toString(2));

        Element docEl = converter.toXML(json);

        XMLOutputter xmlOutputter = new XMLOutputter(Format.getPrettyFormat());

        Document doc2 = new Document();
        doc2.setRootElement(docEl);
        System.out.println("==============================================");
        xmlOutputter.output(doc2, System.out);
        BufferedWriter out = null;
        String reconstructedXmlFile = "/tmp/043dbb8c-66de-6897-e054-00144fdd4fa6_reconstructed.xml";
        try {
            out = Utils.newUTF8CharSetWriter(reconstructedXmlFile);
            xmlOutputter.output(doc2, out);
        } finally {
            Utils.close(out);
        }
        Source source = Input.fromURI(xmlSource).build();
        Source target = Input.fromFile(reconstructedXmlFile).build();

        /*
        DifferenceEngine diff = new DOMDifferenceEngine();

        diff.addDifferenceListener(new ComparisonListener() {
            public void comparisonPerformed(Comparison comparison, ComparisonResult outcome) {
                fail("found a difference: " + comparison);
            }
        });

        diff.compare(source, target);
        */
       // Diff d = DiffBuilder.compare(source).withTest(target).build();
       // assertFalse(d.hasDifferences());

    }
}
