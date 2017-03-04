package org.neuinfo.foundry.common.util;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.SlimJDOMFactory;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by bozyurt on 11/6/14.
 */
public class PullJDOMXmlHandlerTest {

    @Test
    public void testDryadParsing() throws Exception {
        String relPath = "testdata/DryadDataRepository_hdl_10255_dryad.7872_04-07-2016.xml";
        String xmlFile = "/tmp/" + new File(relPath).getName();
        Utils.stream2File(relPath, xmlFile);
        PullJDOMXmlHandler xmlHandler = null;

        try {
            xmlHandler = new PullJDOMXmlHandler(xmlFile);
            Element el = xmlHandler.nextElementStart();
            assertNotNull(el);
            System.out.println("el:" + el.getName());
            while ((el = xmlHandler.nextElement("record")) != null) {
                System.out.println(toXmlString(el));
            }
        } finally {
            xmlHandler.shutdown();
            new File(xmlFile).delete();
        }
    }

    public static String toXmlString(Element el) {
        XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
        return xout.outputString(el);
    }

    public void testRDFParsing() throws Exception{
        File xmlContentFile = new File("/home/bozyurt/biositemap_NCBC_Simbios.rdf");
        SAXBuilder builder = new SAXBuilder();
        builder.setJDOMFactory(new SlimJDOMFactory(true));
        Document doc = builder.build(xmlContentFile);
        Element rootEl = doc.getRootElement();
        List<Namespace> namespacesInScope = rootEl.getNamespacesInScope();
        Map<String, Namespace> nsMap = new HashMap<String, Namespace>();
        for(Namespace ns : namespacesInScope) {
            nsMap.put(ns.getPrefix(), ns);
        }
        List<Element> children = rootEl.getChildren();
        for(Element el : children) {
            System.out.println(el.getName());
        }


        children = rootEl.getChildren("Resource_Description", nsMap.get("desc"));
        assertFalse(children.isEmpty());
    }
}
