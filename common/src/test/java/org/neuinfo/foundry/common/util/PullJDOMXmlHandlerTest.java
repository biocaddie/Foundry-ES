package org.neuinfo.foundry.common.util;

import org.jdom2.Element;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertNotNull;

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
}
