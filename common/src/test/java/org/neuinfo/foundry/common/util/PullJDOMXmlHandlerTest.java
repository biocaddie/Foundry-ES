package org.neuinfo.foundry.common.util;

import junit.framework.TestCase;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

/**
 * Created by bozyurt on 11/6/14.
 */
public class PullJDOMXmlHandlerTest extends TestCase {

    public void testDryadParsing() throws Exception {
        String xmlFile = "/tmp/DryadDataRepository/DryadDataRepository_hdl_10255_dryad.148_11-06-2014.xml";
        PullJDOMXmlHandler xmlHandler = null;

        try {
            xmlHandler = new PullJDOMXmlHandler(xmlFile);
            Element el = xmlHandler.nextElementStart();
            assertNotNull(el);
            System.out.println("el:" + el.getName());
            while( (el = xmlHandler.nextElement("record")) != null ) {
                System.out.println( toXmlString(el));
            }
        } finally {
            xmlHandler.shutdown();
        }
    }

    public static String toXmlString(Element el) {
        XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
        return xout.outputString(el);
    }
}
