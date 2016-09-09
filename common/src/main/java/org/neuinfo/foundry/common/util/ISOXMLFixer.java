package org.neuinfo.foundry.common.util;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;


/**
 * Utility functions to fix common ISO XML metadata problems
 * <p/>
 * Created by bozyurt on 3/13/15.
 */
public class ISOXMLFixer {
    static Namespace gco = Namespace.getNamespace("gco", "http://www.isotc211.org/2005/gco");
    static Namespace gmx = Namespace.getNamespace("gmx", "http://www.isotc211.org/2005/gmx");

    public static Element fixAnchorProblem(Element docEl) {
        XPathFactory factory = XPathFactory.instance();
        XPathExpression<Element> expr = factory.compile("//gmx:Anchor",
                Filters.element(), null, gmx);
        List<Element> elements = expr.evaluate(docEl);
        for (Element el : elements) {
            Element csEl = el.getChild("CharacterString", gco);
            if (csEl == null) {
                String text = el.getTextTrim();
                el.setText("");
                csEl = new Element("CharacterString", gco).setText(text);
                el.addContent(csEl);
            }
        }
        return docEl;
    }


    public static void main(String[] args) throws Exception {
        File f = new File("/tmp/gov.noaa.nodc:0000850.xml");
        SAXBuilder sb = new SAXBuilder();
        Document doc = sb.build(f);
        Element docEl = doc.getRootElement();
        docEl = ISOXMLFixer.fixAnchorProblem(docEl);
        Utils.saveXML(docEl, "/tmp/gov.noaa.nodc:0000850_fixed.xml");
    }
}
