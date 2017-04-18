package org.neuinfo.foundry.common.transform;

import org.jdom2.*;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.neuinfo.foundry.common.util.Assertion;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 4/24/15.
 */
public class XpathFieldExtractor {
    Map<String, Namespace> nsMap = new HashMap<String, Namespace>();
    XPathFactory xpathFactory;

    public XpathFieldExtractor(List<Namespace> nsList) {
        for (Namespace ns : nsList) {
            nsMap.put(ns.getPrefix(), ns);
        }
        xpathFactory = XPathFactory.instance();
    }


    public static String extractPrefix(String xpathStr, int idx) {
        StringBuilder sb = new StringBuilder();
        int i = idx - 1;
        while (i >= 0) {
            char c = xpathStr.charAt(i);
            if (c == '/') {
                break;
            }
            sb.append(c);
            i--;
        }
        return sb.reverse().toString();

    }

    public void extractValue(Element el, String xpathStr, Map<String, List<String>> nameValueMap) {
        XPathExpression<Element> expr = null;
        XPathExpression<Attribute> attrExpr = null;
        XPathExpression<Text> textExpr = null;
        int idx = xpathStr.lastIndexOf(':');
        if (idx != -1) {
            Namespace ns;
            String prefix = extractPrefix(xpathStr, idx);
            ns = nsMap.get(prefix);
            Assertion.assertNotNull(ns);
            int attrIdx = xpathStr.lastIndexOf('@');
            if (attrIdx != -1) {
                boolean last = xpathStr.substring(attrIdx + 1).lastIndexOf('/') == -1;
                if (last) {
                    attrExpr = xpathFactory.compile(xpathStr, Filters.attribute(), null, ns);
                } else {
                    expr = xpathFactory.compile(xpathStr, Filters.element(ns), null, ns);
                }
            } else {
                expr = xpathFactory.compile(xpathStr, Filters.element(ns), null, ns);
            }
        } else {
            int attrIdx = xpathStr.lastIndexOf('@');
            if (attrIdx != -1) {
                boolean last = xpathStr.substring(attrIdx + 1).lastIndexOf('/') == -1;
                if (last) {
                    attrExpr = xpathFactory.compile(xpathStr, Filters.attribute());
                } else {
                    expr = xpathFactory.compile(xpathStr, Filters.element());
                }
            } else {
                if (xpathStr.endsWith("text()")) {
                    textExpr = xpathFactory.compile(xpathStr, Filters.text());
                } else {
                    expr = xpathFactory.compile(xpathStr, Filters.element());
                }
            }
        }
        if (expr != null) {
            List<Element> elements = expr.evaluate(el);
            if (elements != null && !elements.isEmpty()) {
                List<String> values = new ArrayList<String>(elements.size());
                for (Element elem : elements) {
                    values.add(elem.getTextTrim());
                    System.out.println(elem.getTextTrim());
                }
                nameValueMap.put(xpathStr, values);
            }
        } else if (textExpr != null) {
            List<Text> texts = textExpr.evaluate(el);
            if (texts != null && !texts.isEmpty()) {
                List<String> values = new ArrayList<String>(texts.size());
                for(Text text : texts) {
                    values.add(text.getTextTrim());
                }
                nameValueMap.put(xpathStr, values);
            }
        } else {
            List<Attribute> attrs = attrExpr.evaluate(el);
            if (attrs != null && !attrs.isEmpty()) {
                List<String> values = new ArrayList<String>(attrs.size());
                for (Attribute attr : attrs) {
                    values.add(attr.getValue());
                    System.out.println(attr.getValue());
                }
                nameValueMap.put(xpathStr, values);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        SAXBuilder builder = new SAXBuilder();
        //Document doc = builder.build(new File("/tmp/pdb/100d.xml"));
        //Document doc = builder.build(new File("/tmp/pdb_ftp/01/101d.xml"));
        Document doc = builder.build(new File("/tmp/projects.xml"));
        Element rootEl = doc.getRootElement();

        XpathFieldExtractor extractor = new XpathFieldExtractor(rootEl.getNamespacesInScope());
        Map<String, List<String>> nameValueMap = new HashMap<String, List<String>>(7);

        //extractor.extractValue(rootEl, "//PDBx:audit_author/PDBx:name", nameValueMap);
        //extractor.extractValue(rootEl, "//PDBx:citation_author[@citation_id='primary']/@name", nameValueMap);
        // extractor.extractValue(rootEl, "//PDBx:citation[@id='primary']/PDBx:title", nameValueMap);
        // extractor.extractValue(rootEl, "//PDBx:citation[@id='primary']/PDBx:year", nameValueMap);
        extractor.extractValue(rootEl, "//columns/column/text()", nameValueMap);
        System.out.println(nameValueMap);

    }
}
