package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.http.client.utils.URIBuilder;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.SlimJDOMFactory;
import org.jdom2.input.SAXBuilder;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 2/16/16.
 */
public class XMLIterator implements Iterator<Element> {
    List<Element> elements;
    Iterator<Element> iter;
    int curOffset = 0;
    private String docElName;
    private String ingestURL;
    private String limitParam;
    private int limitValue;
    private String offsetParam;
    private boolean sampleMode;
    private String topElName;

    public XMLIterator(String docElName, String ingestURL, String limitParam,
                       int limitValue, String offsetParam, boolean sampleMode, String totElName) throws Exception {
        this.docElName = docElName;
        this.ingestURL = ingestURL;
        this.limitParam = limitParam;
        this.limitValue = limitValue;
        this.offsetParam = offsetParam;
        this.sampleMode = sampleMode;
        this.topElName = totElName;
        getNextBatch(true);
    }

    void getNextBatch(boolean first) throws Exception {
        if (!first && sampleMode) {
            return;
        }
        System.out.println("getNextBatch");
        SAXBuilder builder = new SAXBuilder();
        builder.setJDOMFactory(new SlimJDOMFactory(true));

        URIBuilder uriBuilder = new URIBuilder(ingestURL);
        if (limitParam != null) {
            uriBuilder.addParameter(limitParam, String.valueOf(limitValue));
        }
        if (offsetParam != null && !first) {
            uriBuilder.addParameter(offsetParam, String.valueOf(curOffset));
        }
        String url = uriBuilder.build().toString();
        // String xmlContent = Utils.sendGetRequest(url);
        File xmlContentFile = Utils.getContentFromURL(url);
        System.out.println("got xmlContent:" + xmlContentFile);
        Document doc = builder.build(xmlContentFile);

        System.out.println("to XML");
        Element rootEl = doc.getRootElement();
        List<Namespace> namespacesInScope = rootEl.getNamespacesInScope();
        Map<String, Namespace> nsMap = new HashMap<String, Namespace>();
        for (Namespace ns : namespacesInScope) {
            nsMap.put(ns.getPrefix(), ns);
        }
        Element topEl;
        String topElPrefix = extractNamespacePrefix(topElName);

        if (topElPrefix != null) {
            if (rootEl.getName().equals(toLocalName(topElName))) {
                topEl = rootEl;
            } else {
                Assertion.assertNotNull(nsMap.get(topElPrefix));
                topEl = rootEl.getChild(toLocalName(topElName), nsMap.get(topElPrefix));
            }
        } else {
            if (rootEl.getName().equals(topElName)) {
                topEl = rootEl;
            } else {
                topEl = rootEl.getChild(topElName);
            }
        }
        Assertion.assertNotNull(topEl);
        String docElPrefix = extractNamespacePrefix(docElName);
        if (docElPrefix != null) {
            Assertion.assertNotNull(nsMap.get(docElPrefix));
            this.elements = topEl.getChildren(toLocalName(docElName), nsMap.get(docElPrefix));
        } else {
            this.elements = topEl.getChildren(docElName);
        }
        iter = elements.iterator();
        // remove temp file
        xmlContentFile.delete();
    }

    public static boolean hasNamespace(String elName) {
        return elName.indexOf(':') != -1;
    }

    public static String toLocalName(String elementName) {
        int idx = elementName.indexOf(':');
        if (idx != -1) {
            return elementName.substring(idx + 1);
        }
        return elementName;
    }

    public static String extractNamespacePrefix(String elementName) {
        int idx = elementName.indexOf(':');
        if (idx != -1) {
            return elementName.substring(0, idx);
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = iter.hasNext();
        if (!hasNext && offsetParam != null && limitParam != null) {
            if (elements.size() < limitValue) {
                return false;
            }
            curOffset += elements.size();
            try {
                getNextBatch(false);
                hasNext = iter.hasNext();
            } catch (Exception e) {
                NIFXMLIngestor.log.error("hasNext", e);
                e.printStackTrace();
                return false;
            }
        }
        return hasNext;
    }

    @Override
    public Element next() {
        return iter.next();
    }

    @Override
    public void remove() {
        // no op
    }
}
