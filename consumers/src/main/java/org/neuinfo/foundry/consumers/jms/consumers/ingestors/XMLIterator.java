package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.http.client.utils.URIBuilder;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.SlimJDOMFactory;
import org.jdom2.input.SAXBuilder;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;

import java.io.File;
import java.util.Iterator;
import java.util.List;

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
    private String totElName;

    public XMLIterator(String docElName, String ingestURL, String limitParam,
                       int limitValue, String offsetParam, boolean sampleMode, String totElName) throws Exception {
        this.docElName = docElName;
        this.ingestURL = ingestURL;
        this.limitParam = limitParam;
        this.limitValue = limitValue;
        this.offsetParam = offsetParam;
        this.sampleMode = sampleMode;
        this.totElName = totElName;
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
        Element topEl;
        if (rootEl.getName().equals(totElName)) {
            topEl = rootEl;
        } else {
            topEl = rootEl.getChild(totElName);
        }
        Assertion.assertNotNull(topEl);
        this.elements = topEl.getChildren(docElName);
        iter = elements.iterator();
        // remove temp file
        xmlContentFile.delete();
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
