package org.neuinfo.foundry.common.util;

import org.jdom2.Element;
import org.jdom2.Namespace;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/**
 * Created by bozyurt on 11/5/14.
 */
public class PullJDOMXmlHandler {
    protected XmlPullParser pullParser;
    protected BufferedReader in;
    protected boolean inTagEnd = false;
    protected Stack<String> tagStack = new Stack<String>();
    protected Map<String, Namespace> nsMap = new HashMap<String, Namespace>();

    public PullJDOMXmlHandler(String xmlFile) throws XmlPullParserException, IOException {
        XmlPullParserFactory factory = XmlPullParserFactory.newInstance();
        factory.setNamespaceAware(false);
        this.pullParser = factory.newPullParser();
        in = Utils.newUTF8CharSetReader(xmlFile);
        pullParser.setInput(in);
    }

    public void shutdown() {
        Utils.close(in);
    }


    public Element nextElementStart() throws XmlPullParserException, IOException {
        int eventType = pullParser.getEventType();
        boolean inElemStart = false;
        Element el = null;
        do {
            if (eventType == XmlPullParser.START_TAG) {
                String name = pullParser.getName();
                tagStack.push(name);
                el = prepElemStart(name);
                inElemStart = true;
            }
            if (eventType != XmlPullParser.END_DOCUMENT) {
                eventType = pullParser.next();
            }
        } while (!inElemStart && eventType != XmlPullParser.END_DOCUMENT);
        if (eventType == XmlPullParser.END_DOCUMENT) {
            return null;
        }
        return el;
    }

    public Element nextElement(String tagName) throws XmlPullParserException, IOException {
        Stack<Element> elemStack = new Stack<Element>();
        Element el = null;
        inTagEnd = false;
        int eventType = pullParser.next();
        if (!pullParser.getName().equals(tagName)) {
            return null;
        }
        boolean first = true;
        do {
            if (eventType == XmlPullParser.START_TAG) {
                if (first && !pullParser.getName().equals(tagName)) {
                    throw new XmlPullParserException("not at the start of tag:"
                            + tagName);
                }
                if (first) {
                    first = false;
                    el = prepElemStart(pullParser.getName());
                    elemStack.push(el);
                } else {
                    processStartElement(elemStack);
                }
            } else if (eventType == XmlPullParser.TEXT) {
                processText(elemStack);
            } else if (eventType == XmlPullParser.END_TAG) {
                String name = pullParser.getName();
                if (name.equals(tagName)) {
                    inTagEnd = true;
                } else {
                    elemStack.pop();
                }
            }
            eventType = pullParser.next();

        } while (!inTagEnd && eventType != XmlPullParser.END_DOCUMENT);
        if (eventType == XmlPullParser.END_DOCUMENT) {
            return null;
        }
        return el;
    }

    Namespace findNamespace(String prefix) {
        Namespace ns = nsMap.get(prefix);
        if (ns == null) {
            int numAttr = pullParser.getAttributeCount();
            for (int i = 0; i < numAttr; i++) {
                String attrName = pullParser.getAttributeName(i);
                if (attrName.startsWith("xmlns")) {
                    int idx = attrName.indexOf(':');
                    String pf = "";
                    if (idx != -1) {
                        pf = attrName.substring(idx + 1);
                    }
                    if (pf.equals(prefix)) {
                        String url = pullParser.getAttributeValue(i);
                        ns = Namespace.getNamespace(prefix, url);
                        nsMap.put(prefix, ns);
                    }
                }
            }
        }
        return ns;
    }

    Element createElementWithNamespace(String name) {
        Element el;
        int numAttr = pullParser.getAttributeCount();
        for (int i = 0; i < numAttr; i++) {
            String attrName = pullParser.getAttributeName(i);
            if (attrName.startsWith("xmlns")) {
                int idx = attrName.indexOf(':');
                String pf = "";
                if (idx != -1) {
                    pf = attrName.substring(idx + 1);
                }
                Namespace ns = nsMap.get(pf);
                if (ns == null) {
                    String url = pullParser.getAttributeValue(i);
                    ns = Namespace.getNamespace(pf, url);
                    nsMap.put(pf, ns);
                }
            }
        }

        int idx = name.indexOf(':');
        if (idx != -1)
        {
            String localName = name.substring(idx + 1);
            String prefix = name.substring(0, idx);
            Namespace ns = nsMap.get(prefix);
            Assertion.assertNotNull(ns);
            el = new Element(localName);
            el.setNamespace(ns);
        } else {
            el = new Element(name);
            Namespace ns = nsMap.get("");
            if (ns != null) {
                el.setNamespace(ns);
            }
        }
        return el;
    }

    protected Element prepElemStart(String name) {
        Element el = createElementWithNamespace(name);
        int numAttr = pullParser.getAttributeCount();

        for (int i = 0; i < numAttr; i++) {
            String attrName = pullParser.getAttributeName(i);

            if (attrName.startsWith("xmlns")) {
                int idx = attrName.indexOf(':');
                String prefix = "";
                if (idx != -1) {
                    prefix = attrName.substring(idx + 1);
                }
                Namespace ns = findNamespace(prefix);
                if (ns == null) {
                    String url = pullParser.getAttributeValue(i);
                    ns = Namespace.getNamespace(prefix, url);
                    nsMap.put(prefix, ns);
                }
                if (el.getNamespace(prefix) != null) {
                    el.addNamespaceDeclaration(ns);
                }
            } else if (attrName.startsWith("xsi:")) {
                // just ignore
                continue;
            } else {
                el.setAttribute(attrName, pullParser.getAttributeValue(i));
            }
        }
        return el;
    }

    protected void processStartElement(Stack<Element> elemStack) {
        String name = pullParser.getName();
        int idx = name.indexOf(':');

        Element elem = prepElemStart(name);
        Element parentEl = elemStack.peek();
        parentEl.addContent(elem);
        elemStack.push(elem);
    }

    protected void processText(Stack<Element> elemStack) {
        String content = pullParser.getText().trim();
        if (content.length() > 0) {
            Element elem = elemStack.peek();
            elem.addContent(content);
        }
    }
}
