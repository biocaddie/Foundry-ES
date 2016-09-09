package org.neuinfo.foundry.consumers.common;

import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.neuinfo.foundry.common.util.CharSetEncoding;
import org.neuinfo.foundry.common.util.MutableInt;
import org.neuinfo.foundry.common.util.Utils;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/**
 * Created by bozyurt on 10/7/15.
 */
public class Pull2JDOMXmlHandler {
    protected XmlPullParser pullParser;
    protected BufferedReader in;
    protected boolean inTagEnd = false;
    protected boolean ignoreNamespaces = false;
    protected Stack<String> tagStack = new Stack<String>();
    protected Map<String, Namespace> nsMap = new HashMap<String, Namespace>(7);
    Map<String, MutableInt> tagCountMap = new HashMap<String, MutableInt>();
    static Logger log = Logger.getLogger(Pull2JDOMXmlHandler.class);

    public Pull2JDOMXmlHandler(String xmlFile, CharSetEncoding csEncoding)
            throws XmlPullParserException, IOException {
        XmlPullParserFactory factory = XmlPullParserFactory.newInstance();
        factory.setNamespaceAware(true);
        pullParser = factory.newPullParser();

        if (csEncoding == CharSetEncoding.UTF8) {
            in = Utils.newUTF8CharSetReader(xmlFile);
            pullParser.setInput(in);
        } else {
            throw new RuntimeException("Only UTF8 encoding is supported!");
        }
    }

    public void shutdown() {
        if (in != null) {
            Utils.close(in);
            in = null;
        }
    }


    public Element nextElementStart() throws XmlPullParserException,
            IOException {
        int eventType = pullParser.getEventType();
        boolean inElemStart = false;
        Element el = null;
        do {
            if (eventType == XmlPullParser.START_TAG) {
                String name = pullParser.getName();
                tagStack.push(name);
                el = prepElemStart(name, pullParser.getNamespace(), pullParser.getPrefix());
                inElemStart = true;
            }
            if (!inElemStart && eventType != XmlPullParser.END_DOCUMENT) {
                eventType = pullParser.next();
            }
        } while (!inElemStart && eventType != XmlPullParser.END_DOCUMENT);
        if (eventType == XmlPullParser.END_DOCUMENT) {
            return null;
        }

        return el;
    }

    public void advance() throws IOException, XmlPullParserException {
        pullParser.next();
    }

    public Element nextElement(String tagName) throws XmlPullParserException,
            IOException {
        int eventType;
        Stack<Element> elemStack = new Stack<Element>();
        Element el = null;
        inTagEnd = false;

        eventType = pullParser.next();
        while (eventType != XmlPullParser.START_TAG) {
            if (eventType == XmlPullParser.END_DOCUMENT) {
                return null;
            }
            eventType = pullParser.next();
        }
        System.out.println(pullParser.getName());
        if (!pullParser.getName().equals(tagName)) {
            return null;
        }
        boolean first = true;
        do {
            // System.out.println("ns: " + pullParser.getNamespace());
            if (eventType == XmlPullParser.START_TAG) {
                if (first && !pullParser.getName().equals(tagName)) {
                    throw new XmlPullParserException("not at the start of tag:"
                            + tagName);
                }
                if (first) {
                    first = false;

                    el = prepElemStart(pullParser.getName(), pullParser.getNamespace(), pullParser.getPrefix());
                    elemStack.push(el);
                } else {
                    processStartElement(elemStack, pullParser.getNamespace(), pullParser.getPrefix());
                }
            } else if (eventType == XmlPullParser.TEXT) {
                processText(elemStack);
            } else if (eventType == XmlPullParser.END_TAG) {
                String name = pullParser.getName();
                if (name.equals(tagName)) {
                    inTagEnd = true;
                    break;
                } else {
                    elemStack.pop();
                }
            }
            eventType = pullParser.next();
        } while (!inTagEnd && eventType != XmlPullParser.END_DOCUMENT);
        if (eventType == XmlPullParser.END_DOCUMENT) {
            return null;
        }
        // showCounts();
        return el;
    }

    Namespace getNamespace(String namespace, String prefix) {
        Namespace ns = nsMap.get(namespace);
        if (ns == null) {
            ns = Namespace.getNamespace(prefix, namespace);
            nsMap.put(namespace, ns);
        }
        return ns;
    }

    void incrTagCount(String tagName) {
        MutableInt counter = this.tagCountMap.get(tagName);
        if (counter == null) {
            counter = new MutableInt(0);
            this.tagCountMap.put(tagName, counter);
        }
        counter.incr();
    }

    public void showCounts() {
        System.out.println("Tag stats\n===============");
        for (String tag : tagCountMap.keySet()) {
            MutableInt counter = tagCountMap.get(tag);
            System.out.println(tag + " : " + counter.getValue());
        }
    }

    protected Element prepElemStart(String name, String namespace, String nsPrefix) {
        Element el;
        if (ignoreNamespaces) {
            int idx = name.indexOf(':');
            if (idx != -1) {
                name = name.substring(idx + 1);
            }
        }
        incrTagCount(name);
        if (namespace != null) {
            Namespace ns = getNamespace(namespace, nsPrefix);
            el = new Element(name, ns);
        } else {
            el = new Element(name);
        }
        int numAttr = pullParser.getAttributeCount();
        for (int i = 0; i < numAttr; i++) {
            String attrName = pullParser.getAttributeName(i);
            if (ignoreNamespaces) {
                int idx = attrName.indexOf(':');
                if (idx != -1) {
                    attrName = attrName.substring(idx + 1);
                }
            }
            el.setAttribute(attrName,
                    pullParser.getAttributeValue(i));
        }
        return el;
    }

    protected void processStartElement(Stack<Element> elemStack, String namespace, String nsPrefix) {
        String name = pullParser.getName();
        Element elem = prepElemStart(name, namespace, nsPrefix);
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
