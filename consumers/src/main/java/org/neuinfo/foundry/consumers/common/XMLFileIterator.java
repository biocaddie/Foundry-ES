package org.neuinfo.foundry.consumers.common;

import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.CharSetEncoding;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.RemoteFileIterator;

import java.io.File;
import java.util.Iterator;

/**
 * Created by bozyurt on 10/6/15.
 */
public class XMLFileIterator implements Iterator<Element> {
    String topElement;
    String docElement;
    RemoteFileIterator xmlFileIterator;
    Pull2JDOMXmlHandler handler;
    Element curElement = null;
    File extractedFile;
    File curFile;
    boolean empty = false;
    static Logger logger = Logger.getLogger(XMLFileIterator.class);

    public XMLFileIterator(RemoteFileIterator fileIterator, String topElement, String docElement)
            throws Exception {
        Assertion.assertNotNull(docElement);
        this.topElement = topElement;
        this.docElement = docElement;
        xmlFileIterator = fileIterator;
        if (xmlFileIterator.hasNext()) {
            prepHandler();
        } else {
            empty = true;
        }
    }

    private void prepHandler() throws Exception {
        this.curFile = xmlFileIterator.next();
        String xmlFilePath = curFile.getAbsolutePath();
        System.out.println("curFile:" + xmlFilePath);
        extractedFile = null;
        if (curFile.getName().endsWith(".gz")) {
            extractedFile = new File(xmlFilePath.replaceFirst("\\.gz$", ""));
            Utils.extractGzippedFile(xmlFilePath, extractedFile);
            Assertion.assertTrue(extractedFile.isFile());
            xmlFilePath = extractedFile.getAbsolutePath();
        }
        if (logger.isDebugEnabled()) {
            logger.debug("handling " + xmlFilePath);
        }
        handler = new Pull2JDOMXmlHandler(xmlFilePath, CharSetEncoding.UTF8);
        if (!Utils.isEmpty(this.topElement)) {
            Element el;
            do {
                el = handler.nextElementStart();
                if (el != null && !el.getName().equals(this.topElement)) {
                    handler.advance();
                }
            } while (el != null && !el.getName().equals(this.topElement));
            if (el == null) {
                empty = true;
            }
        }
    }

    public File getCurFile() {
        return this.curFile;
    }

    private void cleanup() {
        if (handler != null) {
            handler.shutdown();
        }
        if (this.extractedFile != null) {
            this.extractedFile.delete();
            this.extractedFile = null;
        }
    }

    @Override
    public boolean hasNext() {
        if (empty) {
            return false;
        }
        try {
            curElement = handler.nextElement(this.docElement);
            if (curElement == null) {
                if (xmlFileIterator.hasNext()) {
                    cleanup();
                    prepHandler();
                    curElement = handler.nextElement(this.docElement);
                    return curElement != null;
                } else {
                    cleanup();
                    return false;
                }
            } else {
                return true;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Element next() {
        return curElement;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public String[] getAddedFieldsForCurrentSourceRec() {
        return xmlFileIterator.getAddedFieldsForCurrentSourceRec();
    }
}
