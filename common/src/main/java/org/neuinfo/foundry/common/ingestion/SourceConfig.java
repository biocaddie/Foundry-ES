package org.neuinfo.foundry.common.ingestion;

import org.jdom2.Element;
import org.neuinfo.foundry.common.util.Assertion;

import java.io.File;

/**
 * Created by bozyurt on 6/4/14.
 * TODO remove this not used anymore (11/20/2014)
 */
public class SourceConfig {
    private String nifId;
    private String path;
    private String rootEl;
    private String docEl;


    public String getNifId() {
        return nifId;
    }

    public void setNifId(String nifId) {
        this.nifId = nifId;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getRootEl() {
        return rootEl;
    }

    public void setRootEl(String rootEl) {
        this.rootEl = rootEl;
    }

    public String getDocEl() {
        return docEl;
    }

    public void setDocEl(String docEl) {
        this.docEl = docEl;
    }

    public static SourceConfig fromXML(Element el) {
        SourceConfig sc = new SourceConfig();
        sc.setNifId( el.getAttributeValue("nifId"));
        final Element child = el.getChild("xml-file");
        sc.setPath( child.getAttributeValue("path"));
        sc.setRootEl( child.getAttributeValue("rootEl"));
        sc.setDocEl( child.getAttributeValue("docEl"));
        // Assertion.assertTrue( new File(sc.getPath()).isFile());
        return sc;
    }
}
