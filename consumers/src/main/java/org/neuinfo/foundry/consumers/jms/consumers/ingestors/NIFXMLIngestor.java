package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.common.util.XML2JSONConverter;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by bozyurt on 10/28/14.
 * <p/>
 * FIXME needs maxDocs, testMode support
 */
public class NIFXMLIngestor implements Ingestor {
    String ingestURL;
    boolean allowDuplicates = false;
    String totElName;
    String docElName;
    String offsetParam;
    String limitParam;
    int limitValue = -1;
    boolean sampleMode = false;
    int sampleSize = 1;
    Iterator<Element> recordIterator;
    Map<String, String> optionMap;
    static Logger log = Logger.getLogger(NIFXMLIngestor.class);

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.ingestURL = options.get("ingestURL");
        this.allowDuplicates = options.containsKey("allowDuplicates") ?
                Boolean.parseBoolean(options.get("allowDuplicates")) : false;
        this.totElName = options.get("topElement");
        this.docElName = options.get("documentElement");
        if (options.containsKey("offsetParam")) {
            this.offsetParam = options.get("offsetParam");
        }
        if (options.containsKey("limitParam")) {
            this.limitParam = options.get("limitParam");
        }
        if (options.containsKey("limitValue")) {
            this.limitValue = Utils.getIntValue(options.get("limitValue"), -1);
        }
        this.sampleMode = options.containsKey("sampleMode")
                ? Boolean.parseBoolean(options.get("sampleMode")) : false;

        this.sampleSize = Utils.getIntValue(options.get("sampleSize"), 1);
        this.optionMap = options;
        Assertion.assertNotNull(this.ingestURL);
        Assertion.assertNotNull(this.totElName);
        Assertion.assertNotNull(this.docElName);
    }

    @Override
    public void startup() throws Exception {
        this.recordIterator = new XMLIterator(docElName, ingestURL, limitParam, limitValue, offsetParam, sampleMode, totElName);

    }

    public static String getXMLContent(String ingestURL) throws Exception {
        return Utils.sendGetRequest(ingestURL);
    }

    @Override
    public Result prepPayload() {
        try {
            Element el = this.recordIterator.next();
            XML2JSONConverter converter = new XML2JSONConverter();
            JSONObject json = converter.toJSON(el);
            return new Result(json, Result.Status.OK_WITH_CHANGE);
        } catch (Throwable t) {
            log.error("prepPayload", t);
            t.printStackTrace();
            return new Result(null, Result.Status.ERROR, t.getMessage());
        }

    }

    @Override
    public String getName() {
        return "NIFXMLIngestor";
    }

    @Override
    public int getNumRecords() {
        return -1;
    }

    @Override
    public String getOption(String optionName) {
        return this.optionMap.get(optionName);
    }

    @Override
    public void shutdown() {
        // no op
    }

    @Override
    public boolean hasNext() {
        return this.recordIterator.hasNext();
    }

}
