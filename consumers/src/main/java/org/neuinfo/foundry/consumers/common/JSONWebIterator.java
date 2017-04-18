package org.neuinfo.foundry.consumers.common;

import org.apache.http.client.utils.URIBuilder;
import org.jdom2.input.SAXBuilder;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONPathProcessor2;
import org.neuinfo.foundry.common.util.Utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by bozyurt on 2/16/16.
 */
public class JSONWebIterator implements Iterator<JSONObject> {
    int curOffset = 0;
    List<JSONObject> elements;
    Iterator<JSONObject> iter;
    String ingestURL;
    String offsetParam;
    String totalParamJsonPath;
    String docElJsonPath;
    boolean sampleMode = false;
    int total = -1;

    public JSONWebIterator(String ingestURL, String offsetParam, String totalParamJsonPath,
                           String docElJsonPath, boolean sampleMode) throws Exception {
        this.ingestURL = ingestURL;
        this.offsetParam = offsetParam;
        this.totalParamJsonPath = totalParamJsonPath;
        this.docElJsonPath = docElJsonPath;
        this.sampleMode = sampleMode;
        getNextBatch(true);
    }

    void getNextBatch(boolean first) throws Exception {
        URIBuilder uriBuilder = new URIBuilder(ingestURL);
        if (offsetParam != null && !first) {
            uriBuilder.addParameter(offsetParam, String.valueOf(curOffset));
        }
        String url = uriBuilder.build().toString();
        String content = null;
        // retry upto three times after waiting in between
        for(int i = 0; i < 3; i++) {
            try {
                content = Utils.sendGetRequest(url);
            } catch (Utils.ServiceUnavailableException sue) {
                System.out.println(sue.getMessage());
                try {
                    Thread.sleep((i + 1) * 2000);
                } catch(InterruptedException e) {
                    // no op
                }
            }
        }
        if (content == null) {
            return;
        }
        JSONObject json = new JSONObject(content);
        JSONPathProcessor2 pathProcessor2 = new JSONPathProcessor2();
        List<JSONPathProcessor2.JPNode> jpNodes = pathProcessor2.find(totalParamJsonPath, json);
        Assertion.assertTrue(jpNodes != null && jpNodes.size() == 1);
        this.total = Utils.getIntValue(jpNodes.get(0).getPayload().toString(), -1);

        List<JSONPathProcessor2.JPNode> docJsonNodes = pathProcessor2.find(docElJsonPath, json);
        elements = new ArrayList<JSONObject>(docJsonNodes.size());
        for (JSONPathProcessor2.JPNode jpNode : docJsonNodes) {
            if (jpNode.getPayload() instanceof JSONObject) {
                elements.add((JSONObject) jpNode.getPayload());
            } else {
                JSONObject js = new JSONObject();
                js.put("value", jpNode.getValue());
                elements.add(js);
            }
        }
        iter = elements.iterator();
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = iter.hasNext();
        if (!hasNext && offsetParam != null) {
            if (curOffset >= total) {
                return false;
            }
            curOffset += elements.size();
            try {
                getNextBatch(false);
                hasNext = iter.hasNext();
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return hasNext;
    }

    @Override
    public JSONObject next() {
        return iter.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
