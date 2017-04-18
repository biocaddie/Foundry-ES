package org.neuinfo.foundry.consumers.common;

import org.jdom2.Element;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.*;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.ContentLoader;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.IngestorHelper;

import java.io.File;
import java.util.*;

/**
 * Created by bozyurt on 2/17/17.
 */
public class OnDemandWebTable implements IJoinIterable<JSONObject> {
    String alias;
    String fieldJsonPath;
    WebTableMeta wtm;
    String username;
    String password;
    JSONObject currentRec;
    JSONPathProcessor.Path path;
    Iterator<JSONObject> recordIterator;
    String resetJsonPath;
    String joinValueJsonPath;

    public OnDemandWebTable(WebTableMeta wtm, String username, String password,
                            String resetJsonPath, String joinValueJsonPath) throws Exception {
        this.wtm = wtm;
        this.alias = wtm.getAlias();
        this.fieldJsonPath = wtm.getFieldJsonPath();
        this.username = username;
        this.password = password;
        this.resetJsonPath = resetJsonPath;
        this.joinValueJsonPath = joinValueJsonPath;
        if (this.fieldJsonPath != null) {
            JSONPathProcessor2 processor = new JSONPathProcessor2();
            path = processor.compile(joinValueJsonPath);
        }
    }

    @Override
    public void reset(String refValue) throws Exception {
        List<String> templateVariables = IngestorHelper.extractTemplateVariables(wtm.getUrl());
        Assertion.assertTrue(templateVariables != null && templateVariables.size() == 1);
        Map<String, String> templateVar2ValueMap = new HashMap<String, String>(7);
        templateVar2ValueMap.put(templateVariables.get(0), refValue);
        String url = IngestorHelper.createURL(wtm.url, templateVar2ValueMap);

        //FIXME XML format assumption
        File contentFile = ContentLoader.getContent(url, null, false, username, password);
        Element rootEl = Utils.loadXML(contentFile.getAbsolutePath());
        XML2JSONConverter converter = new XML2JSONConverter();
        JSONObject json = converter.toJSON(rootEl);
        if (wtm.getRecordJsonPath() == null) {
            // single record
            List<JSONObject> records = Arrays.asList(json);
            this.recordIterator = records.iterator();
        } else {
            JSONPathProcessor2 processor = new JSONPathProcessor2();
            List<JSONPathProcessor2.JPNode> list = processor.find(resetJsonPath, json);
            List<JSONObject> records = new ArrayList<JSONObject>(list.size());
            for (JSONPathProcessor2.JPNode jpNode : list) {
                WebJoinIterator.handlePayload(records, jpNode);
            }
            this.recordIterator = records.iterator();
        }
    }

    @Override
    public boolean hasNext() {
        return this.recordIterator != null && this.recordIterator.hasNext();
    }

    @Override
    public JSONObject next() {
        this.currentRec = this.recordIterator.next();
        return this.currentRec;
    }

    @Override
    public JSONObject peek() {
        return this.currentRec;
    }

    @Override
    public String getJoinValue() {
        if (currentRec == null) {
            return null;
        }
        return CursorUtils.extractStringValue(currentRec, path);
        /*
        if (joinValueJsonPath != null) {
            JSONPathProcessor2 processor = new JSONPathProcessor2();
            List<JSONPathProcessor2.JPNode> jpNodes = processor.find(path, currentRec);
            if (jpNodes != null) {
                JSONPathProcessor2.JPNode jpNode = jpNodes.get(0);
                return jpNode.getValue();
            }
        }
        return null;
        */
    }

    @Override
    public String getAlias() {
        return this.alias;
    }


}
