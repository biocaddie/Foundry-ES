package org.neuinfo.foundry.consumers.common;

import org.jdom2.Element;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.ColumnMeta;
import org.neuinfo.foundry.common.util.JSONPathProcessor;
import org.neuinfo.foundry.common.util.JSONPathProcessor2;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.common.util.XML2JSONConverter;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by bozyurt on 2/19/17.
 */
public class WebTable implements IJoinIterable<JSONObject> {
    String alias;
    WebTableMeta wtm;
    JSONObject curRecord;
    List<JSONObject> records;
    Iterator<JSONObject> recordsIterator;
    String resetJsonPath;
    String joinValueJsonPath;
    String columnMetaJsonPath;
    private JSONPathProcessor.Path path;

    public WebTable(WebTableMeta wtm, String resetJsonPath, String joinValueJsonPath, String columnMetaJsonPath) throws Exception {
        this.wtm = wtm;
        this.alias = wtm.getAlias();
        this.resetJsonPath = resetJsonPath;
        this.joinValueJsonPath = joinValueJsonPath;
        this.columnMetaJsonPath = columnMetaJsonPath;
    }

    public void startup(File xmlFile, String recordJsonPath) throws Exception {
        Element rootEl = Utils.loadXML(xmlFile.getAbsolutePath());
        XML2JSONConverter converter = new XML2JSONConverter();
        JSONObject json = converter.toJSON(rootEl);
        JSONPathProcessor2 processor = new JSONPathProcessor2();
        List<JSONPathProcessor2.JPNode> list = processor.find(recordJsonPath, json);
        records = new ArrayList<JSONObject>(list.size());
        for (JSONPathProcessor2.JPNode jpNode : list) {
            WebJoinIterator.handlePayload(records, jpNode);
        }
        if (columnMetaJsonPath != null) {
            processor = new JSONPathProcessor2();
            List<JSONPathProcessor2.JPNode> cmJPList = processor.find(columnMetaJsonPath, json);
            List<ColumnMeta> cmList = new ArrayList<ColumnMeta>(cmJPList.size());
            for (JSONPathProcessor2.JPNode jpNode : cmJPList) {
                if (jpNode.getPayload() instanceof String) {
                    cmList.add(new ColumnMeta((String) jpNode.getPayload(), "string"));
                }
            }
            if (!cmList.isEmpty()) {
                wtm.setCmList(cmList);
            }
        }
        processor = new JSONPathProcessor2();
        path = processor.compile(joinValueJsonPath);
    }

    @Override
    public void reset(String refValue) throws Exception {
        if (refValue == null) {
            if (this.recordsIterator == null) {
                this.recordsIterator = records.iterator();
            }
        } else {

            JSONPathProcessor2 processor = new JSONPathProcessor2();
            JSONPathProcessor.Path path = processor.compile(resetJsonPath);
            List<JSONObject> list = new ArrayList<JSONObject>();
            for (JSONObject json : records) {
                List<JSONPathProcessor2.JPNode> jpNodes = processor.find(path, json);
                if (jpNodes != null && jpNodes.size() == 1) {
                    String value = jpNodes.get(0).getValue();
                    if (value.equals(refValue)) {
                        list.add(json);
                    }
                }
            }
            this.recordsIterator = list.iterator();
        }
    }


    @Override
    public boolean hasNext() {
        return this.recordsIterator != null && this.recordsIterator.hasNext();
    }

    @Override
    public JSONObject next() {
        this.curRecord = this.recordsIterator.next();
        return this.curRecord;
    }

    @Override
    public JSONObject peek() {
        return this.curRecord;
    }

    @Override
    public String getJoinValue() {
        if (this.curRecord == null) {
            return null;
        }
        JSONPathProcessor2 processor = new JSONPathProcessor2();
        List<JSONPathProcessor2.JPNode> jpNodes = processor.find(path, curRecord);
        if (jpNodes != null) {
            JSONPathProcessor2.JPNode jpNode = jpNodes.get(0);
            return jpNode.getValue();
        }
        return null;
    }

    @Override
    public String getAlias() {
        return this.alias;
    }


}
