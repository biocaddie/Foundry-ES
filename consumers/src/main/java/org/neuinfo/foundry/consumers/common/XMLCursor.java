package org.neuinfo.foundry.consumers.common;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.SlimJDOMFactory;
import org.jdom2.input.SAXBuilder;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONPathProcessor;
import org.neuinfo.foundry.common.util.JSONPathProcessor2;
import org.neuinfo.foundry.common.util.XML2JSONConverter;
import org.neuinfo.foundry.consumers.plugin.IngestorIterable;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.File;
import java.util.*;

/**
 * Created by bozyurt on 2/27/17.
 */
public class XMLCursor implements IngestorIterable, Joinable {
    String topElName;
    String docElName;
    File sourceFile;
    String alias;
    String resetJsonPath;
    String joinValueJsonPath;
    List<JSONObject> records;
    JSONObject curRecord;
    Iterator<JSONObject> iterator;
    private JSONPathProcessor.Path path;

    public XMLCursor(String alias, File sourceFile) {
        this.alias = alias;
        this.sourceFile = sourceFile;
    }

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.topElName = options.get("topElement");
        this.docElName = options.get("documentElement");
    }

    @Override
    public void startup() throws Exception {
        SAXBuilder builder = new SAXBuilder();
        builder.setJDOMFactory(new SlimJDOMFactory(true));
        Document doc = builder.build(sourceFile.getAbsoluteFile());
        Element rootEl = doc.getRootElement();
        Element topEl;
        if (rootEl.getName().equals(topElName)) {
            topEl = rootEl;
        } else {
            topEl = rootEl.getChild(topElName);
        }
        Assertion.assertNotNull(topEl);
        List<Element> elements = topEl.getChildren(docElName);
        this.records = new ArrayList<JSONObject>(elements.size());
        XML2JSONConverter converter = new XML2JSONConverter();
        for (Element recEl : elements) {
            JSONObject json = converter.toJSON(recEl);
            this.records.add(json);
        }
        iterator = this.records.iterator();
    }

    @Override
    public Result prepPayload() {
        try {
            JSONObject json = this.iterator.next();
            return new Result(json, Result.Status.OK_WITH_CHANGE);
        } catch (Throwable t) {
            t.printStackTrace();
            return new Result(null, Result.Status.ERROR, t.getMessage());
        }
    }


    @Override
    public boolean hasNext() {
        return iterator != null && iterator.hasNext();
    }

    @Override
    public void reset(String refValue) throws Exception {
        if (refValue == null) {
            if (this.iterator == null) {
                this.iterator = records.iterator();
            }
        } else {
            List<JSONObject> list = CursorUtils.filter(refValue, this.resetJsonPath, this.records);
            this.iterator = list.iterator();
        }

    }

    @Override
    public JSONObject next() {
        this.curRecord = this.iterator.next();
        return this.curRecord;
    }

    @Override
    public JSONObject peek() {
        return this.curRecord;
    }

    @Override
    public String getJoinValue() {
        return CursorUtils.extractStringValue(this.curRecord, path);
    }

    @Override
    public String getAlias() {
        return this.alias;
    }

    @Override
    public void setResetJsonPath(String resetJsonPath) {
        this.resetJsonPath = resetJsonPath;
    }

    @Override
    public void setJoinValueJsonPath(String joinValueJsonPath) throws Exception {
        this.joinValueJsonPath = joinValueJsonPath;
        JSONPathProcessor2 processor = new JSONPathProcessor2();
        this.path = processor.compile(joinValueJsonPath);
    }
}
