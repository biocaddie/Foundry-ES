package org.neuinfo.foundry.consumers.common;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.SlimJDOMFactory;
import org.jdom2.input.SAXBuilder;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.ColumnMeta;
import org.neuinfo.foundry.common.transform.XpathFieldExtractor;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONPathProcessor;
import org.neuinfo.foundry.common.util.JSONPathProcessor2;
import org.neuinfo.foundry.common.util.XML2JSONConverter;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.ContentLoader;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.IngestorHelper;
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
    private String username;
    private String password;
    private String ingestURL;
    private String columnMetaXPath;
    private List<ColumnMeta> cmList;


    public XMLCursor(String alias, String username, String password, String ingestURL) {
        this.alias = alias;
        this.username = username;
        this.password = password;
        this.ingestURL = ingestURL;
    }

    public XMLCursor(String alias, File sourceFile) {
        this.alias = alias;
        this.sourceFile = sourceFile;
    }

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.topElName = options.get("topElement");
        this.docElName = options.get("documentElement");
        this.columnMetaXPath = options.get("columnMetaXPath");
    }

    @Override
    public void startup() throws Exception {
        if (ingestURL == null) {
            this.records = extractRecords(sourceFile);
            iterator = this.records.iterator();
        }
    }

    Element find(Element parentEl, String childName) {
        if (parentEl == null) {
            return null;
        }
        if (parentEl.getName().equals(childName)) {
            return parentEl;
        }
        Element child = parentEl.getChild(childName);
        if (child != null) {
            return child;
        }

        for (Element c : parentEl.getChildren()) {
            Element el = find(c, childName);
            if (el != null) {
                return el;
            }
        }
        return null;
    }

    List<JSONObject> extractRecords(File xmlFile) throws Exception {
        SAXBuilder builder = new SAXBuilder();
        builder.setJDOMFactory(new SlimJDOMFactory(true));
        Document doc = builder.build(xmlFile.getAbsoluteFile());
        Element rootEl = doc.getRootElement();
        if (topElName == null && docElName == null) {
            XML2JSONConverter converter = new XML2JSONConverter();
            JSONObject json = converter.toJSON(rootEl);
            return Arrays.asList(json);
        }

        Element topEl = find(rootEl, topElName);
        Assertion.assertNotNull(topEl);
        List<Element> elements = topEl.getChildren(docElName);
        ArrayList<JSONObject> list = new ArrayList<JSONObject>(elements.size());
        XML2JSONConverter converter = new XML2JSONConverter();
        for (Element recEl : elements) {
            JSONObject json = converter.toJSON(recEl);
            list.add(json);
        }
        // for column names external to the column data
        if (this.columnMetaXPath != null) {
            XpathFieldExtractor extractor = new XpathFieldExtractor(rootEl.getNamespacesInScope());
            Map<String, List<String>> nameValueMap = new HashMap<String, List<String>>(7);
            extractor.extractValue(rootEl, this.columnMetaXPath, nameValueMap);
            if (!nameValueMap.isEmpty()) {
                List<String> columnNames = nameValueMap.values().iterator().next();
                this.cmList = new ArrayList<ColumnMeta>(columnNames.size());
                for (String columnName : columnNames) {
                    this.cmList.add(new ColumnMeta(columnName, "string"));
                }
            }
        }
        return list;
    }

    @Override
    public Result prepPayload() {
        try {
            JSONObject json = this.iterator.next();
            json = CursorUtils.handleExternalColumnNames(json, this.cmList);
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
            if (ingestURL == null) {
                if (this.iterator == null) {
                    this.iterator = records.iterator();
                }
            } else {
                if (!IngestorHelper.isParametrized(ingestURL)) {
                    File contentFile = null;
                    try {
                        contentFile = ContentLoader.getContent(ingestURL, null, false, username, password);
                        this.records = extractRecords(contentFile);
                        this.iterator = records.iterator();
                    } finally {
                        if (contentFile != null) {
                            contentFile.delete();
                        }
                    }
                }
            }
        } else {
            if (ingestURL == null) {
                List<JSONObject> list = CursorUtils.filter(refValue, this.resetJsonPath, this.records);
                this.iterator = list.iterator();
            } else {
                List<String> templateVariables = IngestorHelper.extractTemplateVariables(ingestURL);
                Assertion.assertTrue(templateVariables != null && templateVariables.size() == 1);
                Map<String, String> templateVar2ValueMap = new HashMap<String, String>(7);
                templateVar2ValueMap.put(templateVariables.get(0), refValue);
                String url = IngestorHelper.createURL(ingestURL, templateVar2ValueMap);
                File contentFile = null;
                try {
                    contentFile = ContentLoader.getContent(url, null, false, username, password);
                    this.records = extractRecords(contentFile);
                    List<JSONObject> list = CursorUtils.filter(refValue, this.resetJsonPath, this.records);
                    this.iterator = list.iterator();
                } finally {
                    if (contentFile != null) {
                        contentFile.delete();
                    }
                }
            }
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
        if (path == null) {
            return null;
        }
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
        if (this.joinValueJsonPath != null) {
            JSONPathProcessor2 processor = new JSONPathProcessor2();
            this.path = processor.compile(joinValueJsonPath);
        }
    }

    @Override
    public List<ColumnMeta> getColumnMetaList() {
        return this.cmList;
    }
}
