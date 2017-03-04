package org.neuinfo.foundry.consumers.common;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.ColumnMeta;
import org.neuinfo.foundry.common.transform.XpathFieldExtractor;
import org.neuinfo.foundry.common.util.*;
import org.neuinfo.foundry.consumers.common.DoublyLinkedList.Node;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.ContentLoader;

import java.io.File;
import java.util.*;

import static org.neuinfo.foundry.common.util.JSONPathProcessor2.*;

/**
 * Created by bozyurt on 5/10/16.
 */
public class WebJoinIterator implements Iterator<JSONObject> {
    String username;
    String password;
    boolean useCache = false;
    JSONObject currentRecord;
    String columnMetaJsonPath;
    Map<String, WebTableMeta> wtmMap = new HashMap<String, WebTableMeta>();
    Map<String, String> joinRightPartMap = new HashMap<String, String>();
    List<WebTableMeta> webTableMetaList = new LinkedList<WebTableMeta>();
    Map<String, IJoinIterable<JSONObject>> webTableMap = new HashMap<String, IJoinIterable<JSONObject>>(11);

    DoublyLinkedList<IJoinIterable<JSONObject>> joinState = new DoublyLinkedList<IJoinIterable<JSONObject>>();

    public WebJoinIterator(Map<String, String> options) {
        String joinInfoStr = options.get("joinInfo");
        username = options.get("username");
        password = options.get("password");
        if (options.containsKey("useCache")) {
            useCache = Boolean.parseBoolean(options.get("useCache"));
        }
        columnMetaJsonPath = options.get("columnMetaJsonPath");
        int counter = 1;
        while (true) {
            String url = options.get("webTableURL." + counter);
            if (url == null) {
                break;
            }
            String alias = options.get("webTableAlias." + counter);
            Assertion.assertNotNull(alias);
            WebTableMeta wtm = new WebTableMeta(url, alias);
            webTableMetaList.add(wtm);
            wtmMap.put(alias, wtm);
            counter++;
        }
        if (joinInfoStr != null) {
            String[] joinInfoArr = joinInfoStr.split("\\s*,\\s*");
            for (String joinInfoStatement : joinInfoArr) {
                WebJoinInfo wji = WebJoinInfo.fromText(joinInfoStatement);
                WebTableMeta primaryWTM = wtmMap.get(wji.getPrimaryAlias());
                Assertion.assertNotNull(primaryWTM);
                primaryWTM.setJoinInfo(wji);
                primaryWTM.fieldJsonPath = wji.getPrimaryJsonPath();
                WebTableMeta secWTM = wtmMap.get(wji.getSecondaryAlias());
                joinRightPartMap.put(wji.getSecondaryAlias(), wji.getSecondaryJsonPath());
                Assertion.assertNotNull(secWTM);
                secWTM.recordJsonPath = wji.secondaryRecordJsonPath;
                secWTM.fieldJsonPath = wji.secondaryJsonPath;
            }
        }
    }

    public void startup() throws Exception {
        // load all non-parametrized web tables
        // FIXME XML file download assumption
        boolean first = true;
        for (WebTableMeta wtm : this.webTableMetaList) {
            if (!wtm.hasParametrizedURL()) {
                File contentFile = ContentLoader.getContent(wtm.url, null, useCache, username, password);
                WebTable wt;
                if (first) {
                    first = false;
                    wt = new WebTable(wtm, null, wtm.getFieldJsonPath(), this.columnMetaJsonPath);
                } else {
                    String joinRightPartJsonPath = this.joinRightPartMap.get(wtm.getAlias());
                    wt = new WebTable(wtm, joinRightPartJsonPath, wtm.getFieldJsonPath(), this.columnMetaJsonPath);
                }
                webTableMap.put(wtm.alias, wt);
                wt.startup(contentFile, wtm.getRecordJsonPath());
            } else {
                String joinRightPartJsonPath = this.joinRightPartMap.get(wtm.getAlias());
                OnDemandWebTable wt;
                if (wtm.isLastInChain()) {
                    wt = new OnDemandWebTable(wtm, username, password, null, null);
                } else {
                    wt = new OnDemandWebTable(wtm, username, password, joinRightPartJsonPath,
                            wtm.getFieldJsonPath());
                }
                webTableMap.put(wtm.alias, wt);
            }
        }
        //ASSUMPTION: each web table is joined with one other web table
        // and join order is the table specification order
        for (WebTableMeta wtm : this.webTableMetaList) {
            IJoinIterable<JSONObject> wt = webTableMap.get(wtm.alias);
            joinState.add(wt);
        }

    }

    @Override
    public boolean hasNext() {
        Node<IJoinIterable<JSONObject>> tail = joinState.getTail();
        Node<IJoinIterable<JSONObject>> head = joinState.getHead();
        try {
            if (tail.getPayload().peek() == null) {
                // initialize
                Node<IJoinIterable<JSONObject>> n = head;
                String joinValue = null;
                while (n != null) {
                    IJoinIterable<JSONObject> wt = n.getPayload();
                    wt.reset(joinValue);
                    if (wt.hasNext()) {
                        wt.next();
                        joinValue = wt.getJoinValue();
                    } else {
                        if (n != tail) {
                            n = n.getPrev();
                            if (n == head) {
                                joinValue = null;
                            }
                            continue;
                        } else {
                            return false;
                        }
                    }
                    n = n.getNext();
                }
                buildJoinRecord();
                return true;
            } else {
                IJoinIterable<JSONObject> wt = tail.getPayload();
                if (wt.hasNext()) {
                    wt.next();
                    buildJoinRecord();
                    return true;
                } else {
                    Node<IJoinIterable<JSONObject>> n = tail.getPrev();
                    Node<IJoinIterable<JSONObject>> hasNextNode = null;
                    while (n != null) {
                        if (n.getPayload().hasNext()) {
                            hasNextNode = n;
                            break;
                        }
                        n = n.getPrev();
                    }
                    if (hasNextNode == null) {
                        // all done
                        return false;
                    }
                    n = hasNextNode;
                    String joinValue = null;
                    while (n != null) {
                        wt = n.getPayload();
                        wt.reset(joinValue);
                        if (wt.hasNext()) {
                            wt.next();
                            joinValue = wt.getJoinValue();
                        } else {
                            if (n != tail) {
                                throw new RuntimeException("Should not happen!");
                            }
                        }
                        n = n.getNext();
                    }
                    buildJoinRecord();
                    return true;
                }
            }
        } catch (Exception x) {
            x.printStackTrace();
        }
        return false;
    }

    @Override
    public JSONObject next() {
        return this.currentRecord;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    void buildJoinRecord() {
        Node<IJoinIterable<JSONObject>> tail = joinState.getTail();
        JSONObject json = new JSONObject(tail.getPayload().peek().toString());
        Node<IJoinIterable<JSONObject>> p = tail.getPrev();
        while (p != null) {
            IJoinIterable<JSONObject> wt = p.getPayload();
            WebTableMeta wtm = wtmMap.get(wt.getAlias());
            String fieldName = wt.getAlias();
            JSONObject js;
            if (wtm.getCmList() != null && !wtm.getCmList().isEmpty()) {
                js = handleColumnNameAssignment(wt.peek(), wtm);
            } else {
                js = new JSONObject(wt.peek().toString());
            }
            json.put(fieldName, js);
            p = p.getPrev();
        }

        this.currentRecord = json;
    }

    JSONObject handleColumnNameAssignment(JSONObject json, WebTableMeta wtm) {
        if (json.keySet().size() == 1 && json.get(json.keySet().iterator().next()) instanceof JSONArray) {
            JSONArray jsArr = json.getJSONArray(json.keySet().iterator().next());

            List<ColumnMeta> cmList = wtm.getCmList();
            Assertion.assertTrue(jsArr.length() == cmList.size());
            JSONObject record = new JSONObject();
            for (int i = 0; i < jsArr.length(); i++) {
                Object o = jsArr.get(i);
                if (o instanceof JSONObject) {
                    JSONObject js = (JSONObject) o;
                    if (js.has("_$")) {
                        record.put(cmList.get(i).getName(), js.getString("_$"));
                    } else {
                        record.put(cmList.get(i).getName(), "");
                    }
                } else {
                    record.put(cmList.get(i).getName(), o.toString());
                }

            }

            return record;
        } else {
            return new JSONObject(json.toString());
        }

    }

    public static void handlePayload(List<JSONObject> list, JPNode jpNode) {
        if (jpNode.getPayload() instanceof JSONArray) {
            JSONArray jsArr = (JSONArray) jpNode.getPayload();
            for (int i = 0; i < jsArr.length(); i++) {
                list.add(jsArr.getJSONObject(i));
            }
        } else {
            list.add((JSONObject) jpNode.getPayload());
        }
    }

    public static class WebJoinInfo {
        String primaryAlias;
        String secondaryAlias;
        String primaryJsonPath;
        String secondaryJsonPath;
        String primaryRecordJsonPath;
        String secondaryRecordJsonPath;

        public WebJoinInfo(String firstAlias, String secondAlias, String firstJsonPath,
                           String secondJsonPath) {
            this.primaryAlias = firstAlias;
            this.secondaryAlias = secondAlias;
            this.primaryJsonPath = firstJsonPath;
            this.secondaryJsonPath = secondJsonPath;
        }

        public String getPrimaryAlias() {
            return primaryAlias;
        }

        public String getSecondaryAlias() {
            return secondaryAlias;
        }

        public String getPrimaryJsonPath() {
            return primaryJsonPath;
        }

        public String getSecondaryJsonPath() {
            return secondaryJsonPath;
        }

        public String getSecondaryRecordJsonPath() {
            return secondaryRecordJsonPath;
        }

        public String getPrimaryRecordJsonPath() {
            return primaryRecordJsonPath;
        }

        public static WebJoinInfo fromText(String joinStatement) {
            String[] tokens = joinStatement.split("\\s*=\\s*");
            Assertion.assertTrue(tokens.length == 2);
            String[] parts = tokens[0].split("::");
            String primaryAlias = parts[0];
            String[] primaryJsonPaths = splitJsonPath(parts[1]);

            parts = tokens[1].split("::");
            String secondaryAlias = parts[0];
            WebJoinInfo ji;
            if (parts.length == 2) {
                String[] secondaryJsonPaths = splitJsonPath(parts[1]);

                ji = new WebJoinInfo(primaryAlias, secondaryAlias, primaryJsonPaths[1], secondaryJsonPaths[1]);
                ji.primaryRecordJsonPath = primaryJsonPaths[0];
                ji.secondaryRecordJsonPath = secondaryJsonPaths[0];
            } else {
                ji = new WebJoinInfo(primaryAlias, secondaryAlias, primaryJsonPaths[1], null);
                ji.primaryRecordJsonPath = primaryJsonPaths[0];
            }
            return ji;
        }

        static String[] splitJsonPath(String jsonPathStr) {
            String[] result = new String[2];
            int offset = jsonPathStr.startsWith("$..") ? 3 : 2;
            int idx = jsonPathStr.indexOf('.', offset);
            Assertion.assertTrue(idx != -1);
            result[0] = jsonPathStr.substring(0, idx);
            result[1] = "$." + jsonPathStr.substring(idx + 1);
            return result;
        }

    }//;

    public static class XmlMetaDataExtractor {
        Map<String, String> paramsMap;
        String xpathStr;

        public XmlMetaDataExtractor() {
        }

        public void initialize(Map<String, String> paramsMap) {
            this.paramsMap = paramsMap;
            this.xpathStr = paramsMap.get("path");
        }

        public WebTableMeta extract(String filePath, String alias) throws Exception {
            SAXBuilder builder = new SAXBuilder();
            Document doc = builder.build(new File(filePath));
            Element rootEl = doc.getRootElement();

            XpathFieldExtractor extractor = new XpathFieldExtractor(rootEl.getNamespacesInScope());
            Map<String, List<String>> nameValueMap = new HashMap<String, List<String>>(7);
            extractor.extractValue(rootEl, xpathStr, nameValueMap);
            WebTableMeta wtm = new WebTableMeta(filePath, alias);

            return wtm;
        }
    }


    public static void main(String[] args) throws Exception {
        String pwd = Parameters.getInstance().getParam("xnat.pwd");
        Map<String, String> options = new HashMap<String, String>();
        options.put("username", "scicrunch");
        options.put("password", pwd);
        options.put("useCache", "true");
        options.put("joinInfo", "projects::$..row.cell[0].'_$' = subjects::$..row.cell[1].'_$', " +
                "subjects::$..row.cell[0].'_$' = subject::");
        options.put("webTableURL.1", "https://www.nitrc.org/ir/data/projects?format=xml");
        options.put("webTableAlias.1", "projects");
        options.put("webTableURL.2", "https://www.nitrc.org/ir/data/projects/fcon_1000/subjects?format=xml");
        options.put("webTableAlias.2", "subjects");
        options.put("webTableURL.3", "https://www.nitrc.org/ir/data/projects/fcon_1000/subjects/${subject}?format=xml");
        options.put("webTableAlias.3", "subject");
        options.put("columnMetaJsonPath", "$.ResultSet.results.columns.column[*].'_$'");

        WebJoinIterator iter = new WebJoinIterator(options);
        iter.startup();
        int count = 0;
        while (iter.hasNext()) {
            JSONObject record = iter.next();
            System.out.println(record.toString(2));
            count++;
            if (count > 10) {
                break;
            }
        }

    }
}
