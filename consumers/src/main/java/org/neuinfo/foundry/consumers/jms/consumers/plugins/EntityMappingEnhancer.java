package org.neuinfo.foundry.consumers.jms.consumers.plugins;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;
import org.neuinfo.foundry.common.ingestion.GridFSService;
import org.neuinfo.foundry.common.model.JsonNode;
import org.neuinfo.foundry.common.model.VocabularyInfo;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.common.util.SynonymsRequestManager;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.plugin.IPlugin;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.net.URI;
import java.util.*;

/**
 * Created by bozyurt on 3/8/16.
 */
public class EntityMappingEnhancer implements IPlugin {
    GridFSService gridFSService;
    DocumentIngestionService dis;
    SynonymsRequestManager srm;
    private final static Logger log = Logger.getLogger(EntityMappingEnhancer.class);

    public EntityMappingEnhancer() {
        srm = SynonymsRequestManager.getInstance();
    }

    @Override
    public void setDocumentIngestionService(DocumentIngestionService dis) {
        this.dis = dis;
    }

    @Override
    public void setGridFSService(GridFSService gridFSService) {
        this.gridFSService = gridFSService;
    }

    @Override
    public void initialize(Map<String, String> options) throws Exception {

    }

    @Override
    public Result handle(DBObject docWrapper) {
        try {
            DBObject siDBO = (DBObject) docWrapper.get("SourceInfo");
            String srcID = siDBO.get("SourceID").toString();
            String dataSource = (String) siDBO.get("DataSource");
            BasicDBObject transformedDBO = (BasicDBObject) docWrapper.get("transformedRec");
            JSONObject json = JSONUtils.toJSON(transformedDBO, true);

            addEntityMappings(srcID, json);

            DBObject updatedTransformedDBO = JSONUtils.encode(json, true);
            docWrapper.put("transformedRec", updatedTransformedDBO);

            return  new Result(docWrapper, Result.Status.OK_WITH_CHANGE);
        } catch (Throwable t) {
            log.error("handle", t);
            t.printStackTrace();
            Result r = new Result(docWrapper, Result.Status.ERROR);
            r.setErrMessage(t.getMessage());
            return r;
        }
    }

    public void addEntityMappings(String srcID, JSONObject json) throws Exception {
        JsonNode rootNode = JSONUtils.toTraversableJSONDOM(json);
        List<JsonNode> allLeafNodes = JSONUtils.findAllLeafNodes(rootNode);
        Map<String, String> path2ValueMap = new HashMap<String, String>();
        Set<String> valueSet = new HashSet<String>();
        for (JsonNode leaf : allLeafNodes) {
            if (!Utils.isNumber(leaf.getJson())) {
                String path = leaf.toDotNotation();
                String value = leaf.getJson().toString();
                path2ValueMap.put(path, value);
                valueSet.add(value);
            }
        }
        Map<String, List<EntityMappingInfo>> value2EntityMappingsMap = getEntityMappings(
                new ArrayList<String>(valueSet), srcID);

        Map<String,List<String>> value2PathsMap = new HashMap<String, List<String>>();
        for(String path : path2ValueMap.keySet()) {
            String value = path2ValueMap.get(path).toLowerCase();
            List<String> paths = value2PathsMap.get(value);
            if (paths == null) {
                paths = new LinkedList<String>();
                value2PathsMap.put(value, paths);
            }
            paths.add(path);
        }
        JSONObject emJSON = new JSONObject();
        JSONObject synJSON = new JSONObject();
        for(String valueLC : value2EntityMappingsMap.keySet()) {
            List<EntityMappingInfo> emiList = value2EntityMappingsMap.get(valueLC);
            List<String> paths = value2PathsMap.get(valueLC);
            insertEntityMappings(emJSON, emiList, paths);
            insertSynonymns(synJSON, emiList, paths, valueLC);
        }
        json.put("entityMappings", emJSON);
        json.put("synonyms", synJSON);
    }



    void insertSynonymns(JSONObject synJSON, List<EntityMappingInfo> emiList, List<String> paths, String valueLC) {
        Map<String, String> leaf2PathMap = getLeaf2PathMap(paths);
        for(EntityMappingInfo emi : emiList) {
            if (!emi.relation.equals("exact")) {
                continue;
            }
            String key = emi.column + ":" + emi.tableName;
            String path = leaf2PathMap.get(key);
            if (path != null) {
                int idx = path.indexOf('.');
                Assertion.assertTrue(idx != -1);
                String newPath = path.substring(idx+1);
                try {
                    VocabularyInfo vi =  srm.getSynonyms(emi.identifier);
                    if (vi != null && !vi.getSynonyms().isEmpty()) {
                        List<String> synonyms = vi.getSynonyms();
                        idx = newPath.lastIndexOf('.');
                        Assertion.assertTrue(idx != -1);
                        String prefix = newPath.substring(0, idx);
                        String suffix = newPath.substring(idx+1);
                        suffix = JSONUtils.extractName(suffix);
                        StringBuilder sb = new StringBuilder(128);
                        int arrIdx = 0;
                        for(int i = 0; i < synonyms.size(); i++) {
                            String synonym = synonyms.get(i);
                            if (synonym.equalsIgnoreCase(valueLC)) {
                                continue;
                            }
                            sb.setLength(0);
                            sb.append(prefix).append('.').append(suffix).append('[').append(arrIdx++).append(']');
                            newPath = sb.toString();
                            // System.out.println(newPath);
                            JSONUtils.createOrSetFullPath(synJSON, newPath, synonym);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
    void insertEntityMappings(JSONObject emJSON, List<EntityMappingInfo> emiList, List<String> paths) {
        Map<String, String> leaf2PathMap = getLeaf2PathMap(paths);
        for(EntityMappingInfo emi : emiList) {
            if (!emi.relation.equals("exact")) {
                continue;
            }
            String key = emi.column + ":" + emi.tableName;
            String path = leaf2PathMap.get(key);
            if (path != null) {
                int idx = path.indexOf('.');
                Assertion.assertTrue(idx != -1);
                String newPath = path.substring(idx+1);
                JSONUtils.createOrSetFullPath(emJSON, newPath, emi.identifier);
            }
        }
    }

    private Map<String, String> getLeaf2PathMap(List<String> paths) {
        Map<String, String> leaf2PathMap = new HashMap<String, String>();
        for(String path : paths) {
            int idx = path.lastIndexOf('.');
            Assertion.assertTrue(idx != -1);
            String prefix = path.substring(0, idx);
            String leafName = path.substring(idx+1);
            String tableName = prefix;
            idx = prefix.lastIndexOf('.');
            if (idx != -1) {
                tableName = prefix.substring(idx+1);
            }
            leafName = JSONUtils.extractName(leafName);
            tableName = JSONUtils.extractName(tableName);

            String key = leafName + ":" + tableName;

            leaf2PathMap.put(key, path);
        }
        return leaf2PathMap;
    }

    @Override
    public String getPluginName() {
        return "EntityMappingEnhancer";
    }

    public static Map<String, List<EntityMappingInfo>> getEntityMappings(List<String> values, String sourceID) throws Exception {
        HttpClient client = new DefaultHttpClient();
        URIBuilder builder = new URIBuilder("https://stage.scicrunch.org");
        builder.setPath("/api/1/entitymapping/byvaluelist");
        String list = Utils.join(values, "|");
        builder.setParameter("list", list);
        builder.setParameter("delimiter", "|");
        URI uri = builder.build();
        log.info("getEntityMappings: uri:" + uri);
        HttpGet httpGet = new HttpGet(uri);
        Map<String, List<EntityMappingInfo>> emListByValueMap = new HashMap<String, List<EntityMappingInfo>>();
        try {
            httpGet.addHeader("Accept", "application/json");
            HttpResponse response = client.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String jsonStr = EntityUtils.toString(entity);
                    JSONObject json = new JSONObject(jsonStr);
                    JSONArray dataArr = json.getJSONArray("data");
                    for (int i = 0; i < dataArr.length(); i++) {
                        EntityMappingInfo emi = EntityMappingInfo.fromJSON(dataArr.getJSONObject(i));
                        if (emi.source.equals(sourceID)) {
                            String key = emi.value.toLowerCase();
                            List<EntityMappingInfo> emiList = emListByValueMap.get(key);
                            if (emiList == null) {
                                emiList = new ArrayList<EntityMappingInfo>(5);
                                emListByValueMap.put(key, emiList);
                            }
                            emiList.add(emi);
                        }
                    }
                }
            }
        } finally {
            if (httpGet != null) {
                httpGet.releaseConnection();
            }
        }
        return emListByValueMap;
    }

    public static class EntityMappingInfo {
        String source;
        String tableName;
        String column;
        String value;
        String identifier;
        String relation;

        public static EntityMappingInfo fromJSON(JSONObject json) {
            EntityMappingInfo emi = new EntityMappingInfo();
            emi.source = json.getString("source");
            emi.tableName = json.getString("table_name");
            emi.column = json.getString("col");
            emi.value = json.getString("value");
            emi.identifier = json.getString("identifier");
            emi.relation = json.getString("relation");
            return emi;
        }
    }
}
