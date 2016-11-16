package org.neuinfo.foundry.consumers.jms.consumers.plugins;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;
import org.neuinfo.foundry.common.ingestion.GridFSService;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.transform.JsonPathFieldExtractor;
import org.neuinfo.foundry.common.transform.Mapping;
import org.neuinfo.foundry.common.transform.MappingEngine;
import org.neuinfo.foundry.common.transform.TransformationEngine;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.ServiceFactory;
import org.neuinfo.foundry.consumers.plugin.IPlugin;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by bozyurt on 5/5/15.
 */
public class TransformationEnhancer implements IPlugin {
    GridFSService gridFSService;
    DocumentIngestionService dis;
    boolean addResourceInfo = true;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd'T'HHmmssZ");
    private final static Logger log = Logger.getLogger(TransformationEnhancer.class);

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
        addResourceInfo = options.containsKey("addResourceInfo") ?
                Boolean.parseBoolean(options.get("addResourceInfo")) : true;
    }

    @Override
    public Result handle(DBObject docWrapper) {
        try {
            BasicDBObject data = (BasicDBObject) docWrapper.get("Data");
            String originalFileIDstr = (String) docWrapper.get("originalFileId");
            JSONObject json;
            if (!Utils.isEmpty(originalFileIDstr)) {
                // large file
                Assertion.assertNotNull(this.gridFSService);
                json = gridFSService.findJSONFile(new ObjectId(originalFileIDstr));
            } else {
                DBObject originalDoc = (DBObject) docWrapper.get("OriginalDoc");
                json = JSONUtils.toJSON((BasicDBObject) originalDoc, false);
            }
            DBObject siDBO = (DBObject) docWrapper.get("SourceInfo");
            String srcId = siDBO.get("SourceID").toString();
            String dataSource = (String) siDBO.get("DataSource");

            TransformationRegistry registry = TransformationRegistry.getInstance();
            MappingRegistry mappingRegistry = MappingRegistry.getInstance();

            TransformationEngine trEngine = registry.getTransformationEngine(srcId);
            Assertion.assertNotNull(trEngine);
            MappingEngine mappingEngine = mappingRegistry.getMappingEngine(srcId);
            if (mappingEngine != null) {
                mappingEngine.bootstrap(dis);
            }
            JSONObject transformedJson = new JSONObject();
            trEngine.transform(json, transformedJson);

            JSONArray dataTypesArr = new JSONArray();
            for (String key : transformedJson.keySet()) {
                if (!key.equalsIgnoreCase("id")) {
                    dataTypesArr.put(key);
                }
            }

            JSONObject dataItem;
            if (transformedJson.has("dataItem")) {
                dataItem = transformedJson.getJSONObject("dataItem");
            } else {
                dataItem = new JSONObject();
                transformedJson.put("dataItem", dataItem);
            }
            dataItem.put("dataTypes", dataTypesArr);

            Source source = dis.findSource(srcId, dataSource);
            Assertion.assertNotNull(source);
            if (addResourceInfo) {
                ResourceInfoFinder.ResourceInfo ri;
                if (!Utils.isEmpty(source.getRepositoryID())) {
                    ri = ResourceInfoFinder.getResourceInfo(source.getRepositoryID());
                } else {

                    // FIXME
                    if (srcId.equals("biocaddie-0005")) {
                        ri = ResourceInfoFinder.getResourceInfo("nlx_156062");
                    } else if (srcId.equals("biocaddie-0006")) {
                        ri = ResourceInfoFinder.getResourceInfo("nif-0000-00142");
                    } else if (srcId.equals("biocaddie-0009")) {
                        ri = ResourceInfoFinder.getResourceInfo("nif-0000-30123");
                    } else if (srcId.equals("biocaddie-0008")) {
                        ri = ResourceInfoFinder.getResourceInfo("nlx_143909");
                    } else if (srcId.equals("biocaddie-0007")) {
                        ri = ResourceInfoFinder.getResourceInfo("nif-0000-08127");
                    } else {
                        ri = ResourceInfoFinder.getResourceInfo(srcId);
                    }
                }
                Assertion.assertNotNull(ri);
                transformedJson.put("dataResource", ri.toJSON());
            }
            // apply mapping also (if any)
            if (mappingEngine != null) {
                mappingEngine.map(transformedJson);
            }

            // provenance
            JSONObject provenance = prepProvenance(source);
            transformedJson.put("provenance", provenance);

            if (log.isDebugEnabled()) {
                log.info("==========================");
                log.info(transformedJson.toString(2));
                log.info("==========================");
            }
            BasicDBObject trDBO = (BasicDBObject) data.get("transformedRec");
            if (trDBO != null) {
                JSONObject oldTransformedJson = JSONUtils.toJSON(trDBO, false);
                merge(oldTransformedJson, transformedJson);
                transformedJson = oldTransformedJson;
            }

            data.put("transformedRec", JSONUtils.encode(transformedJson, true));
            return new Result(docWrapper, Result.Status.OK_WITH_CHANGE);
        } catch (Throwable t) {
            t.printStackTrace();
            log.error("handle", t);
            Result r = new Result(docWrapper, Result.Status.ERROR);
            r.setErrMessage(t.getMessage());
            return r;
        }
    }


    private void merge(JSONObject oldTransformedJson, JSONObject newTransformedJson) {
        Set<String> exceptionSet = new HashSet<String>();
        // For Biocaddie Pipeline
        exceptionSet.add("citationInfo");
        exceptionSet.add("NLP_Fields");
        List<String> oldKeys = new ArrayList<String>(oldTransformedJson.keySet());
        for(String oldKey: oldKeys) {
            if (!exceptionSet.contains(oldKey)) {
                oldTransformedJson.remove(oldKey);
            }
        }
        for(String newKey : newTransformedJson.keySet()) {
            oldTransformedJson.put(newKey, newTransformedJson.get(newKey));
        }
    }

    private JSONObject prepProvenance(Source source) {
        JSONObject json = new JSONObject();
        JSONObject ingestConfiguration = source.getIngestConfiguration();
        JSONObject contentSpec = source.getContentSpecification();
        String ingestMethod = ingestConfiguration.getString("ingestMethod");
        String ingestTarget = null;
        String filePattern;
        if (ingestConfiguration.has("ingestURL")) {
            ingestTarget = ingestConfiguration.getString("ingestURL");
        }
        if (ingestMethod.equalsIgnoreCase("RSYNC")) {
            ingestTarget = contentSpec.getString("rsyncSource");
        }
        if (ingestMethod.equalsIgnoreCase("disco")) {
            ingestMethod = "dkNET";
        }
        filePattern = null;
        if (contentSpec.has("filenamePattern")) {
           filePattern = contentSpec.getString("filenamePattern");
        }
        json.put("ingestMethod", ingestMethod);
        json.put("ingestTarget", ingestTarget != null ? ingestTarget : "");
        json.put("filePattern", filePattern != null ? filePattern : "");
        json.put("ingestTime", sdf.format( new Date()));

        return json;
    }

    @Override
    public String getPluginName() {
        return "TransformationEnhancer";
    }
}
