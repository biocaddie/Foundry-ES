package org.neuinfo.foundry.common.transform;

import com.mongodb.BasicDBObject;
import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.common.util.Utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 9/4/15.
 */
public class Locator {
    JSONObject refDoc;
    String refSourceID;
    String jsonPath4PK;
    Source source;
    DocumentIngestionService dis;

    public Locator(Mapping mapping, DocumentIngestionService dis) {
        this.refSourceID = mapping.getRefSourceID();
        this.jsonPath4PK = mapping.getJsonPathForPK();
        this.dis = dis;
        source = dis.findSource(this.refSourceID, null);
        Assertion.assertNotNull(source);
    }

    public List<String> locate(JSONObject document, Mapping mapping) {
        Map<String, List<String>> dataMap = new HashMap<String, List<String>>();
        if (refDoc == null) {
            JsonPathFieldExtractor extractor = new JsonPathFieldExtractor();
            extractor.extractValue(document, jsonPath4PK, dataMap);
            if (dataMap.isEmpty() || Utils.isEmpty(dataMap.get(jsonPath4PK).get(0))) {
                // no primary key value
                return Collections.emptyList();
            }
            String pkValue = dataMap.get(jsonPath4PK).get(0);
            // FIXME hardcoded collection name;
            BasicDBObject ref = dis.findDocumentByPK(pkValue, source, "nifRecords");
            BasicDBObject dataDB = (BasicDBObject) ref.get("Data");
            BasicDBObject transformedRecDB = (BasicDBObject) dataDB.get("transformedRec");
            if (transformedRecDB != null) {
                refDoc = JSONUtils.toJSON(transformedRecDB, false);
            }
            dataMap.clear();
        }

        Assertion.assertNotNull(refDoc);
        String refJsonPath = mapping.getRefJsonPath();
        JsonPathFieldExtractor extractor = new JsonPathFieldExtractor();
        extractor.extractValue(refDoc, refJsonPath, dataMap);

        return dataMap.get(refJsonPath);
    }

    /**
     * needs to be called before each new document to cleanup cache
     */
    public void reset() {
        refDoc = null;
    }
}
