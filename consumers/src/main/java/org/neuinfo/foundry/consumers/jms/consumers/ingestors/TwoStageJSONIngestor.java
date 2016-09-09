package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import com.mongodb.util.JSON;
import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.json.JSONObject;
import org.neuinfo.foundry.common.transform.JSONPathUtils;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONPathProcessor2;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.JSONMultiWebIterator;
import org.neuinfo.foundry.consumers.common.JSONWebIterator;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.util.*;

/**
 * Created by bozyurt on 2/16/16.
 */
public class TwoStageJSONIngestor implements Ingestor {
    String idIngestURL;
    String idIngestURLTemplate;
    String idIngestURLTemplateParams;
    String parentDocJsonPath;
    String childDocParentIDPath;
    String dataIngestURLTemplate;
    boolean allowDuplicates = false;
    String idJsonPath;
    String docJsonPath;
    String offsetParam;
    boolean sampleMode = false;
    int sampleSize = 1;
    Map<String, String> optionMap;
    JSONWebIterator idIterator;
    JSONMultiWebIterator multiIdIterator;
    List<String> templateVariables;
    List<String> ingestURLTemplateVariables;
    String totalParamJsonPath;
    static Logger log = Logger.getLogger(TwoStageJSONIngestor.class);

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.idIngestURL = options.get("idIngestURL");
        this.dataIngestURLTemplate = options.get("dataIngestURLTemplate");
        this.allowDuplicates = options.containsKey("allowDuplicates") ?
                Boolean.parseBoolean(options.get("allowDuplicates")) : false;
        this.idJsonPath = options.get("idJsonPath");
        this.totalParamJsonPath = options.get("totalParamJsonPath");
        this.docJsonPath = options.get("docJsonPath");
        if (options.containsKey("offsetParam")) {
            this.offsetParam = options.get("offsetParam");
        }
        this.sampleMode = options.containsKey("sampleMode")
                ? Boolean.parseBoolean(options.get("sampleMode")) : false;

        this.sampleSize = Utils.getIntValue(options.get("sampleSize"), 1);
        this.optionMap = options;
        Assertion.assertNotNull(this.dataIngestURLTemplate);
        this.templateVariables = IngestorHelper.extractTemplateVariables(this.dataIngestURLTemplate);
        Assertion.assertNotNull(this.templateVariables);
        Assertion.assertTrue(this.templateVariables.size() == 1);

        this.idIngestURLTemplate = options.get("idIngestURLTemplate");
        this.idIngestURLTemplateParams = options.get("idIngestURLTemplateParams");
        if (idIngestURLTemplate != null) {
            ingestURLTemplateVariables = IngestorHelper.extractTemplateVariables(this.idIngestURLTemplate);
            Assertion.assertNotNull(ingestURLTemplateVariables);
            Assertion.assertTrue(ingestURLTemplateVariables.size() == 1);
            parentDocJsonPath = options.get("parentDocJsonPath");
            childDocParentIDPath = options.get("childDocParentIDPath");
            Assertion.assertNotNull(parentDocJsonPath);
            Assertion.assertNotNull(childDocParentIDPath);
        }
    }

    @Override
    public void startup() throws Exception {
        if (idIngestURLTemplate != null) {
            String[] urlTemplateParams = idIngestURLTemplateParams.split("\\s*,\\s*");
            Map<String, String> templateVar2ValueMap = new HashMap<String, String>(3);
            List<String> ingestURLs = new ArrayList<String>(urlTemplateParams.length);
            for (String urlTemplateParam : urlTemplateParams) {
                templateVar2ValueMap.put(ingestURLTemplateVariables.get(0), urlTemplateParam.trim());
                String url = IngestorHelper.createURL(this.idIngestURLTemplate, templateVar2ValueMap);
                ingestURLs.add(url);
            }
            this.multiIdIterator = new JSONMultiWebIterator(ingestURLs, Arrays.asList(urlTemplateParams),
                    parentDocJsonPath, idJsonPath);

        } else {
            this.idIterator = new JSONWebIterator(idIngestURL, offsetParam, totalParamJsonPath, idJsonPath, sampleMode);
        }
    }

    @Override
    public Result prepPayload() {
        if (idIterator != null) {
            try {
                JSONObject idJson = idIterator.next();
                String idValue = idJson.getString("value");
                Map<String, String> templateVar2ValueMap = new HashMap<String, String>(3);
                templateVar2ValueMap.put(templateVariables.get(0), idValue);
                String url = IngestorHelper.createURL(this.dataIngestURLTemplate, templateVar2ValueMap);
                String jsonContent = Utils.sendGetRequest(url);
                JSONObject json = new JSONObject(jsonContent);
                JSONPathProcessor2 pathProcessor2 = new JSONPathProcessor2();
                List<JSONPathProcessor2.JPNode> jpNodes = pathProcessor2.find(docJsonPath, json);
                Assertion.assertTrue(jpNodes != null && jpNodes.size() == 1);
                JSONObject docJson = (JSONObject) jpNodes.get(0).getPayload();

                return new Result(docJson, Result.Status.OK_WITH_CHANGE);
            } catch (Throwable t) {
                log.error("prepPayload", t);
                t.printStackTrace();
                return new Result(null, Result.Status.ERROR, t.getMessage());
            }
        } else {
            try {
                JSONMultiWebIterator.ParentJSONInfo pji = multiIdIterator.next();
                Map<String, String> templateVar2ValueMap = new HashMap<String, String>(3);
                templateVar2ValueMap.put(templateVariables.get(0), pji.getChildID());
                String url = IngestorHelper.createURL(this.dataIngestURLTemplate, templateVar2ValueMap);
                String jsonContent = Utils.sendGetRequest(url);
                JSONObject json = new JSONObject(jsonContent);

                if (childDocParentIDPath != null) {
                    List<String> parentIDs = new ArrayList<String>(pji.getParentIDs());
                    Collections.sort(parentIDs);
                    String prefix = childDocParentIDPath.replaceFirst("\\[\\]$","");
                    for (int i = 0; i < parentIDs.size(); i++) {
                        String parentID = parentIDs.get(i);
                        String leafPath = prefix + "[" + i + "]";
                        JSONUtils.createOrSetFullPath(json, leafPath, parentID);
                    }
                }
                return new Result(json, Result.Status.OK_WITH_CHANGE);
            } catch (Throwable t) {
                log.error("prepPayload", t);
                t.printStackTrace();
                return new Result(null, Result.Status.ERROR, t.getMessage());
            }
        }
    }

    @Override
    public String getName() {
        return "TwoStageJSONIngestor";
    }

    @Override
    public int getNumRecords() {
        return -1;
    }

    @Override
    public String getOption(String optionName) {
        return optionMap.get(optionName);
    }

    @Override
    public void shutdown() {
        // no op
    }

    @Override
    public boolean hasNext() {
        if (this.idIterator != null) {
            return this.idIterator.hasNext();
        } else {
            return this.multiIdIterator.hasNext();
        }
    }
}
