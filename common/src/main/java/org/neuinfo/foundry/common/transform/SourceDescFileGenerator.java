package org.neuinfo.foundry.common.transform;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.PrimaryKeyDef;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.util.Utils;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Generates source/harvest description file with transformation and mapping scripts.
 * <p/>
 * Created by bozyurt on 5/5/15.
 */
public class SourceDescFileGenerator {

    public static Source prepareSource(SourceInfo si, File outFile) throws IOException {
        Source.Builder builder = new Source.Builder(si.getSourceId(), si.getName());
        builder.dataSource(si.getDataSource());
        JSONObject icJson = new JSONObject();
        icJson.put("ingestMethod", si.getIngestMethod().toString());
        if (si.getIngestURL() != null) {
            icJson.put("ingestURL", si.getIngestURL());
        }
        icJson.put("allowDuplicates", "false");
        JSONObject crawlFreqJson = new JSONObject();
        crawlFreqJson.put("crawlType", "Frequency");
        crawlFreqJson.put("hours", "48");
        crawlFreqJson.put("minutes", "0");
        JSONArray jsArr = new JSONArray();
        String[] startDays = {"Sunday", "Monday", "Tuesday", "Wednesday",
                "Thursday", "Friday", "Saturday"};
        for (String startDay : startDays) {
            jsArr.put(startDay);
        }
        crawlFreqJson.put("startDays", jsArr);
        crawlFreqJson.put("startTime", "0:00");
        crawlFreqJson.put("operationEndTime", "24:00");
        icJson.put("crawlFrequency", crawlFreqJson);
        builder.ingestConfiguration(icJson);
        List<String> workflowSteps = new ArrayList<String>(3);
        workflowSteps.add("UUID Generation");
        workflowSteps.add("Index");
        PrimaryKeyDef pkDef = new PrimaryKeyDef(
                si.getPrimaryKeyJsonPaths(),
                Arrays.asList(":"), "Value");
        JSONObject contentSpecJson = new JSONObject();
        for (String name : si.getContentSpecMap().keySet()) {
            contentSpecJson.put(name, si.getContentSpecMap().get(name));
        }
        Source source = builder.workflowSteps(workflowSteps).primaryKey(pkDef)
                .contentSpecification(contentSpecJson)
                .transformScript(si.getTransformScript())
                .mappingScript(si.getMappingScript()).build();

        builder.collectionName(si.getCollectionName()).repositoryID(si.getRepositoryID());
        System.out.println(source.toJSON().toString(2));

        if (outFile != null) {
            Utils.saveText(source.toJSON().toString(2), outFile.getAbsolutePath());
        }
        return source;
    }

    public enum IngestMethod {
        CSV, XML, FTP, RSS, RSYNC, RESOURCE, ASPERA, DISCO, OAI, WEB, JSON2
    }

    public static class SourceInfo {
        String sourceId;
        String name;
        String dataSource;
        String repositoryID;
        String collectionName;
        IngestMethod ingestMethod;
        String ingestURL;


        List<String> primaryKeyJsonPaths = new LinkedList<String>();
        Map<String, String> contentSpecMap = new HashMap<String, String>(17);
        String transformScript;
        String mappingScript;

        public SourceInfo(String sourceId, String name, String dataSource, IngestMethod ingestMethod) {
            this.sourceId = sourceId;
            this.name = name;
            this.dataSource = dataSource;
            this.ingestMethod = ingestMethod;
        }

        public SourceInfo setRepositoryID(String repositoryID) {
            this.repositoryID = repositoryID;
            return this;
        }

        public SourceInfo setCollectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public SourceInfo setIngestURL(String ingestURL) {
            this.ingestURL = ingestURL;
            return this;
        }

        public SourceInfo setPrimaryKeyJsonPath(String primaryKeyJsonPath) {
            this.primaryKeyJsonPaths.add(primaryKeyJsonPath);
            return this;
        }

        public SourceInfo addPrimaryKeyJsonPath(String primaryKeyJsonPath) {
            this.primaryKeyJsonPaths.add(primaryKeyJsonPath);
            return this;
        }

        public List<String> getPrimaryKeyJsonPaths() {
            return primaryKeyJsonPaths;
        }

        public void setContentSpecParam(String name, String value) {
            contentSpecMap.put(name, value);
        }

        public String getSourceId() {
            return sourceId;
        }

        public String getName() {
            return name;
        }

        public String getDataSource() {
            return dataSource;
        }

        public IngestMethod getIngestMethod() {
            return ingestMethod;
        }

        public String getIngestURL() {
            return ingestURL;
        }


        public Map<String, String> getContentSpecMap() {
            return contentSpecMap;
        }

        public void setTransformScript(String scriptPath) {
            this.transformScript = TransformMappingUtils.loadTransformMappingScript(scriptPath);
        }

        public void setMappingScript(String scriptPath) {
            this.mappingScript = TransformMappingUtils.loadTransformMappingScript(scriptPath);
        }

        public String getTransformScript() {
            return transformScript;
        }

        public String getMappingScript() {
            return mappingScript;
        }

        public String getRepositoryID() {
            return repositoryID;
        }

        public String getCollectionName() {
            return collectionName;
        }
    }


    public static void main(String[] args) {


    }
}
