package org.neuinfo.foundry.ws.resources;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.transform.SourceDescFileGenerator;
import org.neuinfo.foundry.common.transform.SourceDescFileGenerator.IngestMethod;
import org.neuinfo.foundry.common.transform.TransformationEngine;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSTreeJSONGenerator;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.coordinator.ConsumerFactory;
import org.neuinfo.foundry.consumers.plugin.Ingestable;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;
import org.neuinfo.foundry.ws.common.CacheManager;
import org.neuinfo.foundry.ws.common.MongoService;
import org.neuinfo.foundry.ws.common.SecurityService;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.util.*;

/**
 * Created by bozyurt on 8/27/15.
 */
@Path("sources")
public class SourceManagerResource {
    private final static Logger logger = Logger.getLogger(SourceManagerResource.class);

    @Path("/")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response saveSource(@FormParam("payload") String payload, @FormParam("apiKey") String apiKey) {
        MongoService mongoService;
        try {
            boolean ok = SecurityService.getInstance().isAuthenticated(apiKey);
            if (!ok) {
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            // System.out.println("payload:" + payload);
            mongoService = MongoService.getInstance();
            JSONObject json = new JSONObject(payload);
            System.out.println(json.toString(2));
            String opMode = json.getString("opMode");
            String srcId = json.getString("sourceID");
            String sourceName = json.getString("sourceName");
            String dataSource = json.getString("dataSource");
            String ingestMethod = json.getString("ingestMethod");
            String repositoryID = null;
            if (json.has("repositoryID")) {
                repositoryID = json.getString("repositoryID");
            }
            if (Utils.isEmpty(dataSource)) {
                dataSource = srcId;
            }

            Source existingSource = mongoService.findSource(srcId, dataSource.equals(srcId) ? null : dataSource);
            if (opMode.equals("new") && existingSource != null) {
                return Response.status(Response.Status.BAD_REQUEST).build();
            }
            if (opMode.equals("update") && existingSource == null) {
                return Response.status(Response.Status.BAD_REQUEST).build();
            }


            if (opMode.equals("update")) {
                boolean updated = mongoService.updateSource(srcId,
                        dataSource.equals(srcId) ? null : dataSource, json);
                if (!updated) {
                    return Response.status(Response.Status.BAD_REQUEST).build();
                }
                return Response.ok().build();
            }

            IngestMethod im = IngestMethod.valueOf(ingestMethod.toUpperCase());
            Assertion.assertNotNull(im);
            JSONObject paramsJSON = json.getJSONObject("params");
            String ingestURL = null;
            if (paramsJSON.has("ingestURL")) {
                ingestURL = paramsJSON.getString("ingestURL");
            }
            SourceDescFileGenerator.SourceInfo si = new SourceDescFileGenerator.SourceInfo(srcId,
                    sourceName, dataSource, im);
            si.setRepositoryID(repositoryID);
            si.setIngestURL(ingestURL);
            for (String key : paramsJSON.keySet()) {
                if (!key.equals("ingestURL")) {
                    si.setContentSpecParam(key, paramsJSON.getString(key));
                }
            }
            // empty one initially as it depends on the data
            si.setPrimaryKeyJsonPath("");
            Source source = SourceDescFileGenerator.prepareSource(si, null);
            boolean saved = mongoService.saveSource(source);
            if (!saved) {
                return Response.status(Response.Status.BAD_REQUEST).build();
            }
            return Response.ok().build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.serverError().build();
        }
    }

    @Path("/update")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateSource(@FormParam("payload") String payload, @FormParam("apiKey") String apiKey) {
        MongoService mongoService;
        try {
            boolean ok = SecurityService.getInstance().isAuthenticated(apiKey);
            if (!ok) {
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            mongoService = MongoService.getInstance();
            JSONObject json = new JSONObject(payload);
            System.out.println(json.toString(2));
            String srcId = json.getString("sourceID");
            String dataSource = null;
            if (json.has("dataSource")) {
                dataSource = json.getString("dataSource");
            }
            String transformScript = null;
            List<String> pkJsonPaths = new ArrayList<String>(5);
            if (json.has("transformScript")) {
                transformScript = json.getString("transformScript");
            }
            if (json.has("pkJsonPath")) {
                JSONArray pkJsonPathArr = json.getJSONArray("pkJsonPath");
                for (int i = 0; i < pkJsonPathArr.length(); i++) {
                    pkJsonPaths.add(pkJsonPathArr.getString(i));
                }
            }

            boolean updated = mongoService.updateSource(srcId, dataSource, pkJsonPaths, transformScript);
            if (!updated) {
                return Response.status(Response.Status.BAD_REQUEST).build();
            }
            return Response.ok().build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.serverError().build();
        }
    }

    @Path("/source")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSource(@QueryParam("sourceID") String sourceID, @QueryParam("dataSource") String dataSource,
                              @QueryParam("apiKey") String apiKey) {
        MongoService mongoService;
        try {
            boolean ok = SecurityService.getInstance().isAuthenticated(apiKey);
            if (!ok) {
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            System.out.println("sourceID:" + sourceID + " dataSource:" + dataSource);
            mongoService = MongoService.getInstance();
            Source source = mongoService.findSource(sourceID, dataSource);
            JSONObject json = new JSONObject();
            json.put("name", source.getName());
            json.put("sourceID", source.getResourceID());
            json.put("dataSource", source.getDataSource());
            if (source.getRepositoryID() != null) {
                json.put("repositoryID", source.getRepositoryID());
            } else {
                json.put("repositoryID", "");
            }
            JSONObject paramsJSON = new JSONObject();
            json.put("params", paramsJSON);
            JSONObject ingestConfiguration = source.getIngestConfiguration();
            JSONObject contentSpecification = source.getContentSpecification();
            for (String key : contentSpecification.keySet()) {
                Object o = contentSpecification.get(key);
                if (!(o instanceof JSONObject) && !(o instanceof JSONArray)) {
                    paramsJSON.put(key, contentSpecification.getString(key));
                }
            }
            String ingestURLKey = "ingestURL";
            if (ingestConfiguration.has(ingestURLKey)) {
                paramsJSON.put(ingestURLKey, ingestConfiguration.getString(ingestURLKey));
            }

            String ingestMethod = ingestConfiguration.getString("ingestMethod");
            json.put("type", ingestMethod.toLowerCase());

            String jsonStr = json.toString(2);
            return Response.ok(jsonStr).build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.serverError().build();
        }
    }

    @Path("/")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSourceSummaries(@QueryParam("apiKey") String apiKey) {
        MongoService mongoService;
        try {
            boolean ok = SecurityService.getInstance().isAuthenticated(apiKey);
            if (!ok) {
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            mongoService = MongoService.getInstance();

            JSONArray jsArr = new JSONArray();
            List<JSONObject> sourceSummaries = mongoService.getSourceSummaries();
            int count = 0;
            for (Iterator<JSONObject> iter = sourceSummaries.iterator(); iter.hasNext(); ) {
                JSONObject ss = iter.next();
                if (ss.getString("sourceID").indexOf("cinergi") != -1) {
                    iter.remove();
                } else {
                    ss.put("id", count);
                    count++;
                    jsArr.put(ss);
                }
            }
            String jsonStr = jsArr.toString(2);
            return Response.ok(jsonStr).build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.serverError().build();
        }
    }

    @Path("/testTransform")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response testTransformOnSample(@FormParam("sourceId") String sourceId,
                                          @FormParam("dataSource") String dataSource,
                                          @FormParam("transformScript") String transformScript,
                                          @FormParam("apiKey") String apiKey) {

        try {
            boolean ok = SecurityService.getInstance().isAuthenticated(apiKey);
            if (!ok) {
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            String cacheKey = prepKey(sourceId, dataSource);
            JSONObject json = CacheManager.getInstance().getJSON(cacheKey);
            if (json == null) {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            JSONObject origData = json.getJSONObject("sample");
            TransformationEngine trEngine = new TransformationEngine(transformScript);
            JSONObject transformedJson = new JSONObject();
            trEngine.transform(origData, transformedJson);
            String jsonStr = transformedJson.toString(2);
            return Response.ok(jsonStr).build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.serverError().build();
        }
    }

    @Path("/sample")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSourceAndSampledData(@QueryParam("sourceId") String sourceId,
                                            @QueryParam("dataSource") String dataSource,
                                            @QueryParam("apiKey") String apiKey) {
        MongoService mongoService;
        try {
            boolean ok = SecurityService.getInstance().isAuthenticated(apiKey);
            if (!ok) {
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            logger.info("getSourceAndSampledData sourceId:" + sourceId + " dataSource:" + dataSource);
            mongoService = MongoService.getInstance();

            Source source = mongoService.findSource(sourceId, dataSource);
            if (source == null) {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            JSONObject resultJSON = new JSONObject();
            resultJSON.put("source", source.toJSON());

            CacheManager cache = CacheManager.getInstance();
            String cacheKey = prepKey(sourceId, dataSource);
            JSONObject cached = cache.getJSON(cacheKey);
            if (cached != null) {
                resultJSON = cached;
                if (resultJSON.has("sample")) {
                    resultJSON.put("source", source.toJSON());
                    cache.putJSON(cacheKey, resultJSON);

                    String jsonStr = resultJSON.toString(2);
                    return Response.ok(jsonStr).build();
                }
            }


            JSONObject icJSON = source.getIngestConfiguration();
            String ingestMethod = icJSON.getString("ingestMethod");
            String ingestUrl = null;
            if (icJSON.has("ingestURL")) {
                ingestUrl = icJSON.getString("ingestURL");
            }
            Map<String, String> options = new HashMap<String, String>();
            options.put("ingestURL", ingestUrl);
            options.put("sampleMode", "true");
            JSONObject csJSON = source.getContentSpecification();
            if (csJSON.has("sampleFile")) {
                File sampleFile = new File(csJSON.getString("sampleFile"));
                if (sampleFile.isDirectory()) {
                    File[] files = sampleFile.listFiles();
                    File theFile = null;
                    for (File f : files) {
                        if (f.isFile() && f.getName().endsWith(".json")) {
                            theFile = f;
                            break;
                        }
                    }
                    sampleFile = theFile;
                }
                if (sampleFile != null) {
                    String jsonStr = Utils.loadAsString(sampleFile.getAbsolutePath());
                    JSONObject recJSON = new JSONObject(jsonStr);
                    JSTreeJSONGenerator gen = new JSTreeJSONGenerator();
                    JSONObject jsTreeJSON = gen.toJSTree(recJSON);
                    resultJSON.put("sampleTree", jsTreeJSON);
                    resultJSON.put("sample", recJSON);

                    cache.putJSON(cacheKey, resultJSON);
                    jsonStr = resultJSON.toString(2);
                    return Response.ok(jsonStr).build();
                }

            }
            for (String key : csJSON.keySet()) {
                options.put(key, csJSON.getString(key));
            }

            ConsumerFactory consumerFactory = ConsumerFactory.getInstance(false);
            Ingestable consumer = (Ingestable) consumerFactory.createHarvester(ingestMethod, "genIngestor",
                    "nifRecords", options);
            Ingestor ingestor = consumer.getIngestor();
            try {
                logger.info("starting sampling...");
                ingestor.startup();
                if (ingestor.hasNext()) {
                    Result result = ingestor.prepPayload();
                    logger.info("sampled from " + sourceId);
                    JSONObject recJSON = result.getPayload();
                    // convert to jstree format
                    JSTreeJSONGenerator gen = new JSTreeJSONGenerator();
                    JSONObject jsTreeJSON = gen.toJSTree(recJSON);
                    resultJSON.put("sampleTree", jsTreeJSON);
                    resultJSON.put("sample", recJSON);
                }
            } finally {
                if (ingestor != null) {
                    ingestor.shutdown();
                }
            }

            cache.putJSON(cacheKey, resultJSON);

            String jsonStr = resultJSON.toString(2);
            return Response.ok(jsonStr).build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.serverError().build();
        }
    }

    public static String prepKey(String sourceId, String dataSource) {
        StringBuilder sb = new StringBuilder();
        sb.append(sourceId);
        if (dataSource != null) {
            sb.append(':').append(dataSource);
        }
        return sb.toString();
    }
}
