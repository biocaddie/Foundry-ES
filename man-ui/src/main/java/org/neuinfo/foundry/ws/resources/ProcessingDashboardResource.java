package org.neuinfo.foundry.ws.resources;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.DocProcessingStatsService;
import org.neuinfo.foundry.common.ingestion.DocProcessingStatsService.SourceStats;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.ws.common.CacheManager;
import org.neuinfo.foundry.ws.common.MessagingService;
import org.neuinfo.foundry.ws.common.MongoService;
import org.neuinfo.foundry.ws.common.SecurityService;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 1/4/16.
 */
@Path("dashboard")
public class ProcessingDashboardResource {

    @Path("/stats")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDocProcessingStats(@QueryParam("refresh") boolean refresh,
                                          @QueryParam("apiKey") String apiKey) {
        MongoService mongoService;
        try {
            boolean ok = SecurityService.getInstance().isAuthenticated(apiKey);
            if (!ok) {
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            mongoService = MongoService.getInstance();
            JSONArray jsArr = getSourceSummaries4Dashboard(mongoService, refresh);
            String jsonStr = jsArr.toString(2);
            return Response.ok(jsonStr).build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.serverError().build();
        }
    }

    JSONArray getSourceSummaries4Dashboard(MongoService mongoService, boolean forceRecache) throws IOException {
        List<SourceStats> ssList = (List<SourceStats>) CacheManager.getInstance().get("sourceStats");
        if (forceRecache || ssList == null) {
            DocProcessingStatsService dpss = new DocProcessingStatsService();
            dpss.setMongoClient(mongoService.getMongoClient());
            dpss.setDbName(mongoService.getDbName());
            ssList = dpss.getDocCountsPerStatusPerSource2("nifRecords");
            CacheManager.getInstance().put("sourceStats", (Serializable) ssList);

        }
        List<JSONObject> sourceSummaries = mongoService.getSourceSummaries4Dashboard();
        Map<String, JSONObject> sourceMap = new HashMap<String, JSONObject>();
        for (JSONObject json : sourceSummaries) {
            sourceMap.put(json.getString("sourceID"), json);
        }
        JSONArray jsArr = new JSONArray();
        for (SourceStats ss : ssList) {
            JSONObject json = new JSONObject();
            jsArr.put(json);
            json.put("sourceID", ss.getSourceID());
            JSONObject sourceJson = sourceMap.get(ss.getSourceID());
            Assertion.assertNotNull(sourceJson);
            json.put("name", sourceJson.getString("name"));
            json.put("dataSource", sourceJson.getString("dataSource"));
            Map<String, Integer> statusCountMap = ss.getStatusCountMap();
            JSONObject statusJson = new JSONObject();
            json.put("statusCounts", statusJson);
            int total = 0;
            for (String statusValue : statusCountMap.keySet()) {
                int count = statusCountMap.get(statusValue);
                statusJson.put(statusValue, count);
                total += count;
            }
            json.put("total", total);
            JSONArray batchInfos = sourceJson.getJSONArray("batchInfos");
            JSONObject theLatestBatchInfo = findTheLatestBatchInfo(batchInfos);
            // Assertion.assertNotNull(theLatestBatchInfo);
            if (theLatestBatchInfo != null) {
                json.put("batchInfo", theLatestBatchInfo);
            } else {
                json.put("batchInfo" , new JSONObject());
            }
        }
        return jsArr;
    }

    @Path("/start")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response startProcessing(@FormParam("sourceID") String sourceID, @FormParam("apiKey") String apiKey) {
        MongoService mongoService;
        try {
            boolean ok = SecurityService.getInstance().isAuthenticatedAndHasRole(apiKey, "admin");
            if (!ok) {
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            mongoService = MongoService.getInstance();

            mongoService.ingestSource(sourceID);
            JSONArray ssJsonArr = getSourceSummaries4Dashboard(mongoService, true);
            String jsonStr = ssJsonArr.toString(2);
            return Response.ok(jsonStr).build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.serverError().build();
        }
    }


    public static JSONObject findTheLatestBatchInfo(JSONArray biList) {
        SimpleDateFormat sdf = new SimpleDateFormat("MMddyyyy-HH:mm");
        JSONObject latestBI = null;
        Date latestDate = null;
        for (int i = 0; i < biList.length(); i++) {
            JSONObject js = biList.getJSONObject(i);
            String batchId = js.getString("batchId");
            try {
                Date d = sdf.parse(batchId);
                if (latestDate == null || latestDate.before(d)) {
                    latestBI = js;
                    latestDate = d;
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return latestBI;
    }
}
