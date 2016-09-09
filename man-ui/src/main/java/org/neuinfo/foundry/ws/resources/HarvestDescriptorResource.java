package org.neuinfo.foundry.ws.resources;

import com.wordnik.swagger.annotations.Api;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.transform.SourceDescFileGenerator;
import org.neuinfo.foundry.common.transform.SourceDescFileGenerator.SourceInfo;
import org.neuinfo.foundry.common.transform.SourceDescFileGenerator.IngestMethod;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.ws.common.MongoService;
import org.neuinfo.foundry.ws.common.SecurityService;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Created by bozyurt on 8/25/15.
 * <pre>
 *     localhost:8080/foundry/api/harvest/
 * </pre>
 */
@Path("harvest")
@Api(value = "harvest", description = "Harvest Descriptions")
public class HarvestDescriptorResource {
    @Path("/")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getIngestorConfigs(@QueryParam("apiKey") String apiKey) {
        try {
            boolean ok = SecurityService.getInstance().isAuthenticated(apiKey);
            if (!ok) {
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            List<JSONObject> allIngestorConfigs = MongoService.getInstance().getAllIngestorConfigs();
            JSONArray jsArr = new JSONArray();
            for (JSONObject json : allIngestorConfigs) {
                jsArr.put(json);
            }
            String jsonStr = jsArr.toString(2);
            return Response.ok(jsonStr).build();
        } catch (Exception x) {
            x.printStackTrace();
            return Response.serverError().build();
        }
    }

    @Path("/save")
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_JSON)
    public Response saveSource(@FormParam("source") String srcJSON, @FormParam("apiKey") String apiKey) {
        if (srcJSON == null) {
            throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).build());
        }
        boolean ok = SecurityService.getInstance().isAuthenticated(apiKey);
        if (!ok) {
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        JSONObject json = new JSONObject(srcJSON);
        System.out.println(json.toString(2));
        MongoService mongoService;
        try {
            mongoService = MongoService.getInstance();
            String sourceID = json.getString("sourceID");
            String resourceName = json.getString("resourceName");
            String dataSource = json.getString("dataSource");
            String ingestorType = json.getString("ingestorType");
            IngestMethod im = toIngestMethod(ingestorType);
            Assertion.assertNotNull(im);
            SourceInfo si = new SourceInfo(sourceID, resourceName, dataSource, toIngestMethod(ingestorType));
            JSONArray jsArr = json.getJSONArray("params");
            for (int i = 0; i < jsArr.length(); i++) {
                JSONObject paramJson = jsArr.getJSONObject(i);
                String name = paramJson.getString("name");
                String value = paramJson.getString("value");
                if (name.equalsIgnoreCase("ingesturl")) {
                    si.setIngestURL(value);
                } else {
                    si.setContentSpecParam(name, value);
                }
            }
            Source source = SourceDescFileGenerator.prepareSource(si, null);
            mongoService.saveSource(source);
            return Response.ok().build();
        } catch (Throwable t) {
            t.printStackTrace();
            return Response.serverError().build();
        }
    }

    static IngestMethod toIngestMethod(String ingestorType) {
        if (ingestorType.equals("csv")) {
            return IngestMethod.CSV;
        } else if (ingestorType.equals("xml")) {
            return IngestMethod.XML;
        }
        return null;
    }
}
