package org.neuinfo.foundry.ingestor.ws.dm;

import com.wordnik.swagger.annotations.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.ingestor.ws.MongoService;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Created by bozyurt on 7/17/14.
 */
@Path("cinergi/sources")
@Api(value = "cinergi/sources", description = "Metadata Sources")
public class SourceResource {

    @Path("/{resourceId}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 500, message = "An internal error occurred during source retrieval"),
            @ApiResponse(code = 404, message = "No source is found with the given resource ID")})
    @ApiOperation(value = "Retrieve information about the requested harvest source",
            notes = "",
            response = String.class)
    public Response getSource(@ApiParam(value = "The resource ID for the harvest source", required = true)
                              @PathParam("resourceId") String resourceId) {
        try {
            final JSONObject js = MongoService.getInstance().getSource(resourceId);
            if (js == null) {
                return Response.status(Response.Status.NOT_FOUND).entity("No source with the resourceId:" + resourceId).build();
            }
            String jsonStr = js.toString(2);
            return Response.ok(jsonStr).build();
        } catch (Exception x) {
            x.printStackTrace();
            return Response.serverError().build();
        }
    }

    @Path("/")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 500, message = "An internal error occurred during source list retrieval")})
    @ApiOperation(value = "Retrieve information about all sources",
            notes = "",
            response = String.class)
    public Response getAllSources() {
        try {
            final List<JSONObject> allSources = MongoService.getInstance().getAllSources();
            JSONArray jsArr = new JSONArray();
            for (JSONObject js : allSources) {
                if (!js.has("sourceInformation")) {
                    continue;
                }
                JSONObject siJson = js.getJSONObject("sourceInformation");
                if (siJson.getString("resourceID").startsWith("cinergi-")) {
                    jsArr.put(js);
                }
            }
            String jsonStr = jsArr.toString(2);

            return Response.ok(jsonStr).build();
        } catch (Exception x) {
            x.printStackTrace();
            return Response.serverError().build();
        }
    }
}

