package org.neuinfo.foundry.ws.resources;

import org.json.JSONObject;
import org.neuinfo.foundry.common.transform.IdentityTransformationGenerator;
import org.neuinfo.foundry.common.transform.SyntaxException;
import org.neuinfo.foundry.common.transform.TransformationEngine;
import org.neuinfo.foundry.common.transform.TransformationFunctionRegistry;
import org.neuinfo.foundry.common.util.JSTreeJSONGenerator;
import org.neuinfo.foundry.ws.common.SecurityService;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by bozyurt on 1/15/16.
 */

@Path("transformation")
public class TransformationResource {
    @Path("/transform")
    @POST
    @Consumes("application/x-www-form-urlencoded")
    @Produces(MediaType.APPLICATION_JSON)
    public Response transform(@FormParam("jsonInput") String jsonInput,
                              @FormParam("transformationScript") String transformationScript,
                              @FormParam("apiKey") String apiKey) {
        try {
            boolean authenticated = SecurityService.getInstance().isAuthenticated(apiKey);
            if (!authenticated) {
                return Response.status(Response.Status.FORBIDDEN).build();
            }

            JSONObject input = new JSONObject(jsonInput);

            TransformationEngine trEngine = new TransformationEngine(transformationScript);
            JSONObject transformedJson = new JSONObject();
            trEngine.transform(input, transformedJson);
            String jsonStr = transformedJson.toString(2);
            return Response.ok(jsonStr).build();

        } catch (Throwable t) {
            t.printStackTrace();
            if (t instanceof SyntaxException) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(t.getMessage()).type(MediaType.TEXT_PLAIN).build();
            }
            return Response.serverError().build();
        }
    }

    @Path("/genIdentityTransform")
    @POST
    @Consumes("application/x-www-form-urlencoded")
    @Produces(MediaType.APPLICATION_JSON)
    public Response generateIdentityTransform(@FormParam("jsonInput") String jsonInput,
                                              @FormParam("apiKey") String apiKey) {
        try {
            boolean authenticated = SecurityService.getInstance().isAuthenticated(apiKey);
            if (!authenticated) {
                return Response.status(Response.Status.FORBIDDEN).build();
            }

            JSONObject input = new JSONObject(jsonInput);
            IdentityTransformationGenerator itg = new IdentityTransformationGenerator();

            String transformScript = itg.generateTransformScript(input);
            JSONObject result = new JSONObject();
            result.put("identityTrScript", transformScript);
            return Response.ok(result.toString(2)).build();

        } catch (Throwable t) {
            t.printStackTrace();
            return Response.serverError().build();
        }
    }

    @Path("/testrule")
    @POST
    @Consumes("application/x-www-form-urlencoded")
    @Produces(MediaType.APPLICATION_JSON)
    public Response testTransformRule(@FormParam("jsonInput") String jsonInput,
                                      @FormParam("transformationRule") String transformationRule,
                                      @FormParam("apiKey") String apiKey) {
        try {
            boolean authenticated = SecurityService.getInstance().isAuthenticated(apiKey);
            if (!authenticated) {
                return Response.status(Response.Status.FORBIDDEN).build();
            }

            JSONObject input = new JSONObject(jsonInput);

            TransformationEngine trEngine = new TransformationEngine(transformationRule);
            JSONObject transformedJson = new JSONObject();
            trEngine.transform(input, transformedJson);
            // convert to jstree format
            JSTreeJSONGenerator gen = new JSTreeJSONGenerator();
            JSONObject jsTreeJSON = gen.toJSTree(transformedJson);
            JSONObject resultJSON = new JSONObject();
            resultJSON.put("ruleTrTree", jsTreeJSON);
            resultJSON.put("ruleTrJSON", transformedJson);

            String jsonStr = resultJSON.toString(2);
            return Response.ok(jsonStr).build();
        } catch (Throwable t) {
            t.printStackTrace();
            if (t instanceof SyntaxException) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(t.getMessage()).type(MediaType.TEXT_PLAIN).build();
            }
            return Response.serverError().build();
        }
    }
}
