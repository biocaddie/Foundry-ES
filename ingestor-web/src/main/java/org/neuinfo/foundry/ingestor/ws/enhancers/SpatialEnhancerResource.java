package org.neuinfo.foundry.ingestor.ws.enhancers;

import com.wordnik.swagger.annotations.*;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.CinergiXMLUtils;
import org.neuinfo.foundry.common.util.Utils;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.StringWriter;
import java.net.URI;

/**
 * Created by bozyurt on 1/16/15.
 */
@Path("cinergi/enhancers/spatial")
@Api(value = "cinergi/enhancers/spatial", description = "Spatial enhancement of ISO XML Metadata documents")
public class SpatialEnhancerResource {
    private static String serverURL = "http://photon.sdsc.edu:8080/cinergi/SpatialEnhancer";

    @POST
    @Consumes({"application/xml"})
    @Produces({"application/xml"})
    @ApiOperation(value = "Enhance an ISO Metadata Document with spatial extent",
            notes = "Uses the abstract and title text in the given metadata document to detect the spatial extent",
            response = String.class)
    @ApiResponses(value = {@ApiResponse(code = 400, message = "no ISO XML document is supplied"),
            @ApiResponse(code = 500, message = "An internal error occurred during spatial enhancement")})
    public Response post(@ApiParam(value = "ISO Metadata XML document for spatial enhancements", required = true)
                         String isoMetaXml) {
        if (isoMetaXml == null) {
            throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).build());
        }
        try {
            JSONObject spatialJson = callSpatialEnhancer(serverURL, isoMetaXml);
            Element docEl = Utils.readXML(isoMetaXml);
            docEl = addSpatialExtent(docEl, spatialJson);
            XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
            StringWriter sw = new StringWriter(isoMetaXml.length());
            xout.output(docEl, sw);
            return Response.ok(sw.toString()).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().build();
        }
    }

    public static JSONObject callSpatialEnhancer(String serverURL, String origDocXmlStr) throws Exception {
        HttpClient client = new DefaultHttpClient();

        URIBuilder builder = new URIBuilder(serverURL);

        URI uri = builder.build();
        System.out.println("uri:" + uri);
        HttpPost httpPost = new HttpPost(uri);
        boolean ok = false;
        try {
            httpPost.addHeader("Accept", "application/json");
            httpPost.addHeader("Content-Type", "application/xml");
            StringEntity entity = new StringEntity(origDocXmlStr, "UTF-8");
            httpPost.setEntity(entity);
            final HttpResponse response = client.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200 || statusCode == 201) {
                ok = true;
                final HttpEntity responseEntity = response.getEntity();
                if (entity != null) {
                    String jsonStr = EntityUtils.toString(responseEntity);
                    return new JSONObject(jsonStr);
                }
            } else {
                System.out.println(response.getStatusLine());
            }
        } finally {
            if (httpPost != null) {
                httpPost.releaseConnection();
            }
        }
        return null;
    }

    Element addSpatialExtent(Element docEl, JSONObject spatial) throws Exception {
        Namespace gmd = Namespace.getNamespace("gmd", "http://www.isotc211.org/2005/gmd");
        JSONArray boundingBoxes = spatial.getJSONArray("bounding_boxes");
        boolean hasBB = boundingBoxes.length() > 0;
        boolean hasBBFromPlaces = false;

        Element identificationInfo = docEl.getChild("identificationInfo", gmd);
        Element dataIdentification = identificationInfo.getChild("MD_DataIdentification", gmd);
        if (dataIdentification == null) {
            dataIdentification = new Element("MD_DataIdentification", gmd);
            identificationInfo.addContent(dataIdentification);
        }

        if (!hasBB) {
            JSONObject derivedBoundingBoxes = spatial.getJSONObject("derived_bounding_boxes_from_places");
            if (derivedBoundingBoxes.length() > 0) {
                for (String place : derivedBoundingBoxes.keySet()) {
                    JSONObject placeJson = derivedBoundingBoxes.getJSONObject(place);
                    JSONObject swJson = placeJson.getJSONObject("southwest");
                    JSONObject neJson = placeJson.getJSONObject("northeast");
                    String wbLongVal = String.valueOf(swJson.getDouble("lng"));
                    String sblatVal = String.valueOf(swJson.getDouble("lat"));
                    String ebLongVal = String.valueOf(neJson.getDouble("lng"));
                    String nbLatVal = String.valueOf(neJson.getDouble("lat"));
                    Element bbEl = CinergiXMLUtils.createBoundaryBox(wbLongVal, ebLongVal, sblatVal, nbLatVal, place);
                    dataIdentification.addContent(bbEl);
                }
                hasBBFromPlaces = true;
            }
        }
        if (!hasBB && !hasBBFromPlaces) {
            JSONObject derivedBoundingBoxes = spatial.getJSONObject("derived_bounding_boxes_from_derived_place");
            if (derivedBoundingBoxes.length() > 0) {
                for (String place : derivedBoundingBoxes.keySet()) {
                    JSONObject placeJson = derivedBoundingBoxes.getJSONObject(place);
                    JSONObject swJson = placeJson.getJSONObject("southwest");
                    JSONObject neJson = placeJson.getJSONObject("northeast");
                    String wbLongVal = String.valueOf(swJson.getDouble("lng"));
                    String sblatVal = String.valueOf(swJson.getDouble("lat"));
                    String ebLongVal = String.valueOf(neJson.getDouble("lng"));
                    String nbLatVal = String.valueOf(neJson.getDouble("lat"));
                    Element bbEl = CinergiXMLUtils.createBoundaryBox(wbLongVal, ebLongVal, sblatVal, nbLatVal, place);
                    dataIdentification.addContent(bbEl);
                }
            }
        }
        return docEl;
    }

}
