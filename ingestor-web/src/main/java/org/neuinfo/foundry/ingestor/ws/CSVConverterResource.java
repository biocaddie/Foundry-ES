package org.neuinfo.foundry.ingestor.ws;

import org.json.JSONObject;
import org.neuinfo.foundry.common.util.CSV2JSONConverter;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by bozyurt on 5/29/14.
 */
@Path("ingestor/csv2json")
public class CSVConverterResource {

    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response post(String content, @QueryParam("hasHeader") boolean hasHeader) {
        System.out.println("hasHeader:" + hasHeader);
        System.out.println("content:" + content);
        System.out.println("=============================");
        CSV2JSONConverter converter = new CSV2JSONConverter(hasHeader);
        try {
            final JSONObject json = converter.toJSONFromString(content);
            String jsonStr = json.toString(2);

            return Response.ok(jsonStr).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
