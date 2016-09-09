package org.neuinfo.foundry.ingestor.ws;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.XML2JSONConverter;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.StringReader;


/**
 * Created by bozyurt on 5/29/14.
 */
@Path("ingestor/xml2json")
public class XMLConverterResource {
    @POST
    @Consumes("application/xml")
    @Produces(MediaType.APPLICATION_JSON)
    public Response post(String content) {
        System.out.println("content:" + content);
        System.out.println("=============================");
        XML2JSONConverter converter = new XML2JSONConverter();
        SAXBuilder builder = new SAXBuilder();
        try {
            Document doc = builder.build(new StringReader(content));
            Element rootEl = doc.getRootElement();

            final JSONObject json = converter.toJSON(rootEl);
            String jsonStr = json.toString(2);
            System.out.println("json:\n" + jsonStr);
            System.out.println("=============================");

            return Response.ok(jsonStr).build();

        } catch (Exception x) {
            x.printStackTrace();
        }

        return null;
    }

}
