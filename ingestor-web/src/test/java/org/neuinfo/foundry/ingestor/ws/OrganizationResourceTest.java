package org.neuinfo.foundry.ingestor.ws;

import org.glassfish.grizzly.http.server.HttpServer;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

/**
 * Created by bozyurt on 10/17/14.
 */
public class OrganizationResourceTest {
    private HttpServer server;
    private WebTarget target;
    private Client c;


    @Before
    public void setup() throws Exception {
        server = Main.startServer();
        c = ClientBuilder.newClient();
        target = c.target(Main.BASE_URI);
    }

    @After
    public void tearDown() throws Exception {
        server.shutdownNow();
    }

    @Ignore
    public void testAddOrg() throws IOException {
        JSONObject js = new JSONObject();
        js.put("organization-name", "UCSD");
        String content = js.toString(2);
        Response response = target.path("cinergi/organization").queryParam("action", "create")
                .request(MediaType.APPLICATION_JSON_TYPE).post(Entity.text(content));

        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
    }

    @Ignore
    public void testDeleteOrg() throws IOException {
        JSONObject js = new JSONObject();
        js.put("id", "54415906e4b0e995a4e078d4");
        String content = js.toString(2);
        Response response = target.path("cinergi/organization").queryParam("action", "delete")
                .request(MediaType.APPLICATION_JSON_TYPE).post(Entity.text(content));

        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
    }

    @Test
    public void testFindOrg() throws IOException {
        Response response = target.path("cinergi/organization/search")
                .queryParam("organization-name", "UCSD")
                .request(MediaType.APPLICATION_JSON_TYPE).get();
        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
    }
}
