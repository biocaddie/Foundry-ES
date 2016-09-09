package org.neuinfo.foundry.ws.resources;

import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neuinfo.foundry.ws.Main;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by bozyurt on 8/27/15.
 */
public class SourceManagerResourceTest {
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
        Main.stopServer();
    }

    @Test
    public void testGetSourceAndSampledData() throws Exception {
        Response response = target.path("sources/sample")
                .queryParam("sourceId", "nif-0000-00241").request(MediaType.APPLICATION_JSON_TYPE).get();
        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
    }

    @Test
    public void testGetSourceSummaries() throws Exception {
        Response response = target.path("/sources").request(MediaType.APPLICATION_JSON_TYPE).get();
        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
    }
}
