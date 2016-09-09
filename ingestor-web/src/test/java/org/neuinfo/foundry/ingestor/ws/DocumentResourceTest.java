package org.neuinfo.foundry.ingestor.ws;

import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by bozyurt on 2/9/15.
 */
public class DocumentResourceTest {
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

    @Test
    public void testGetDocumentIds() throws Exception {
        Response response = target.path("cinergi/docs/cinergi-0006").request(MediaType.APPLICATION_JSON_TYPE).get();
        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
    }

    @Test
    public void testGetDocument() throws Exception {
        Response response = target.path("cinergi/docs/cinergi-0006/gov.noaa.nodc:7301155").request(MediaType.APPLICATION_XML).get();
        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
    }
}
