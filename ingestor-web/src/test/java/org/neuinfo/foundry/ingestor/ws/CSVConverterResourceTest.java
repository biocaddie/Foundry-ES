package org.neuinfo.foundry.ingestor.ws;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.grizzly.http.server.HttpServer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.ingestor.ws.Main;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class CSVConverterResourceTest {

    private HttpServer server;
    private WebTarget target;
    private Client c;

    @Before
    public void setUp() throws Exception {
        // start the server
        server = Main.startServer();
        // create the client
        c = ClientBuilder.newClient();

        // uncomment the following line if you want to enable
        // support for JSON in the client (you also have to uncomment
        // dependency on jersey-media-json module in pom.xml and Main.startServer())
        // --
        // c.configuration().enable(new org.glassfish.jersey.media.json.JsonJaxbFeature());

        target = c.target(Main.BASE_URI);
    }

    @After
    public void tearDown() throws Exception {
        server.shutdownNow();
    }


    // public void testGetIt() {
    //     String responseMsg = target.path("myresource").request().get(String.class);
    //     assertEquals("Got it!", responseMsg);
    // }

    @Test
    public void testPost() throws IOException {
        String csvContent = Utils.loadAsString("/tmp/antibodies.csv");
        final Response response = target.path("ingestor/csv2json").queryParam("hasHeader", "true")
                .request(MediaType.APPLICATION_JSON_TYPE).post(Entity.text(csvContent));

        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
    }
}
