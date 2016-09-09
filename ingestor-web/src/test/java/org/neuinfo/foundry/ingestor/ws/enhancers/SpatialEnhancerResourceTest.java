package org.neuinfo.foundry.ingestor.ws.enhancers;

import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.ingestor.ws.Main;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by bozyurt on 1/20/15.
 */
public class SpatialEnhancerResourceTest {
    private HttpServer server;
    private WebTarget target;
    private Client c;
    static final String HOME_DIR = System.getProperty("user.home");

    @Before
    public void setUp() throws Exception {
        // start the server
        server = Main.startServer();
        // create the client
        c = ClientBuilder.newClient();
        target = c.target(Main.BASE_URI);
    }

    @After
    public void tearDown() throws Exception {
        server.shutdownNow();
    }

    @Test
    public void testPost() throws Exception {
        String sampleDocFile = HOME_DIR + "/etc/hydro10/ScienceBase/018F7983-78AA-4368-989A-B84F4FEB36D9.xml";
        String isoXmlInput = Utils.loadAsString(sampleDocFile);
        Response response = target.path("cinergi/enhancers/spatial").request(MediaType.APPLICATION_XML).post(Entity.xml(isoXmlInput));

        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
    }
}
