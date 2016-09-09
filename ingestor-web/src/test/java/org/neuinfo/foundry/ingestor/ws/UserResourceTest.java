package org.neuinfo.foundry.ingestor.ws;

import org.glassfish.grizzly.http.server.HttpServer;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neuinfo.foundry.common.model.User;

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
public class UserResourceTest {

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
    public void testAddUser() throws IOException {
        User.Builder builder = new User.Builder("bozyurt", "pwd", "iozyurt@ucsd.edu");
        User user = builder.firstName("Burak").lastName("Ozyurt").build();
        JSONObject js = user.toJSON();
        Response response = target.path("cinergi/user").queryParam("action", "create")
                .request(MediaType.APPLICATION_JSON_TYPE).post(Entity.text(js.toString(2)));

        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
    }

    @Ignore
    public void testRemoveUser() throws IOException {
        JSONObject js = new JSONObject();
        js.put("id", "5441648ee4b073d206ec500e");
        String content = js.toString(2);
        Response response = target.path("cinergi/user").queryParam("action", "delete")
                .request(MediaType.APPLICATION_JSON_TYPE).post(Entity.text(content));

        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
    }

    @Test
    public void testFindUser() throws IOException {
       Response response = target.path("cinergi/user/search")
                .queryParam("username", "bozyurt")
                .request(MediaType.APPLICATION_JSON_TYPE).get();
        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
    }

    @Test
    public void testFindUserById() throws IOException {
        Response response = target.path("cinergi/user/5441672ae4b0a517fd255dc8")
                .request(MediaType.APPLICATION_JSON_TYPE).get();
        System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
    }
}