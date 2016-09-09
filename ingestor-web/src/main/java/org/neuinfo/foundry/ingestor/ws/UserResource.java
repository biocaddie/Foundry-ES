package org.neuinfo.foundry.ingestor.ws;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.User;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by bozyurt on 10/17/14.
 */
@Path("cinergi")
//@Api(value="cinergi/user", description="Operations about users")
public class UserResource {
    @POST
    @Path("user")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
//    @ApiOperation(value="Add a new user")
    public Response post(String content, @QueryParam("action") String action) {
        System.out.println("action:" + action);
        System.out.println("content:" + content);
        System.out.println("=============================");
        try {
            JSONObject userJSON = new JSONObject(content);
            if (action.equals("create")) {
                User user = User.fromJSON(userJSON);
                ObjectId objectId = MongoService.getInstance().saveUser(user);
                JSONObject js = new JSONObject();
                js.put("id", objectId.toHexString());
                return Response.ok(js.toString(2)).build();
            } else if (action.equals("delete")) {
                String username = userJSON.has("username") ? userJSON.getString("username") : null;
                String id = userJSON.has("id") ? userJSON.getString("id") : null;
                MongoService.getInstance().removeUser(username, id);
                return Response.ok().build();
            }
        } catch (Exception x) {
            x.printStackTrace();
        }
        return Response.serverError().build();
    }

    @Path("user/{userId}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
//    @ApiOperation(value="Find the user with the given userId")
    public Response getUser(@PathParam("userId") String userId) {
        try {
            User user = MongoService.getInstance().findUser(null, userId);
            if (user == null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("No user with the id:" + userId).build();
            }
            JSONObject js = user.toJSON();
            return Response.ok(js.toString(2)).build();
        } catch (Exception x) {
            x.printStackTrace();
            return Response.serverError().build();
        }
    }

    @Path("user/search")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response searchOrganization(@QueryParam("username") String username) {
        try {
            User user = MongoService.getInstance().findUser(username, null);
            if (user == null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("No user with the username:" + username).build();
            }
            JSONObject js = user.toJSON();
            return Response.ok(js.toString(2)).build();
        } catch (Exception x) {
            x.printStackTrace();
            return Response.serverError().build();
        }
    }
}
