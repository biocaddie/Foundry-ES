package org.neuinfo.foundry.ws.resources;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.Constants;
import org.neuinfo.foundry.common.model.User;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.ws.common.MongoService;
import org.neuinfo.foundry.ws.common.SecurityService;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Created by bozyurt on 10/20/15.
 */

@Path("user")
public class UserResource {

    @Path("/login")
    @POST
    @Consumes("application/x-www-form-urlencoded")
    @Produces(MediaType.APPLICATION_JSON)
    public Response authenticate(@FormParam("user") String userName, @FormParam("pwd") String password) {
        System.out.println("authenticate user:" + userName);
        try {
            String sessionKey = SecurityService.getInstance().authenticate(userName, password);

            JSONObject json = new JSONObject();
            if (sessionKey == null) {
                json.put("error", "No record found matching the username and password!");
            } else {
                // StringBuilder sb = new StringBuilder(80);
                // sb.append(password).append(':').append(Constants.SALT).append(':').append(userName);
                // String apiKey = Utils.getMD5ChecksumOfString(sb.toString());
                json.put("apiKey", sessionKey);
            }
            String jsonStr = json.toString(2);
            return Response.ok(jsonStr).build();
        } catch (Throwable t) {
            return getErrorResponse(t);
        }
    }

    @Path("/logout")
    @POST
    @Consumes("application/x-www-form-urlencoded")
    @Produces(MediaType.APPLICATION_JSON)
    public Response logOut(@FormParam("user") String userName) {
        try {
            SecurityService.getInstance().logout(userName);
            return Response.ok().build();
        } catch (Throwable t) {
            return getErrorResponse(t);
        }
    }

    @Path("/users")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getUsers(@QueryParam("apiKey") String apiKey) {
        try {
            boolean ok = SecurityService.getInstance().isAuthenticatedAndHasRole(apiKey, "admin");
            if (!ok) {
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            List<User> allUsers = MongoService.getInstance().getAllUsers();
            JSONArray jsArr = new JSONArray();
            for (User user : allUsers) {
                jsArr.put(user.toJSON());
            }
            String jsonStr = jsArr.toString(2);
            return Response.ok(jsonStr).build();
        } catch (Throwable t) {
            return getErrorResponse(t);
        }
    }

    Response getErrorResponse(Throwable t) {
        t.printStackTrace();
        return Response.serverError().build();
    }


    @Path("/users/{username}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response findUser(@PathParam("username") String userName, @QueryParam("apiKey") String apiKey) {
        try {
            boolean ok = SecurityService.getInstance().isAuthenticatedAndHasRole(apiKey, "admin");
            if (!ok) {
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            User user = MongoService.getInstance().findUser(userName, null);
            if (user == null) {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            String jsonStr = user.toJSON().toString(2);
            return Response.ok(jsonStr).build();
        } catch (Throwable t) {
            return getErrorResponse(t);
        }
    }

    @Path("/users")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response saveUser(@FormParam("payload") String payload, @FormParam("apiKey") String apiKey) {
        MongoService mongoService;
        try {
            boolean ok = SecurityService.getInstance().isAuthenticatedAndHasRole(apiKey, "admin");
            if (!ok) {
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            JSONObject json = new JSONObject(payload);
            System.out.println(json.toString(2));
            String opMode = json.getString("opMode");
            JSONObject userJS = json.getJSONObject("user");
            User user = User.fromJSON(userJS);
            mongoService = MongoService.getInstance();
            User existingUser = mongoService.findUser(user.getUsername(), null);
            if (opMode.equals("new") && existingUser != null) {
                return Response.status(Response.Status.BAD_REQUEST).build();
            }
            if (opMode.equals("update") && existingUser == null) {
                return Response.status(Response.Status.BAD_REQUEST).build();
            }

            mongoService.saveUser(user);

            return Response.ok().build();
        } catch (Throwable t) {
            return getErrorResponse(t);
        }
    }


}
