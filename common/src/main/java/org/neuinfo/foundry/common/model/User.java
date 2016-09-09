package org.neuinfo.foundry.common.model;

import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.JSONUtils;

/**
 * Created by bozyurt on 10/16/14.
 */
public class User {
    private String objectId;
    private String username;
    private String password;
    private String firstName;
    private String middleName;
    private String lastName;
    private String role;
    private String email;


    private User(Builder builder) {
        this.objectId = builder.objectId;
        this.username = builder.username;
        this.password = builder.password;
        this.firstName = builder.firstName;
        this.middleName = builder.middleName;
        this.lastName = builder.lastName;
        this.email = builder.email;
        this.role = builder.role;
    }

    public String getObjectId() {
        return objectId;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getMiddleName() {
        return middleName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getEmail() {
        return email;
    }

    public String getRole() {
        return role;
    }

    public JSONObject toJSON() {
        JSONObject js = new JSONObject();
        js.put("username", username);
        js.put("password", password);
        js.put("role", role);

        JSONUtils.add2JSON(js, "email", email);
        JSONUtils.add2JSON(js, "firstName", firstName);
        JSONUtils.add2JSON(js, "middleName", middleName);
        JSONUtils.add2JSON(js, "lastName", lastName);

        return js;
    }

    public static User fromJSON(JSONObject js) {
        String userName = js.getString("username");
        String pwd = js.getString("password");
        String email = js.getString("email");
        String role = "curator";

        Builder builder = new Builder(userName, pwd, email);
        if (js.has("firstName")) {
            builder.firstName(js.getString("firstName"));
        }
        if (js.has("middleName")) {
            builder.middleName(js.getString("middleName"));
        }
        if (js.has("lastName")) {
            builder.lastName(js.getString("lastName"));
        }
        if (js.has("role")) {
            role = js.getString("role");
        }
        builder.role(role);
        return builder.build();
    }

    public static User fromDBObject(DBObject userDBO) {
        String userName = (String) userDBO.get("username");
        String password; // = (String) userDBO.get("password");
        String email = (String) userDBO.get("email");
        password = "";
        String role = "curator";
        if (userDBO.get("role") != null) {
            role = (String) userDBO.get("role");
        }
        Builder builder = new Builder(userName, password, email);

        User user = builder.firstName((String) userDBO.get("firstName"))
                .middleName((String) userDBO.get("middleName"))
                .lastName((String) userDBO.get("lastName"))
                .id(((ObjectId) userDBO.get("_id")).toHexString())
                .role(role).build();

        return user;
    }

    public static class Builder {
        private String objectId;
        private String username;
        private String password;
        private String firstName;
        private String middleName;
        private String lastName;
        private String email;
        private String role;

        public Builder(String username, String password, String email) {
            this.username = username;
            this.password = password;
            this.email = email;
        }

        public Builder firstName(String firstName) {
            this.firstName = firstName;
            return this;
        }

        public Builder middleName(String middleName) {
            this.middleName = middleName;
            return this;
        }

        public Builder lastName(String lastName) {
            this.lastName = lastName;
            return this;
        }

        public Builder id(String objectId) {
            this.objectId = objectId;
            return this;
        }

        public Builder role(String role) {
            this.role = role;
            return this;
        }

        public User build() {
            return new User(this);
        }
    }
}
