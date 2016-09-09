package org.neuinfo.foundry.ws;

import org.glassfish.jersey.server.ResourceConfig;

/**
 * Created by bozyurt on 8/25/15.
 */
public class ManagementWSApp extends ResourceConfig {
    public ManagementWSApp() {
        packages("org.neuinfo.foundry.ws.resources");
    }
}
