package org.neuinfo.foundry.ws;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.neuinfo.foundry.ws.common.BootstrapHelper;
import org.neuinfo.foundry.ws.common.CacheManager;
import org.neuinfo.foundry.ws.common.MongoService;

import java.io.IOException;
import java.net.URI;

/**
 * Created by bozyurt on 8/27/15.
 */
public class Main {
    // Base URI the Grizzly HTTP server will listen on
    public static final String BASE_URI = "http://localhost:7080/foundry/";
    static MongoService mongoService;
    static HttpServer server;

    public static HttpServer startServer() throws Exception {
        ManagementWSApp app = new ManagementWSApp();
        mongoService = MongoService.getInstance();
        CacheManager cacheManager = CacheManager.getInstance();
        BootstrapHelper bh = new BootstrapHelper(mongoService);
        bh.bootstrap();
        server = GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), app);
        return server;
    }

    public static void stopServer() {
        if (server != null) {
            server.shutdownNow();
            mongoService.shutdown();
        }
        System.out.println("shutting down cache manager");
        try {
            CacheManager.getInstance().shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
