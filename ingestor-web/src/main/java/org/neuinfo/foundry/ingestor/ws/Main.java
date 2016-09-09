package org.neuinfo.foundry.ingestor.ws;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.IOException;
import java.net.URI;

/**
 * Main class.
 *
 */
public class Main {
    // Base URI the Grizzly HTTP server will listen on
    public static final String BASE_URI = "http://localhost:7080/foundry/";
    static MongoService mongoService;

    /**
     * Starts Grizzly HTTP server exposing JAX-RS resources defined in this application.
     * @return Grizzly HTTP server.
     */
    public static HttpServer startServer() throws Exception {
        // create a resource config that scans for JAX-RS resources and providers
        // in org.neuinfo.foundry package
       // final ResourceConfig rc = new ResourceConfig()
       //         .packages("org.neuinfo.foundry.ingestor.ws")
       //         .register(MultiPartFeature.class);
        IngestorWSApp app = new IngestorWSApp();

        mongoService = MongoService.getInstance();
        // create and start a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
        return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), app);
    }

    /**
     * Main method.
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws Exception {
        System.setProperty("jersey.config.server.tracing.type","ALL");

        final HttpServer server = startServer();
        System.out.println(String.format("Jersey app started with WADL available at "
                + "%sapplication.wadl\nHit enter to stop it...", BASE_URI));
        System.in.read();
        server.shutdownNow();
        Main.mongoService.shutdown();
    }
}

