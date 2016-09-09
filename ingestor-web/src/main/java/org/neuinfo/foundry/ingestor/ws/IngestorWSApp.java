package org.neuinfo.foundry.ingestor.ws;

import com.wordnik.swagger.jaxrs.config.BeanConfig;
import com.wordnik.swagger.jaxrs.listing.ResourceListingProvider;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.neuinfo.foundry.common.util.Utils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ws.rs.core.Application;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by bozyurt on 7/18/14.
 */
public class IngestorWSApp extends ResourceConfig {
    public IngestorWSApp() {
        String basePath = "http://localhost:8080/foundry/api";
        try {
            Properties properties = Utils.loadProperties("ingestor-web.properties");
            if (properties != null && properties.getProperty("base.path") != null) {
                basePath = properties.getProperty("base.path");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //packages("org.neuinfo.foundry.ingestor.ws");
        packages("org.neuinfo.foundry.ingestor.ws.enhancers");
        packages("org.neuinfo.foundry.ingestor.ws.dm");
        register(MultiPartFeature.class);

        register(com.wordnik.swagger.jersey.listing.ApiListingResource.class);
        register(com.wordnik.swagger.jersey.listing.JerseyApiDeclarationProvider.class);
        register(com.wordnik.swagger.jersey.listing.ApiListingResourceJSON.class);
        register(ResourceListingProvider.class);

        BeanConfig beanConfig = new BeanConfig();
        beanConfig.setVersion("0.1");
        beanConfig.setBasePath(basePath);
        beanConfig.setResourcePackage("org.neuinfo.foundry.ingestor.ws.enhancers");
        beanConfig.setScan(true);
    }


}
