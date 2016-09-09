package org.neuinfo.foundry.consumers.common;

import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;

import java.net.UnknownHostException;

/**
 * Created by bozyurt on 7/24/15.
 */
public class ServiceFactory {
    private Configuration config;
    private static ServiceFactory instance = null;

    private ServiceFactory(String configFile) throws Exception {
        this.config = ConfigLoader.load(configFile);
    }

    public static synchronized ServiceFactory getInstance(String configFile) throws Exception {
        if (instance == null) {
            instance = new ServiceFactory(configFile);
        }
        return instance;
    }

    public static synchronized ServiceFactory getInstance() {
        if (instance == null) {
            throw new RuntimeException("ServiceFactory is not properly initialized!");
        }
        return instance;
    }

    public DocumentIngestionService createDocumentIngestionService() throws UnknownHostException {
        DocumentIngestionService dis =  new DocumentIngestionService();
        dis.start(config);
        return dis;
    }
}
