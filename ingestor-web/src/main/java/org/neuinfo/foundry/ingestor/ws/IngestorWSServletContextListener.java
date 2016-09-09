package org.neuinfo.foundry.ingestor.ws;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Created by bozyurt on 7/18/14.
 */
public class IngestorWSServletContextListener implements ServletContextListener {
    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        try {
            System.out.println("starting mongoService");
            MongoService.getInstance();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        try {
            System.out.println("shutting down mongoService");
            MongoService.getInstance().shutdown();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }
}
