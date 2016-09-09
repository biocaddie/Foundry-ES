package org.neuinfo.foundry.ws;

import org.neuinfo.foundry.common.transform.TransformationFunctionRegistry;
import org.neuinfo.foundry.ws.common.*;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import static org.quartz.SimpleScheduleBuilder.*;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Created by bozyurt on 8/25/15.
 */
public class ManagementWSServletContextListener implements ServletContextListener {
    Scheduler scheduler;

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        try {
            System.out.println("starting mongoService");
            MongoService mongoService = MongoService.getInstance();
            // MessagingService.getInstance();
            CacheManager.getInstance();
            System.out.println("starting security service");
            SecurityService.getInstance(mongoService);
            BootstrapHelper bh = new BootstrapHelper(mongoService);
            bh.bootstrap();

            SchedulerFactory factory = new StdSchedulerFactory();
            scheduler = factory.getScheduler();
            scheduler.start();
            JobDetail job = JobBuilder.newJob(DocProcessingStatsJob.class).withIdentity("SourceStats").build();
            Trigger trigger = TriggerBuilder.newTrigger().withIdentity("manUITrigger")
                    .startNow().withSchedule(simpleSchedule().withIntervalInMinutes(10).repeatForever())
                    .build();

            scheduler.scheduleJob(job, trigger);

        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        try {
            System.out.println("shutting down mongoService");
            MongoService.getInstance().shutdown();
            CacheManager.getInstance().shutdown();
            if (scheduler != null) {
                scheduler.shutdown();
            }
        } catch (Exception x) {
            x.printStackTrace();
        }
    }
}
