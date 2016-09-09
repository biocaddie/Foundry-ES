package org.neuinfo.foundry.ws.common;

import org.neuinfo.foundry.common.ingestion.DocProcessingStatsService;
import org.neuinfo.foundry.common.ingestion.DocProcessingStatsService.SourceStats;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.Serializable;
import java.util.List;

/**
 * Created by bozyurt on 12/15/15.
 */
public class DocProcessingStatsJob implements Job {
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        DocProcessingStatsService dpss = new DocProcessingStatsService();
        try {
            MongoService mongoService = MongoService.getInstance();
            dpss.setMongoClient(mongoService.getMongoClient());
            dpss.setDbName(mongoService.getDbName());
            List<SourceStats> ssList = dpss.getDocCountsPerStatusPerSource2("nifRecords");
            CacheManager.getInstance().put("sourceStats", (Serializable) ssList);

        } catch (Exception e) {
            e.printStackTrace();
            throw new JobExecutionException(e);
        }
    }
}
