package org.neuinfo.foundry.jms.producer.scheduler;

import org.json.JSONObject;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.jms.producer.PipelineTriggerHelper;
import org.quartz.*;


/**
 * Created by bozyurt on 6/15/15.
 */
public class PipelineProcTriggerJob implements Job {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        PipelineTriggerHelper helper = null;
        try {
            JobKey key = context.getJobDetail().getKey();
            JobDataMap dataMap = context.getJobDetail().getJobDataMap();
            String sourceNifId = dataMap.getString("sourceNifId");
            String configFile = dataMap.getString("configFile");

            helper = new PipelineTriggerHelper("foundry.consumer.head");
            helper.startup(configFile);

            Source source = helper.findSource(sourceNifId);
            if (source != null) {
                JSONObject json = helper.prepareMessageBody("ingest", source);
                helper.sendMessage(json);
            }

        } catch (Throwable t) {
            t.printStackTrace();
            throw new JobExecutionException(t.getMessage());
        } finally {
            if (helper != null) {
                helper.shutdown();
            }
        }
    }


}
