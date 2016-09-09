package org.neuinfo.foundry.jms.producer.scheduler;


import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.util.Assertion;
import org.quartz.CronScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;


/**
 * Created by bozyurt on 6/15/15.
 */
public class PipelineCronTriggerBuilder {
    Source source;
    String triggerName;
    String jobName;
    String jobGroup;
    String group = "sourceTrigger";
    public static Map<String, String> dayMap = new HashMap<String, String>();

    static {
        dayMap.put("Sunday", "SUN");
        dayMap.put("Monday", "MON");
        dayMap.put("Tuesday", "TUE");
        dayMap.put("Wednesday", "WED");
        dayMap.put("Thursday", "THU");
        dayMap.put("Friday", "FRI");
        dayMap.put("Saturday", "SAT");
    }

    public PipelineCronTriggerBuilder(Source source) {
        this.source = source;
    }

    public PipelineCronTriggerBuilder jobName(String jobName) {
        this.jobName = jobName;
        return this;
    }

    public PipelineCronTriggerBuilder jobGroup(String jobGroup) {
        this.jobGroup = jobGroup;
        return this;
    }

    public Trigger build() {
        CronScheduleBuilder csBuilder = buildCronSchedule();
        return TriggerBuilder.newTrigger().withIdentity(triggerName, group).
                withSchedule(csBuilder)
                .forJob(jobName, jobGroup).build();

    }

    private CronScheduleBuilder buildCronSchedule() {
        JSONObject cfJSON = source.getIngestConfiguration().getJSONObject("crawFrequency");
        Assertion.assertNotNull(cfJSON);
        String hours = cfJSON.getString("hours").trim();
        String minutes = cfJSON.getString("minutes").trim();


        StringBuilder sb = new StringBuilder(128);
        sb.append("0 ");
        JSONArray jsArr = cfJSON.getJSONArray("startDays");
        List<String> days = new ArrayList<String>(7);
        for (int i = 0; i < jsArr.length(); i++) {
            String day = dayMap.get(jsArr.getString(i));
            Assertion.assertNotNull(day);
            days.add(day);
        }
        if (hours.equals("24")) {
            sb.append("0 ");
        } else {
            sb.append(hours).append(' ');
        }
        sb.append(minutes).append(' ');
        if (days.size() == 7) {
            sb.append("* * ?");
        } else {
            sb.append("? * ");
            sb.append(StringUtils.join(days, ','));
        }
        System.out.println(sb.toString());
        return CronScheduleBuilder.cronSchedule(sb.toString().trim());
    }

}
