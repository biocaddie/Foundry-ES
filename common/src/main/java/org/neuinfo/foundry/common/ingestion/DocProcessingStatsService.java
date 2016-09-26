package org.neuinfo.foundry.common.ingestion;

import com.mongodb.*;
import org.bson.types.ObjectId;
import org.neuinfo.foundry.common.model.BatchInfo;
import org.neuinfo.foundry.common.model.Status;
import org.neuinfo.foundry.common.util.Assertion;

import java.io.Serializable;
import java.util.*;


/**
 * Created by bozyurt on 12/9/15.
 */
public class DocProcessingStatsService extends BaseIngestionService {

    public void setMongoClient(MongoClient mc) {
        super.mongoClient = mc;
    }

    public void setDbName(String dbName) {
        super.dbName = dbName;
    }

    public List<SourceStats> getDocCountsPerStatusPerSource2(String collectionName, String theSourceID) {
        DB db = mongoClient.getDB(dbName);
        DBCollection collection = db.getCollection(collectionName);
        List sourceIDs = collection.distinct("SourceInfo.SourceID");
        if (theSourceID != null) {
            for(Iterator<Object> iter = sourceIDs.iterator(); iter.hasNext();) {
                String s = iter.next().toString();
                if (!s.equals(theSourceID)) {
                 iter.remove();
                }
            }
        }
        List statusList = collection.distinct("Processing.status");
        List<SourceStats> ssList = new ArrayList<SourceStats>(sourceIDs.size());
        for (Object sourceID : sourceIDs) {
            // System.out.println(sourceID);
            SourceStats ss = new SourceStats(sourceID.toString());
            int totCount = 0;
            for (Object status : statusList) {
                BasicDBObject query = new BasicDBObject("SourceInfo.SourceID", sourceID)
                        .append("Processing.status", status);
                int count = (int) collection.count(query);
                String statusStr = status.toString();
                if (count > 0) {
                    totCount += count;
                    ss.put(statusStr, count);
                } else {
                    if (statusStr.equals("error")) {
                        ss.put(statusStr, count);
                    }
                }
            }
            if (totCount > 0) {
                ssList.add(ss);
            }
        }
        return ssList;
    }

    public Map<String, WFStatusInfo> getWorkflowStatusInfo(String sourceID, String finishedStatus, List<SourceStats> ssList) {
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        BasicDBObject query = new BasicDBObject();
        BasicDBObject keys = new BasicDBObject("sourceInformation", 1)
                .append("batchInfos", 1);
        if (sourceID != null) {
            query = new BasicDBObject("sourceInformation.resourceID", sourceID);
        }
        DBCursor cursor = sources.find(query, keys);
        Map<String, WFStatusInfo> wdsiMap = new HashMap<String, WFStatusInfo>();
        Map<String, SourceStats> ssMap = new HashMap<String, SourceStats>();
        for(SourceStats ss : ssList) {
            ssMap.put(ss.getSourceID(), ss);
        }
        try {
            while (cursor.hasNext()) {
                BasicDBObject dbo = (BasicDBObject) cursor.next();
                BasicDBObject siDBO = (BasicDBObject) dbo.get("sourceInformation");
                BasicDBList biList = (BasicDBList) dbo.get("batchInfos");
                if (!biList.isEmpty()) {
                    DBObject biDBO = (DBObject) biList.get(biList.size() - 1);
                    BatchInfo bi = BatchInfo.fromDbObject(biDBO);
                    String resourceID = siDBO.getString("resourceID");
                    // calculate status
                    SourceStats ss = ssMap.get(resourceID);
                    if (ss == null) {
                        continue;
                    }
                    Map<String, Integer> statusCountMap = ss.getStatusCountMap();
                    int totCount = 0;
                    for (Integer count : statusCountMap.values()) {
                        totCount += count;
                    }
                    Integer finishedCount = statusCountMap.get(finishedStatus);
                    if (finishedCount == null) {
                        // FIXME single workflow assumption
                        finishedCount = statusCountMap.get(finishedStatus + ".1");
                    }
                    Integer errorCount = statusCountMap.get("error");
                    int totalFinished = 0;
                    totalFinished += finishedCount != null ? finishedCount.intValue() : 0;
                    totalFinished += errorCount != null ? errorCount.intValue() : 0;
                    String status = Status.NOT_STARTED.name().toLowerCase();
                    if ((bi.getIngestionStatus() == Status.IN_PROCESS || bi.getIngestionStatus() == Status.FINISHED) && totalFinished < totCount) {
                        status = Status.IN_PROCESS.name().toLowerCase();
                    } else if (bi.getIngestionStatus() == Status.FINISHED && totalFinished >= totCount) {
                        status = Status.FINISHED.name().toLowerCase();
                    }

                    WFStatusInfo wfsi = new WFStatusInfo(status, bi.getIngestionStatus().name().toLowerCase());
                    wdsiMap.put(resourceID, wfsi);
                }
            }

        } finally {
            cursor.close();
        }
        return wdsiMap;
    }

    public static class WFStatusInfo {
        final String status;
        final String ingestionStatus;

        public WFStatusInfo(String status, String ingestionStatus) {
            this.status = status;
            this.ingestionStatus = ingestionStatus;
        }

        public String getStatus() {
            return status;
        }

        public String getIngestionStatus() {
            return ingestionStatus;
        }
    }

    public List<SourceStats> getDocCountsPerStatusPerSource(String collectionName) {
        DB db = mongoClient.getDB(dbName);
        DBCollection collection = db.getCollection(collectionName);
        DBObject groupFields = new BasicDBObject("_id",
                new BasicDBObject("source", "$SourceInfo.SourceID")
                        .append("status", "$Processing.status"));
        groupFields.put("count", new BasicDBObject("$sum", 1));
        DBObject group = new BasicDBObject("$group", groupFields);

        AggregationOutput aggregationOutput = collection.aggregate(Arrays.asList(group));
        Map<String, SourceStats> ssMap = new HashMap<String, SourceStats>();
        for (DBObject dbo : aggregationOutput.results()) {
            BasicDBObject idDBO = (BasicDBObject) dbo.get("_id");
            String sourceID = idDBO.getString("source");
            String status = idDBO.getString("status");
            int count = ((BasicDBObject) dbo).getInt("count");
            SourceStats ss = ssMap.get(sourceID);
            if (ss == null) {
                ss = new SourceStats(sourceID);
                ssMap.put(sourceID, ss);
            }
            ss.put(status, count);
            // System.out.println(dbo);
        }

        List<SourceStats> ssList = new ArrayList<SourceStats>(ssMap.values());
        Collections.sort(ssList, new Comparator<SourceStats>() {
            @Override
            public int compare(SourceStats o1, SourceStats o2) {
                return o1.getSourceID().compareTo(o2.getSourceID());
            }
        });
        return ssList;
    }


    public static class SourceStats implements Serializable {
        final String sourceID;
        Map<String, Integer> statusCountMap = new HashMap<String, Integer>(3);

        public SourceStats(String sourceID) {
            this.sourceID = sourceID;
        }

        public void put(String status, int count) {
            statusCountMap.put(status, count);
        }

        public String getSourceID() {
            return sourceID;
        }

        public Map<String, Integer> getStatusCountMap() {
            return statusCountMap;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(80);
            sb.append("SourceStats::[").append("sourceID:").append(sourceID);
            for (String status : statusCountMap.keySet()) {
                sb.append("\n\t").append(status).append(": ").append(statusCountMap.get(status));
            }
            sb.append("]");
            return sb.toString();
        }
    }
}
