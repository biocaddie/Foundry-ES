package org.neuinfo.foundry.consumers.jms.consumers.plugins;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;
import org.neuinfo.foundry.common.ingestion.GridFSService;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.consumers.plugin.IPlugin;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.sql.*;
import java.util.*;

/**
 * Created by bozyurt on 8/10/16.
 */
public class DataMentionEnhancer implements IPlugin {
    GridFSService gridFSService;
    DocumentIngestionService dis;
    Connection con;
    String sqliteDBFile;
    Map<String,String> sourceNameMap = new HashMap<String, String>(17);
    private final static Logger log = Logger.getLogger(DataMentionEnhancer.class);

    @Override
    public void setDocumentIngestionService(DocumentIngestionService dis) {
        this.dis = dis;
    }

    @Override
    public void setGridFSService(GridFSService gridFSService) {
        this.gridFSService = gridFSService;
    }

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.sqliteDBFile = options.get("sqliteDBFile");
        Properties config = new Properties();
        config.setProperty("open_mode", "1"); // read only
        Class.forName("org.sqlite.JDBC").newInstance();
        String dbURL = "jdbc:sqlite:" + sqliteDBFile;
        con = DriverManager.getConnection(dbURL, config);
        sourceNameMap.put("NCBI GEO DataSets","GEO Data Sets");
        sourceNameMap.put("RCSB Protein Data Bank","PDB");
    }

    @Override
    public Result handle(DBObject docWrapper) {
        try {
            DBObject siDBO = (DBObject) docWrapper.get("SourceInfo");
            String srcId = siDBO.get("SourceID").toString();
            String dataSource = (String) siDBO.get("DataSource");
            Source source = dis.findSource(srcId, dataSource);
            Assertion.assertNotNull(source);
            if (!sourceNameMap.containsKey(source.getName())) {
                return new Result(docWrapper, Result.Status.OK_WITHOUT_CHANGE);
            }
            //TODO
            BasicDBObject data = (BasicDBObject) docWrapper.get("Data");
            BasicDBObject trDBO = (BasicDBObject) data.get("transformedRec");
            JSONObject transformedJson = JSONUtils.toJSON(trDBO, false);
            JSONObject dataset = transformedJson.getJSONObject("dataset");
            if (dataset != null) {
                String datasetID = dataset.getString("ID");
                String provider = sourceNameMap.get(source.getName());
                if (provider != null && datasetID != null) {
                    int idx = datasetID.indexOf(':');
                    if (idx != -1) {
                        datasetID = datasetID.substring(idx+1);
                    }
                    List<String> citations = getCitations(provider, datasetID);
                    if (citations != null && !citations.isEmpty()) {
                        JSONObject ciJSON = new JSONObject();
                        ciJSON.put("citationCount", citations.size());
                        JSONArray jsArr = new JSONArray();
                        for(String citation : citations) {
                            jsArr.put(citation);
                        }
                        ciJSON.put("citations", jsArr);
                        transformedJson.put("citationInfo", ciJSON);
                        data.put("transformedRec", JSONUtils.encode(transformedJson, true));
                        return new Result(docWrapper, Result.Status.OK_WITH_CHANGE);
                    }
                }
            }
            return new Result(docWrapper, Result.Status.OK_WITHOUT_CHANGE);
        } catch (Throwable t) {
            t.printStackTrace();
            log.error("handle", t);
            Result r = new Result(docWrapper, Result.Status.ERROR);
            r.setErrMessage(t.getMessage());
            return r;
        }
    }

    private List<String> getCitations(String provider, String dataSetId) {
        PreparedStatement pst = null;
        try {
            pst = con.prepareStatement("select pmid from data_mentions where mentioned_entity = ? and provider = ?");
            pst.setString(1, dataSetId);
            pst.setString(2, provider);
            ResultSet rs = pst.executeQuery();
            List<String> list = new LinkedList<String>();
            while(rs.next()) {
                list.add( "PMID:" + rs.getString(1));
            }
            rs.close();
            return list;
        } catch (SQLException e) {
            log.error("getCitations",e );
            return null;
        } finally {
            if (pst != null) {
                try {
                    pst.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    @Override
    public String getPluginName() {
        return "DataMentionEnhancer";
    }
}
