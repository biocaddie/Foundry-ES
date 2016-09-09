package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.JDBCJoinIterator;
import org.neuinfo.foundry.consumers.common.Parameters;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.util.Map;

/**
 * Created by bozyurt on 12/4/15.
 */
public class DISCOIngestor implements Ingestor {
    Map<String, String> optionMap;
    String jdbcURL;
    String dbUser;
    String dbPassword;
    String tableName;
    String tableNames;
    String joinInfoStr;
    String schemaName;
    int recordIdx = 0;
    boolean sampleMode = false;
    int sampleSize = 1;
    private final static Logger log = Logger.getLogger(DISCOIngestor.class);

    JDBCJoinIterator joinIterator;

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.optionMap = options;
        this.jdbcURL = options.get("jdbcURL");
        this.dbUser = options.get("dbUser");
        this.dbPassword = options.get("dbPassword");
        this.tableName = options.get("tableName");
        this.joinInfoStr = options.get("joinInfo");
        this.schemaName = options.containsKey("schemaName") ? options.get("schemaName") : null;
        this.tableNames = options.get("tableNames");
        this.sampleMode = options.containsKey("sampleMode") ?
                Boolean.parseBoolean(options.get("sampleMode")) : false;
        this.sampleSize = Utils.getIntValue(options.get("sampleSize"), 1);
        if (tableNames == null) {
            Assertion.assertNotNull(tableName);
            tableNames = tableName;
        }

        Parameters parameters = Parameters.getInstance();
        if (this.jdbcURL == null) {
            log.info("using DISCO default connection parameters...");
            optionMap.put("jdbcURL", parameters.getParam("disco.dbURL"));
            optionMap.put("dbUser", parameters.getParam("disco.user"));
            optionMap.put("dbPassword", parameters.getParam("disco.password"));
        }
    }

    @Override
    public void startup() throws Exception {
        Class.forName("org.postgresql.Driver");
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        joinIterator = new JDBCJoinIterator(optionMap);
        joinIterator.startup();
    }

    @Override
    public Result prepPayload() {
        this.recordIdx++;

        try {
            JSONObject json = joinIterator.next();
            if (json == null) {
                throw joinIterator.getLastError();
            }
            return new Result(json, Result.Status.OK_WITH_CHANGE);
        } catch (Throwable t) {
            t.printStackTrace();
            return new Result(new JSONObject(), Result.Status.ERROR);
        }
    }

    @Override
    public String getName() {
        return "DISCOIngestor";
    }

    @Override
    public int getNumRecords() {
        return -1;
    }

    @Override
    public String getOption(String optionName) {
        return optionMap.get(optionName);
    }

    @Override
    public void shutdown() {
        if (joinIterator != null) {
            joinIterator.shutdown();
        }
    }


    @Override
    public boolean hasNext() {
        return joinIterator.hasNext();
    }
}
