package org.neuinfo.foundry.consumers.common;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.ColumnMeta;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JDBCUtils;
import org.neuinfo.foundry.common.util.MutableInt;
import org.neuinfo.foundry.common.util.Utils;

import java.sql.*;
import java.util.*;

/**
 * Created by bozyurt on 2/23/16.
 */
public class JDBCJoinIterator implements Iterator<JSONObject> {
    String tableNames;
    String jdbcURL;
    String dbUser;
    String dbPassword;
    String schemaName;
    boolean sampleMode = false;
    int sampleSize = 1;
    Connection con = null;
    private ResultSet resultSet;
    private Statement statement;
    TableMeta mainTM;
    Map<String, TableMeta> tmMap = new HashMap<String, TableMeta>();
    List<TableMeta> tmList = new ArrayList<TableMeta>(5);
    Throwable lastError;
    boolean fullRecordsOnly = false;
    private final static Logger log = Logger.getLogger(JDBCJoinIterator.class);

    public JDBCJoinIterator(Map<String, String> options) {
        this.jdbcURL = options.get("jdbcURL");
        this.dbUser = options.get("dbUser");
        this.dbPassword = options.get("dbPassword");
        this.tableNames = options.get("tableNames");
        this.schemaName = options.containsKey("schemaName") ? options.get("schemaName") : null;
        String joinInfoStr = options.get("joinInfo");
        this.sampleMode = options.containsKey("sampleMode") ?
                Boolean.parseBoolean(options.get("sampleMode")) : false;
        this.sampleSize = Utils.getIntValue(options.get("sampleSize"), 1);
        if (tableNames == null) {
            tableNames = options.get("tableName");
        }
        if (options.containsKey("fullRecordsOnly")) {
            fullRecordsOnly = Boolean.parseBoolean(options.get("fullRecordsOnly"));
        }

        String[] tableNameArr = this.tableNames.split("\\s*,\\s*");
        boolean first = true;
        for (String tableNameStatement : tableNameArr) {
            String[] toks = tableNameStatement.split("\\s+");
            String alias = null;
            String tableName = toks[0];
            if (toks.length > 1) {
                alias = toks[1];
            }
            TableMeta tm = new TableMeta(tableName, alias);
            tmList.add(tm);
            if (first) {
                mainTM = tm;
                first = false;
            }
            tmMap.put(tm.tableName, tm);
            if (tm.alias != null) {
                tmMap.put(tm.alias, tm);
            }

        }
        if (joinInfoStr != null) {
            String[] joinInfoArr = joinInfoStr.split("\\s*,\\s*");
            for (String joinInfoStatement : joinInfoArr) {
                JoinInfo joinInfo = JoinInfo.fromText(joinInfoStatement, tmMap);
                TableMeta primaryTM = tmMap.get(joinInfo.primaryTableName);
                Assertion.assertNotNull(primaryTM);
                primaryTM.addJoinInfo(joinInfo);
            }
        }
    }

    public void startup() throws SQLException {
        Properties props = new Properties();
        props.put("user", dbUser);
        props.put("password", dbPassword);
        this.con = DriverManager.getConnection(this.jdbcURL, props);
        if (schemaName != null) {
            Statement st = null;
            try {
                st = con.createStatement();
                st.execute(String.format("set search_path to '%s'", schemaName));

            } finally {
                if (st != null) {
                    st.close();
                }
            }
        }
        con.setAutoCommit(false);
        for (TableMeta tm : tmList) {
            log.info("getting table metadata for " + tm.tableName);
            tm.getColumnMetaData(this.con);
        }
        this.statement = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        this.statement.setFetchSize(200);
        String query = String.format("select * from %s", this.mainTM.tableName);
        if (sampleMode) {
            query += String.format(" limit %d", sampleSize);
        }
        log.info("query:" + query);
        this.resultSet = statement.executeQuery(query);
    }

    public void shutdown() {
        JDBCUtils.close(this.statement);
        JDBCUtils.close(this.resultSet);
        JDBCUtils.close(this.con);
    }

    @Override
    public boolean hasNext() {
        try {
            return resultSet.next();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public JSONObject next() {
        try {
            boolean ok = !this.fullRecordsOnly;
            JSONObject json;
            do {
                json = new JSONObject();
                JSONObject nsJson = new JSONObject();
                json.put(mainTM.tableName, nsJson);
                for (int i = 0; i < mainTM.cmList.size(); i++) {
                    ColumnMeta cm = mainTM.cmList.get(i);
                    String name = cm.getName();
                    Object colValue = resultSet.getObject(i + 1);
                    setColValue(nsJson, name, colValue);
                }
                boolean hasJoin = !mainTM.jiList.isEmpty();
                boolean complete = true;
                for (JoinInfo ji : mainTM.jiList) {
                    TableMeta jtm = tmMap.get(ji.secTableName);
                    ColumnMeta jcm = ji.getSecTableCM(tmMap);

                    Object value = nsJson.get(ji.primaryColumnName);
                    if (ji.stripPrimaryCol) {
                        value = value.toString().trim();
                    }
                    Map<Object, List<JoinedRow>> map = getMatching(jtm, jcm,
                            Arrays.asList(value), ji.stripSecCol);
                    if (!map.isEmpty()) {
                        String key = value.toString().trim();
                        List<JoinedRow> joinedRows = map.get(key);
                        String joinFieldName = jtm.alias != null && jtm.alias.length() > 1 ?
                                jtm.alias : jtm.tableName;
                        MutableInt fullRecordStatus = new MutableInt(0);
                        handleJoinedRows(nsJson, jtm, joinedRows, joinFieldName, fullRecordStatus);
                        if (fullRecordStatus.getValue() > 0) {
                            complete = false;
                        }
                    } else {
                        complete = false;
                    }
                }
                if (!hasJoin || !fullRecordsOnly) {
                    break;
                }
                if (fullRecordsOnly && !complete) {
                    resultSet.next();
                } else {
                    break;
                }
            } while (!ok);
            return json;
        } catch (Throwable t) {
            t.printStackTrace();
            this.lastError = t;
            return null;
        }
    }

    private static void setColValue(JSONObject nsJson, String name, Object colValue) {
        if (colValue == null) {
            nsJson.put(name, "");
        } else {
            if (Utils.isNumber(colValue)) {
                nsJson.put(name, colValue);
            } else {
                nsJson.put(name, colValue.toString());
            }
        }
    }

    public Throwable getLastError() {
        return lastError;
    }

    void handleJoin(TableMeta primaryTM, JSONObject json, MutableInt fullRecordStatus) throws SQLException {
        if (primaryTM.jiList.isEmpty()) {
            return;
        }
        for (JoinInfo ji : primaryTM.jiList) {
            TableMeta jtm = tmMap.get(ji.secTableName);
            ColumnMeta jcm = ji.getSecTableCM(tmMap);
            Object value = json.get(ji.primaryColumnName);
            if (ji.stripPrimaryCol) {
                value = value.toString().trim();
            }
            Map<Object, List<JoinedRow>> map = getMatching(jtm, jcm, Arrays.asList(value), ji.stripSecCol);
            if (!map.isEmpty()) {
                String key = value.toString().trim();
                List<JoinedRow> joinedRows = map.get(key);
                if (joinedRows != null) {
                    String joinFieldName = jtm.alias != null && jtm.alias.length() > 1 ? jtm.alias : jtm.tableName;
                    handleJoinedRows(json, jtm, joinedRows, joinFieldName, fullRecordStatus);
                }
            } else {
                fullRecordStatus.incr();
            }
        }
    }


    private void handleJoinedRows(JSONObject json, TableMeta jtm, List<JoinedRow> joinedRows,
                                  String joinFieldName, MutableInt fullRecordStatus) throws SQLException {
        if (joinedRows.size() > 1) {
            JSONArray jsArr = new JSONArray();
            json.put(joinFieldName, jsArr);
            for (JoinedRow jr : joinedRows) {
                JSONObject child = toJSON(jr);
                jsArr.put(child);
                handleJoin(jtm, child, fullRecordStatus);
            }
        } else {
            JSONObject child = toJSON(joinedRows.get(0));
            json.put(joinFieldName, child);
            handleJoin(jtm, child, fullRecordStatus);
        }
    }

    static JSONObject toJSON(JoinedRow jr) {
        JSONObject json = new JSONObject();
        for (int i = 0; i < jr.getColumnNames().size(); i++) {
            String name = jr.getColumnNames().get(i);
            Object value = jr.getData().get(i);
            setColValue(json, name, value);
        }
        return json;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }


    Map<Object, List<JoinedRow>> getMatching(TableMeta tm, ColumnMeta joinCM, List<Object> values2Match,
                                             boolean stripJoinColValue) throws SQLException {
        Map<Object, List<JoinedRow>> map = new HashMap<Object, List<JoinedRow>>();
        Statement st = null;
        ResultSet rs;
        try {
            st = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            String query;
            if (stripJoinColValue) {
                query = String.format("select * from %s  where replace(%s,' ','') in (", tm.tableName, joinCM.getName());
            } else {
                query = String.format("select * from %s  where %s in (", tm.tableName, joinCM.getName());
            }
            StringBuilder sb = new StringBuilder(256);
            boolean isStringType = isStringType(joinCM.getType());
            for (Object value : values2Match) {
                if (isStringType) {
                    sb.append('\'').append(value.toString()).append("',");
                } else {
                    sb.append(value).append(',');
                }
            }
            String inPart = sb.toString().replaceFirst(",$", ")");
            query += inPart;
            System.out.println("query:" + query);
            rs = st.executeQuery(query);
            List<ColumnMeta> cmList = tm.cmList;
            int noCols = cmList.size();
            List<String> columnNames = new ArrayList<String>(noCols);
            for (ColumnMeta cm : cmList) {
                columnNames.add(cm.getName());
            }
            while (rs.next()) {
                List<Object> row = new ArrayList<Object>(noCols);
                Object joinColumnValue = null;
                for (int i = 0; i < noCols; i++) {
                    ColumnMeta cm = cmList.get(i);
                    String name = cm.getName();
                    Object colValue = rs.getObject(i + 1);
                    if (name.equals(joinCM.getName())) {
                        joinColumnValue = colValue;
                    }
                    row.add(colValue);
                }
                Assertion.assertNotNull(joinColumnValue);
                JoinedRow jr = new JoinedRow(row, columnNames);
                String key = joinColumnValue.toString().trim();
                List<JoinedRow> joinedRows = map.get(key);
                if (joinedRows == null) {
                    joinedRows = new LinkedList<JoinedRow>();
                    map.put(key, joinedRows);
                }
                joinedRows.add(jr);
            }
        } finally {
            JDBCUtils.close(st);
        }
        return map;
    }

    public static boolean isStringType(String columnType) {
        return columnType.equalsIgnoreCase("varchar");
    }


    public static class TableMeta {
        final String tableName;
        final String alias;
        List<ColumnMeta> cmList;
        List<JoinInfo> jiList = new ArrayList<JoinInfo>(3);

        public TableMeta(String tableName, String alias) {
            this.tableName = tableName;
            this.alias = alias;
        }

        void addJoinInfo(JoinInfo ji) {
            jiList.add(ji);
        }

        void getColumnMetaData(Connection con) throws SQLException {
            Statement st = null;
            ResultSet rs = null;
            try {
                st = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                String query = String.format("select * from %s limit 1", tableName);
                rs = st.executeQuery(query);
                cmList = JDBCUtils.getColumnMetaData(rs);
            } finally {
                JDBCUtils.close(rs);
                JDBCUtils.close(st);
            }
        }
    }


    public static class JoinedRow {
        final List<Object> data;
        final List<String> columnNames;

        public JoinedRow(List<Object> data, List<String> columnNames) {
            this.data = data;
            this.columnNames = columnNames;
        }

        public List<Object> getData() {
            return data;
        }

        public List<String> getColumnNames() {
            return columnNames;
        }
    }

    public static void main(String[] args) throws Exception {
        Parameters parameters = Parameters.getInstance();
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("jdbcURL", parameters.getParam("disco.dbURL"));
        options.put("dbUser", parameters.getParam("disco.user"));
        options.put("dbPassword", parameters.getParam("disco.password"));
        // NeuroMorpho
        if (args.length == 1 && args[0].equals("nm")) {
            options.put("tableNames", "l2_nif_0000_00006_data_nif_neuron n, " +
                    "l2_nif_0000_00006_data_neuron_article article," +
                    "l2_nif_0000_00006_data_allpublications p");
            options.put("joinInfo", "n.neuron_id = article.neuron_id, article.pmid = p.pmid");
        }
        if (args.length == 0) {
            // NeuroSynth
            options.put("tableNames", "l2_nlx_55906_study_activation activation,l2_nlx_55906_study_detail study, l2_nlx_55906_study_feature feature");
            options.put("joinInfo", "activation.id=study.id, study.id = feature.id::strip");
        }
        options.put("sampleSize", "5");
        options.put("sampleMode", "true");
        if (args.length == 1 && args[0].equals("p")) {
            System.out.println("Doing ProteomeXchange");
            options.put("schemaName", "dvp");
            options.put("joinInfo", null);
            options.put("tableNames", null);
            options.put("tableName", "pr_nlx_158620_1");
        }

        JDBCJoinIterator joinIterator = new JDBCJoinIterator(options);

        try {
            joinIterator.startup();
            while (joinIterator.hasNext()) {
                JSONObject rec = joinIterator.next();
                System.out.println(rec.toString(2));
                System.out.println("====================");
            }


        } finally {
            joinIterator.shutdown();
        }
    }
}
