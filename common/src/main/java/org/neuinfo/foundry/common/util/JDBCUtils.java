package org.neuinfo.foundry.common.util;

import org.neuinfo.foundry.common.model.ColumnMeta;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bozyurt on 3/1/16.
 */
public class JDBCUtils {

    public static List<ColumnMeta> getColumnMetaData(ResultSet rs) throws SQLException {
        List<ColumnMeta> cmList = new ArrayList<ColumnMeta>(10);
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            ColumnMeta columnMeta = new ColumnMeta(metaData.getColumnName(i + 1),
                    metaData.getColumnTypeName(i + 1));
            cmList.add(columnMeta);
        }
        return cmList;
    }

    public static void close(Statement st) {
        if (st != null) {
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(Connection con) {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
