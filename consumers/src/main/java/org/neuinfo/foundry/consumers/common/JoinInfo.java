package org.neuinfo.foundry.consumers.common;

import org.neuinfo.foundry.common.model.ColumnMeta;
import org.neuinfo.foundry.common.util.Assertion;

import java.util.Map;

/**
 * Created by bozyurt on 5/10/16.
 */
public class JoinInfo {
    String primaryTableName;
    String primaryColumnName;
    String secTableName;
    String secColumnName;
    boolean stripPrimaryCol = false;
    boolean stripSecCol = false;

    public ColumnMeta getSecTableCM(Map<String, JDBCJoinIterator.TableMeta> tmMap) {
        return findColumnMeta(secColumnName, secTableName, tmMap);
    }

    public static ColumnMeta findColumnMeta(String columnName, String tableName, Map<String, JDBCJoinIterator.TableMeta> tmMap) {
        JDBCJoinIterator.TableMeta tm = tmMap.get(tableName);
        Assertion.assertNotNull(tm);
        ColumnMeta theCM = null;
        for (ColumnMeta cm : tm.cmList) {
            if (cm.getName().equals(columnName)) {
                theCM = cm;
                break;
            }
        }
        return theCM;
    }

    public static JoinInfo fromText(String joinStatement, Map<String, JDBCJoinIterator.TableMeta> tmMap) {
        String[] tokens = joinStatement.split("\\s*=\\s*");
        Assertion.assertTrue(tokens.length == 2);
        String[] parts = split(tokens[0], tmMap);
        JoinInfo ji = new JoinInfo();
        ji.primaryTableName = parts[0];
        String[] colParts = parseColumnName(parts[1]);
        ji.primaryColumnName = colParts[0];
        if (colParts.length == 2) {
            ji.stripPrimaryCol = true;
        }
        parts = split(tokens[1], tmMap);
        ji.secTableName = parts[0];
        colParts = parseColumnName(parts[1]);
        ji.secColumnName = colParts[0];
        if (colParts.length == 2) {
            ji.stripSecCol = true;
        }
        return ji;
    }

    static String[] parseColumnName(String colNamePart) {
        String[] result;
        int idx = colNamePart.indexOf("::");
        if (idx == -1) {
            result = new String[]{colNamePart};
        } else {
            result = new String[2];
            result[0] = colNamePart.substring(0, idx);
            result[1] = colNamePart.substring(idx + 2);
        }
        return result;
    }

    static String[] split(String joinPartStr, Map<String, JDBCJoinIterator.TableMeta> tmMap) {
        String[] parts = joinPartStr.split("\\.");
        Assertion.assertTrue(parts.length == 2);
        String[] result = new String[2];
        JDBCJoinIterator.TableMeta tm = tmMap.get(parts[0]);
        Assertion.assertNotNull(tm);
        result[0] = tm.tableName;
        result[1] = parts[1];
        return result;
    }

}
