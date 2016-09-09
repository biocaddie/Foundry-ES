package org.neuinfo.foundry.consumers.utils;

import org.neuinfo.foundry.common.util.Utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by bozyurt on 3/1/16.
 */
public class TableCopier {
    Properties props;

    public TableCopier() throws IOException {
        this.props = Utils.loadProperties("migration.properties");
    }


    public void migrateTable(String tableName) throws SQLException {
        Connection srcCon;
        Connection destCon;
        Properties p = new Properties();

    }



}
