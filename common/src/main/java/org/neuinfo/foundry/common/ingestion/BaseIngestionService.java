package org.neuinfo.foundry.common.ingestion;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import org.neuinfo.foundry.common.config.IMongoConfig;
import org.neuinfo.foundry.common.config.ServerInfo;
import org.neuinfo.foundry.common.util.MongoUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bozyurt on 5/27/14.
 */
public class BaseIngestionService {
    protected String dbName;
    MongoClient mongoClient;

    public void start(IMongoConfig conf) throws UnknownHostException {
        this.dbName = conf.getMongoDBName();
        List<ServerAddress> servers = new ArrayList<ServerAddress>(conf.getServers().size());
        for (ServerInfo si : conf.getServers()) {
            InetAddress inetAddress = InetAddress.getByName(si.getHost());
            servers.add(new ServerAddress(inetAddress, si.getPort()));
        }

        this.mongoClient = MongoUtils.createMongoClient(servers);
    }

    public void shutdown() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
