package org.neuinfo.foundry.consumers.common;

import com.mongodb.ServerAddress;
import org.neuinfo.foundry.common.config.ConsumerConfig;
import org.neuinfo.foundry.common.config.IMongoConfig;
import org.neuinfo.foundry.common.config.ServerInfo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 4/24/14.
 */
public class Configuration implements IMongoConfig {
    Map<String, Object> mongoListenerSettings;
    String brokerURL;
    List<ConsumerConfig> consumerConfigs;
    private String mongoDBName;
    private List<ServerInfo> servers = new ArrayList<ServerInfo>(3);
    private String pluginDir;
    private String libDir;
    private String collectionName;

    public List<ConsumerConfig> getConsumerConfigs() {
        return consumerConfigs;
    }

    public void setConsumerConfigs(List<ConsumerConfig> consumerConfigs) {
        this.consumerConfigs = consumerConfigs;
    }


    public List<ServerAddress> getServerAddressList() throws UnknownHostException {
        List<ServerAddress> saList = new ArrayList<ServerAddress>(this.servers.size());
        for(ServerInfo si : servers) {
            saList.add( new ServerAddress(si.getHost(), si.getPort()));
        }
        return saList;
    }

    public String getBrokerURL() {
        return brokerURL;
    }

    public void setBrokerURL(String brokerURL) {
        this.brokerURL = brokerURL;
    }

    public String getPluginDir() {
        return pluginDir;
    }

    public String getLibDir() {
        return libDir;
    }

    public void setPluginDir(String pluginDir) {
        this.pluginDir = pluginDir;
    }

    public void setLibDir(String libDir) {
        this.libDir = libDir;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Configuration{");
        sb.append("mongoListenerSettings=").append(mongoListenerSettings);
        sb.append("brokerURL=").append(brokerURL);
        sb.append('}');
        return sb.toString();
    }


    public void setMongoDBName(String mongoDBName) {
        this.mongoDBName = mongoDBName;
    }

    public void addServerInfo(ServerInfo si) {
        this.servers.add(si);
    }

    @Override
    public String getMongoDBName() {
        return this.mongoDBName;
    }

    @Override
    public List<ServerInfo> getServers() {
        return this.servers;
    }
}
