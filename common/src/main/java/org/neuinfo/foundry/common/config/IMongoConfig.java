package org.neuinfo.foundry.common.config;

import org.neuinfo.foundry.common.config.ServerInfo;

import java.util.List;

/**
 * Created by bozyurt on 10/2/14.
 */
public interface IMongoConfig {
    public String getMongoDBName();
    public List<ServerInfo> getServers();

}
