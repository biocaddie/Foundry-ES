package org.neuinfo.foundry.common.ingestion;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.neuinfo.foundry.common.config.IMongoConfig;
import org.neuinfo.foundry.common.config.ServerInfo;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bozyurt on 5/27/14.
 */
public class Configuration implements IMongoConfig {
    private String mongoDBName;
    private List<ServerInfo> servers = new ArrayList<ServerInfo>(3);
    private String brokerURL;
    private SourceConfig sourceConfig;


    public Configuration(String mongoDBName) {
        this.mongoDBName = mongoDBName;
    }

    public void addServer(ServerInfo si) {
        servers.add(si);
    }

    public String getMongoDBName() {
        return mongoDBName;
    }

    public List<ServerInfo> getServers() {
        return servers;
    }

    public SourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public void setSourceConfig(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    public String getBrokerURL() {
        return brokerURL;
    }

    public void setBrokerURL(String brokerURL) {
        this.brokerURL = brokerURL;
    }

    public static Configuration fromXML(String xmlFile) throws Exception {
        SAXBuilder builder = new SAXBuilder();

        InputStream in = null;
        try {
            in = Configuration.class.getClassLoader().getResourceAsStream(xmlFile);
            Document doc = builder.build(in);
            Element docRoot = doc.getRootElement();
            Element mcEl = docRoot.getChild("mongo-config");
            String db = mcEl.getAttributeValue("db");
            Configuration conf = new Configuration(db);
            List<Element> children = mcEl.getChild("servers").getChildren("server");
            for (Element child : children) {
                String host = child.getAttributeValue("host");
                int port = Utils.getIntValue(child.getAttributeValue("port"), -1);
                Assertion.assertTrue(port != -1);
                ServerInfo si = new ServerInfo(host, port);
                conf.addServer(si);
            }
            return conf;
        } finally {
            Utils.close(in);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Configuration{");
        sb.append("mongoDBName='").append(mongoDBName).append('\'');
        sb.append(", servers=").append(servers);
        sb.append(", brokerURL='").append(brokerURL).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
