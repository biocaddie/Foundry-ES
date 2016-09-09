package org.neuinfo.foundry.consumers.common;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.neuinfo.foundry.common.config.ConsumerConfig;
import org.neuinfo.foundry.common.config.ServerInfo;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bozyurt on 5/1/14.
 */
public class ConfigLoader {

    public static Configuration load(String xmlFile) throws Exception {
        SAXBuilder builder = new SAXBuilder();

        Configuration conf = new Configuration();
        InputStream in = null;
        try {
            in = ConfigLoader.class.getClassLoader().getResourceAsStream(xmlFile);

            Document doc = builder.build(in);
            Element docRoot = doc.getRootElement();
            Element mcEl = docRoot.getChild("mongo-config");

            conf.setPluginDir(docRoot.getChildTextTrim("pluginDir"));
            conf.setLibDir(docRoot.getChildTextTrim("libDir"));
            if (!new File(conf.getPluginDir()).isDirectory()) {
                // throw new Exception("No valid plugin directory is specified!:" + conf.getPluginDir());
                System.err.println("No valid plugin directory is specified!:" + conf.getPluginDir());
            }
            if (!new File(conf.getLibDir()).isDirectory()) {
                //throw new Exception("No valid plugin library directory is specified!:" + conf.getLibDir());
                System.err.println("No valid plugin library directory is specified!:" + conf.getLibDir());
            }

            conf.setMongoDBName(mcEl.getAttributeValue("db"));
            conf.setCollectionName(mcEl.getAttributeValue("collection"));
            if (Utils.isEmpty(conf.getCollectionName())) {
                throw new Exception("A mongo collection needs to be specified!");
            }
            List<Element> sels = mcEl.getChild("servers").getChildren("server");
            for (Element sel : sels) {
                String host = sel.getAttributeValue("host");
                int port = Utils.getIntValue(sel.getAttributeValue("port"), -1);
                Assertion.assertTrue(port != -1);
                ServerInfo si = new ServerInfo(host, port);
                conf.addServerInfo(si);
            }

            final Element amqEl = docRoot.getChild("activemq-config");
            String brokerURL = amqEl.getChildTextTrim("brokerURL");
            conf.setBrokerURL(brokerURL);
            if (docRoot.getChild("consumers") != null) {
                List<ConsumerConfig> cfList = new ArrayList<ConsumerConfig>();
                List<Element> children = docRoot.getChild("consumers").getChildren("consumer-cfg");
                for (Element e : children) {
                    cfList.add(ConsumerConfig.fromXml(e, conf.getCollectionName()));
                }
                conf.setConsumerConfigs(cfList);
            }
            return conf;
        } finally {
            Utils.close(in);
        }
    }
}
