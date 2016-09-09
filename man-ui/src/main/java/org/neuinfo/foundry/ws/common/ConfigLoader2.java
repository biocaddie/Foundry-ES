package org.neuinfo.foundry.ws.common;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.neuinfo.foundry.common.config.ServerInfo;
import org.neuinfo.foundry.common.ingestion.Configuration;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;

import java.io.File;
import java.io.InputStream;
import java.util.List;

/**
 * Created by bozyurt on 8/25/15.
 */
public class ConfigLoader2 {
    public static Configuration load(String xmlFile) throws Exception {
        return load(xmlFile, true);
    }

    public static Configuration load(String xmlFile, boolean loadSourceInfo) throws Exception {
        Document doc = loadFromClasspath(xmlFile);
        return getConfiguration(loadSourceInfo, doc);
    }

    public static Configuration loadFromFile(String xmlFile, boolean loadSourceInfo) throws Exception {
        Document doc = loadFromFile(xmlFile);
        return getConfiguration(loadSourceInfo, doc);
    }

    private static Configuration getConfiguration(boolean loadSourceInfo, Document doc) {
        Configuration conf;

        Element docRoot = doc.getRootElement();
        Element mcEl = docRoot.getChild("mongo-config");
        String db = mcEl.getAttributeValue("db");
        conf = new Configuration(db);
        final List<Element> children = mcEl.getChild("servers").getChildren("server");
        for (Element child : children) {
            String host = child.getAttributeValue("host");
            int port = Utils.getIntValue(child.getAttributeValue("port"), -1);
            Assertion.assertTrue(port != -1);
            ServerInfo si = new ServerInfo(host, port);
            conf.addServer(si);
        }

        Element amqEl = docRoot.getChild("activemq-config");
        if (amqEl != null) {
            conf.setBrokerURL(amqEl.getChildTextTrim("brokerURL"));
        }
        return conf;
    }

    static Document loadFromClasspath(String xmlFile) throws Exception {
        InputStream in = null;
        try {
            SAXBuilder builder = new SAXBuilder();
            in = ConfigLoader2.class.getClassLoader().getResourceAsStream(xmlFile);
            Document doc = builder.build(in);
            return doc;
        } finally {
            Utils.close(in);
        }
    }

    static Document loadFromFile(String xmlFile) throws Exception {
        SAXBuilder builder = new SAXBuilder();
        return builder.build(new File(xmlFile));

    }
}
