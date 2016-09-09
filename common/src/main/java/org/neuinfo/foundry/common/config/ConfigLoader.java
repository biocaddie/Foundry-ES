package org.neuinfo.foundry.common.config;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.neuinfo.foundry.common.util.Utils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 5/1/14.
 */
public class ConfigLoader {

    public static Configuration load(String xmlFile) throws Exception {
        SAXBuilder builder = new SAXBuilder();

        Configuration conf = new Configuration();
        InputStream in = null;
        try {
            in = ConfigLoader.class.getClassLoader().getResourceAsStream(xmlFile); // "dispatcher-cfg.xml");

            Document doc = builder.build(in);
            Element docRoot = doc.getRootElement();
            Element mcEl = docRoot.getChild("mongo-config");
            conf.setMongoDBName(mcEl.getAttributeValue("db"));
            conf.setCollectionName(mcEl.getAttributeValue("collection"));
            List<ServerInfo> siList = new ArrayList<ServerInfo>(3);
            List<Element> children = mcEl.getChild("servers").getChildren("server");
            for (Element c : children) {
                String host = c.getAttributeValue("host");
                int port = Utils.getIntValue(c.getAttributeValue("port"), -1);
                ServerInfo si = new ServerInfo(host, port);
                siList.add(si);
            }
            conf.setMongoServers(siList);

            Map<String, QueueInfo> qiMap = new HashMap<String, QueueInfo>(7);
            if (docRoot.getChild("queues") != null) {
                final List<Element> qEls = docRoot.getChild("queues").getChildren("queue");
                for (Element qEl : qEls) {
                    final QueueInfo qi = QueueInfo.fromXml(qEl);
                    qiMap.put(qi.getName(), qi);
                }
            }
            if (docRoot.getChild("workflows") != null) {
                List<Element> wfEls = docRoot.getChild("workflows").getChildren("workflow");
                for (Element wfEl : wfEls) {
                    Workflow wf = Workflow.fromXml(wfEl, qiMap);
                    conf.getWorkflows().add(wf);
                }
            }

            if (docRoot.getChild("wf-mappings") != null) {
                List<Element> wmEls = docRoot.getChild("wf-mappings").getChildren("wf-mapping");
                for (Element wmEl : wmEls) {
                    WorkflowMapping wm = WorkflowMapping.fromXml(wmEl);
                    conf.getWorkflowMappings().add(wm);
                }
            }

            //final Element cpEl = docRoot.getChild("checkpoint-file");
            //conf.setCheckpointXmlFile(new File(cpEl.getTextTrim()));
            Element amqEl = docRoot.getChild("activemq-config");
            String brokerURL = amqEl.getChildTextTrim("brokerURL");
            conf.setBrokerURL(brokerURL);
            return conf;
        } finally {
            Utils.close(in);
        }
    }
}
