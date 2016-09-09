package org.neuinfo.foundry.common.config;

import org.jdom2.Element;

import java.util.*;

/**
 * Created by bozyurt on 9/24/14.
 */
public class ConsumerConfig {
    String name;
    String type;
    String version;
    String listeningQueueName;
    String inStatus;
    String outStatus;
    String collectionName;
    List<HandlerConfig> handlerConfigs = new ArrayList<HandlerConfig>(1);
    List<Parameter> parameters = new LinkedList<Parameter>();
    List<SubConsumerConfig> subConfigList = new ArrayList<SubConsumerConfig>(5);
    Date lastUpdated;
    String pluginClass;


    public ConsumerConfig(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public static ConsumerConfig fromXml(Element elem, String defaultCollectionName) throws Exception {
        String name = elem.getAttributeValue("name");
        String type = elem.getAttributeValue("type");

        ConsumerConfig cc = new ConsumerConfig(name, type);
        if (elem.getAttribute("version") != null) {
            cc.setVersion(elem.getAttributeValue("version"));
        }
        String lqn = elem.getAttributeValue("listeningQueueName");
        //    String smqn = elem.getAttributeValue("successMessageQueueName");
        //    String fmqn = elem.getAttributeValue("failureMessageQueueName");
        String inStatus = elem.getAttributeValue("inStatus");
        String outStatus = elem.getAttributeValue("outStatus");
        String collectionName = defaultCollectionName;
        if (elem.getAttribute("collection") != null) {
            collectionName = elem.getAttributeValue("collection");
        }
        cc.setCollectionName(collectionName);

        cc.setListeningQueueName(lqn);
        //   cc.setSuccessMessageQueueName(smqn);
        //   cc.setFailureMessageQueueName(fmqn);
        cc.setInStatus(inStatus);
        cc.setOutStatus(outStatus);
        if (elem.getChild("pluginClass") != null) {
            cc.pluginClass = elem.getChildTextTrim("pluginClass");
        }

        if (elem.getChild("handlers") != null) {
            List<Element> handlers = elem.getChild("handlers").getChildren("handler-cfg");
            cc.handlerConfigs = new ArrayList<HandlerConfig>(handlers.size());
            for (Element hc : handlers) {
                cc.handlerConfigs.add(HandlerConfig.fromXml(hc));
            }
        }
        if (elem.getChild("params") != null) {
            List<Element> params = elem.getChild("params").getChildren("param");
            for (Element p : params) {
                cc.parameters.add(Parameter.fromXML(p));
            }
        }
        if (elem.getChild("sub-consumers") != null) {
            List<Element> subConsumersList = elem.getChild("sub-consumers").getChildren("sub-consumer-config");
            for(Element sc : subConsumersList) {
                cc.subConfigList.add( SubConsumerConfig.fromXml(sc));
            }
        }
        return cc;
    }


    public Element toXml() {
        Element el = new Element("consumer-cfg");
        el.setAttribute("name", name);
        el.setAttribute("type", type);
        if (version != null) {
            el.setAttribute("version", version);
        }
        el.setAttribute("listeningQueueName", listeningQueueName);
        el.setAttribute("collection", collectionName);
        el.setAttribute("inStatus", inStatus);
        el.setAttribute("outStatus", outStatus);
        if (pluginClass != null) {
            Element pluginEl = new Element("pluginClass");
            pluginEl.setText(pluginClass);
            el.addContent(pluginEl);
        }
        if (handlerConfigs != null && !handlerConfigs.isEmpty()) {
            Element hctEl = new Element("handlers");
            el.addContent(hctEl);
            for (HandlerConfig hc : handlerConfigs) {
                hctEl.addContent(hc.toXml());
            }
        }
        if (parameters != null && !parameters.isEmpty()) {
            Element pctEl = new Element("params");
            el.addContent(pctEl);
            for (Parameter p : parameters) {
                pctEl.addContent(p.toXML());
            }
        }
        if (subConfigList != null && !subConfigList.isEmpty()) {
            Element sctEl = new Element("sub-consumers");
            el.addContent(sctEl);
            for(SubConsumerConfig scc : subConfigList) {
                sctEl.addContent( scc.toXml());
            }
        }
        return el;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getListeningQueueName() {
        return listeningQueueName;
    }

    public void setListeningQueueName(String listeningQueueName) {
        this.listeningQueueName = listeningQueueName;
    }

    public String getOutStatus() {
        return outStatus;
    }

    public void setOutStatus(String outStatus) {
        this.outStatus = outStatus;
    }

    public String getInStatus() {
        return inStatus;
    }

    public void setInStatus(String inStatus) {
        this.inStatus = inStatus;
    }

    public List<HandlerConfig> getHandlerConfigs() {
        return handlerConfigs;
    }

    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }


    public String getPluginClass() {
        return pluginClass;
    }

    public List<SubConsumerConfig> getSubConfigList() {
        return subConfigList;
    }

    public static class HandlerConfig {
        String name;
        /**
         * the fully qualified class name or script name to delegate work
         */
        String handler;
        String version;
        String description;
        Map<String, String> paramMap = new HashMap<String, String>(17);
        //TODO provenance metadata ?


        public HandlerConfig(String name) {
            this.name = name;
        }


        public Element toXml() {
            Element el = new Element("handler-cfg");
            el.setAttribute("name", name);
            if (version != null) {
                el.setAttribute("version", version);
            }
            Element he = new Element("handler");
            he.setText(handler);
            el.addContent(he);
            if (description != null) {
                Element de = new Element("description");
                de.setText(description);
                el.addContent(de);
            }
            if (!paramMap.isEmpty()) {
                Element pEls = new Element("params");
                el.addContent(pEls);
                for (String name : paramMap.keySet()) {
                    Element pEl = new Element("param");
                    pEls.addContent(pEl);
                    pEl.setAttribute("name", name);
                    pEl.setAttribute("value", paramMap.get(name));
                }
            }
            return el;
        }

        public static HandlerConfig fromXml(Element elem) throws Exception {
            String name = elem.getAttributeValue("name");
            HandlerConfig hc = new HandlerConfig(name);
            if (elem.getAttribute("version") != null) {
                String version = elem.getAttributeValue("version");
                hc.setVersion(version);
            }
            String handler = elem.getChildText("handler");
            hc.setHandler(handler);
            if (elem.getChild("description") != null) {
                hc.setDescription(elem.getChildText("description"));
            }
            if (elem.getChild("params") != null) {
                List<Element> children = elem.getChild("params").getChildren("param");
                for (Element e : children) {
                    String n = e.getAttributeValue("name");
                    String v = e.getAttributeValue("value");
                    hc.paramMap.put(n, v);
                }
            }
            return hc;
        }


        public String getName() {
            return name;
        }

        public String getHandler() {
            return handler;
        }

        public void setHandler(String handler) {
            this.handler = handler;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getParam(String name) {
            return paramMap.get(name);
        }

        public boolean hasParams() {
            return !paramMap.isEmpty();
        }
    }

}
