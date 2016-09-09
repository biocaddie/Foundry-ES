package org.neuinfo.foundry.common.config;

import org.jdom2.Element;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by bozyurt on 12/4/15.
 */
public class SubConsumerConfig {
    String name;
    String pluginClass;
    List<Parameter> parameters = new LinkedList<Parameter>();

    public SubConsumerConfig(String name) {
        this.name = name;
    }

    public static SubConsumerConfig fromXml(Element elem) throws Exception {
        String name = elem.getAttributeValue("name");
        SubConsumerConfig scc = new SubConsumerConfig(name);
        scc.pluginClass = elem.getChildTextTrim("pluginClass");
        if (elem.getChild("params") != null) {
            List<Element> params = elem.getChild("params").getChildren("param");
            for (Element p : params) {
                scc.parameters.add(Parameter.fromXML(p));
            }
        }
        return scc;
    }

    public Element toXml() {
        Element el = new Element("sub-consumer-config");
        el.setAttribute("name", name);
        Element pluginEl = new Element("pluginClass");
        pluginEl.setText(pluginClass);
        el.addContent(pluginEl);
        if (parameters != null && !parameters.isEmpty()) {
            Element pctEl = new Element("params");
            el.addContent(pctEl);
            for (Parameter p : parameters) {
                pctEl.addContent(p.toXML());
            }
        }
        return el;
    }

    public String getName() {
        return name;
    }

    public String getPluginClass() {
        return pluginClass;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }
}
