package org.neuinfo.foundry.common.config;

import org.jdom2.Element;

/**
 * Created by bozyurt on 12/4/15.
 */
public class Parameter {
    final String name;
    final String value;

    public Parameter(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public static Parameter fromXML(Element elem) {
        String name = elem.getAttributeValue("name");
        String value = elem.getAttributeValue("value");
        return new Parameter(name, value);
    }

    public Element toXML() {
        Element el = new Element("param");
        el.setAttribute("name", name);
        el.setAttribute("value", value);
        return el;
    }
}
