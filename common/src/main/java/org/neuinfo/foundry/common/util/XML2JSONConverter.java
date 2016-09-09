package org.neuinfo.foundry.common.util;

import org.apache.commons.cli.*;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.util.*;

/**
 * Created by bozyurt on 4/22/14.
 */
public class XML2JSONConverter {
    private static final String TEXT = "_$";
    private static Map<String, Namespace> nsMap = new HashMap<String, Namespace>(17);
    int jsonObjCount = 0;


    static {
        nsMap.put("gmi", Namespace.getNamespace("gmi", "http://www.isotc211.org/2005/gmi"));
        nsMap.put("gco", Namespace.getNamespace("gco", "http://www.isotc211.org/2005/gco"));
        nsMap.put("gmd", Namespace.getNamespace("gmd", "http://www.isotc211.org/2005/gmd"));
        nsMap.put("xsi", Namespace.getNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance"));
        nsMap.put("xlink", Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink"));

        nsMap.put("csw", Namespace.getNamespace("csw", "http://www.opengis.net/cat/csw/2.0.2"));
        nsMap.put("gmx", Namespace.getNamespace("gmx", "http://www.isotc211.org/2005/gmx"));
        nsMap.put("gsr", Namespace.getNamespace("gsr", "http://www.isotc211.org/2005/gsr"));
        nsMap.put("gss", Namespace.getNamespace("gss", "http://www.isotc211.org/2005/gss"));
        nsMap.put("gts", Namespace.getNamespace("gts", "http://www.isotc211.org/2005/gts"));
        nsMap.put("srv", Namespace.getNamespace("srv", "http://www.isotc211.org/2005/srv"));
        nsMap.put("gml", Namespace.getNamespace("gml", "http://www.opengis.net/gml/3.2"));
    }

    public XML2JSONConverter() {
    }

    public JSONObject toJSON(Element rootEl) throws JSONException {
        List<Namespace> namespacesInScope = rootEl.getNamespacesInScope();
        for (Namespace ns : namespacesInScope) {
            System.out.println(ns);
        }
        JSONObject wrapper = new JSONObject();
        JSONObject root = new JSONObject();
        wrapper.put(rootEl.getQualifiedName(), root);
        for (Namespace ns : namespacesInScope) {
            if (ns.getPrefix().equals("")) {
                wrapper.put("_@default", ns.getURI());
            } else {
                wrapper.put("_@" + ns.getPrefix(), ns.getURI());
            }
        }
        toJSON(rootEl, root);

        return wrapper;
    }

    void incrShowCount() {
        this.jsonObjCount++;
        if ((jsonObjCount % 100) == 0) {
            System.out.println("# of json objects so far: " + jsonObjCount);
        }
    }

    protected void toJSON(Element parentEl, JSONObject parentJSON) throws JSONException {

        List<Attribute> attributes = parentEl.getAttributes();
        if (!attributes.isEmpty()) {
            for (Attribute attribute : attributes) {
                parentJSON.put("@" + attribute.getQualifiedName(), attribute.getValue());
            }
        }
        String text = parentEl.getTextTrim();
        if (text != null && text.length() > 0) {
            parentJSON.put(TEXT, text);
        }
        List<Element> children = parentEl.getChildren();
        if (!children.isEmpty()) {

            if (children.size() == 1) {
                Element child = children.get(0);
                JSONObject childJSON = new JSONObject();
                String qualifiedName = child.getQualifiedName();
                // Assertion.assertNotNull(qualifiedName);
                parentJSON.put(qualifiedName, childJSON);
                //incrShowCount();
                toJSON(child, childJSON);

            } else {
                Map<String, List<Element>> childGroupMap = new LinkedHashMap<String, List<Element>>(17);
                for (Element child : children) {
                    List<Element> childGroup = childGroupMap.get(child.getQualifiedName());
                    if (childGroup == null) {
                        childGroup = new LinkedList<Element>();
                        childGroupMap.put(child.getQualifiedName(), childGroup);
                    }
                    childGroup.add(child);
                }

                for (String childName : childGroupMap.keySet()) {
                    final List<Element> childGroup = childGroupMap.get(childName);
                    if (childGroup.size() == 1) {
                        Element child = childGroup.get(0);
                        JSONObject childJSON = new JSONObject();
                        parentJSON.put(child.getQualifiedName(), childJSON);
                        //incrShowCount();
                        toJSON(child, childJSON);
                    } else {
                        JSONArray jsArr = new JSONArray();
                        parentJSON.put(childGroup.get(0).getQualifiedName(), jsArr);
                        for (Element child : childGroup) {
                            JSONObject childJSON = new JSONObject();
                            jsArr.put(childJSON);
                            //incrShowCount();
                            toJSON(child, childJSON);
                        }
                    }
                }
            }
        }
    }


    public Element toXML(JSONObject wrapper) throws JSONException {
        String qName = wrapper.names().getString(0);
        final JSONObject root = wrapper.getJSONObject(qName);
        Element rootEl;
        Map<String, Namespace> namespaceMap = getNamespaces(wrapper);
        if (qName.indexOf(":") != -1) {
            String[] toks = qName.split(":");
            String localName = toks[1];
            String nsPrefix = toks[0];
            String schemaLocation = null;

            if (root.has("@xsi:schemaLocation")) {
                schemaLocation = root.getString("@xsi:schemaLocation");
            }


            Namespace ns = nsMap.get(nsPrefix);
            if (nsPrefix == null) {
                ns = Namespace.getNamespace(nsPrefix, schemaLocation);
            }


            rootEl = new Element(localName, ns);
            Set<Namespace> seenNSSet = new HashSet<Namespace>();
            for (Namespace ans : namespaceMap.values()) {
                if (ans != ns) {
                    rootEl.addNamespaceDeclaration(ans);
                    seenNSSet.add(ans);
                }
            }
            if (namespaceMap.isEmpty()) {
                for (Namespace ans : nsMap.values()) {
                    if (ans != ns) {
                        rootEl.addNamespaceDeclaration(ans);
                    }
                }
            }
        } else {
            Namespace ns = namespaceMap.get("");
            if (ns == null) {
                rootEl = new Element(qName);
            } else {
                rootEl = new Element(qName, ns);
                Set<Namespace> seenNSSet = new HashSet<Namespace>();
                for (Namespace ans : namespaceMap.values()) {
                    if (ans != ns) {
                        rootEl.addNamespaceDeclaration(ans);
                        seenNSSet.add(ans);
                    }
                }
                if (namespaceMap.isEmpty()) {
                    for (Namespace ans : nsMap.values()) {
                        if (ans != ns) {
                            rootEl.addNamespaceDeclaration(ans);
                        }
                    }
                }
            }
        }
        toXML(rootEl, root, namespaceMap);
        return rootEl;
    }


    public static Map<String, Namespace> getNamespaces(JSONObject js) throws JSONException {
        Map<String, Namespace> nsMap = new HashMap<String, Namespace>();
        final JSONArray names = js.names();
        if (names != null) {
            for (int i = 0; i < names.length(); i++) {
                String name = names.getString(i);
                if (name.startsWith("_@")) {
                    String prefix = name.substring(2);
                    String uri = js.getString(name);
                    if (prefix.equals("default")) {
                        Namespace ns = Namespace.getNamespace("", uri);
                        nsMap.put("", ns);
                    } else {
                        Namespace ns = Namespace.getNamespace(prefix, uri);
                        nsMap.put(prefix, ns);
                    }
                }
            }
        }
        return nsMap;
    }

    public static List<String> getAttributeNames(JSONObject js) throws JSONException {
        List<String> attributeNames = new LinkedList<String>();
        final JSONArray names = js.names();
        if (names != null) {
            for (int i = 0; i < names.length(); i++) {
                String name = names.getString(i);
                if (name.startsWith("@")) {
                    attributeNames.add(name);
                }
            }
        }
        return attributeNames;
    }

    public static List<String> getChildren(JSONObject js) throws JSONException {
        List<String> childNames = new LinkedList<String>();
        final JSONArray names = js.names();
        if (names != null) {
            for (int i = 0; i < names.length(); i++) {
                String name = names.getString(i);
                if (!name.startsWith("@") && !name.equals(TEXT)) {
                    childNames.add(name);
                }
            }
        }

        return childNames;
    }


    protected void toXML(Element parentEl, JSONObject parent, Map<String, Namespace> namespaceMap) throws JSONException {
        if (parent.has(TEXT)) {
            parentEl.setText(parent.getString(TEXT));
        }
        final List<String> attributeNames = getAttributeNames(parent);
        if (!attributeNames.isEmpty()) {
            for (String name : attributeNames) {
                String value = parent.getString(name);
                parentEl.setAttribute(prepAttribute2(name, value));
            }
        }
        final List<String> childNames = getChildren(parent);
        if (!childNames.isEmpty()) {
            for (String childName : childNames) {
                Object obj = parent.get(childName);
                if (obj instanceof JSONArray) {
                    JSONArray carr = (JSONArray) obj;
                    for (int i = 0; i < carr.length(); i++) {
                        JSONObject childJS = carr.getJSONObject(i);
                        Element child = createElement(childName, namespaceMap);
                        parentEl.addContent(child);
                        toXML(child, childJS, namespaceMap);
                    }
                } else {
                    if (!(obj instanceof JSONObject)) {
                        System.out.println(obj);
                    }
                    JSONObject childJS = (JSONObject) obj;
                    Element child = createElement(childName, namespaceMap);
                    parentEl.addContent(child);
                    toXML(child, childJS, namespaceMap);
                }
            }
        }
    }


    public static Element createElement(String qName, Map<String, Namespace> namespaceMap) {
        int idx = qName.indexOf(":");
        if (idx == -1) {
            Namespace ns = namespaceMap.get("");
            if (ns != null) {
                return new Element(qName, ns);
            }
            return new Element(qName);
        }
        String[] toks = qName.split(":");
        String localName = toks[1];
        String nsPrefix = toks[0];
        if (!namespaceMap.isEmpty()) {
            Namespace ns = namespaceMap.get(nsPrefix);
            Assertion.assertNotNull(ns);
            return new Element(localName, ns);
        } else {
            Namespace ns = nsMap.get(nsPrefix);
            if (ns == null) {
                ns = Namespace.getNamespace(nsPrefix, "no-url");
            }
            return new Element(localName, ns);
        }
    }

    public static Attribute prepAttribute(String qName, String value) {
        int idx = qName.indexOf("__");
        if (idx == -1) {
            return new Attribute(qName, value);
        }
        String[] toks = qName.split("__");
        String localName = toks[1];
        String nsPrefix = toks[0];
        Namespace ns = nsMap.get(nsPrefix);
        if (ns == null) {
            ns = Namespace.getNamespace(nsPrefix, "no-url");
        }
        return new Attribute(localName, value, ns);
    }

    public static Attribute prepAttribute2(String qName, String value) {
        // remove the prefix @
        qName = qName.substring(1, qName.length());
        int idx = qName.indexOf(":");
        if (idx == -1) {
            return new Attribute(qName, value);
        }
        String[] toks = qName.split(":");
        String localName = toks[1];
        String nsPrefix = toks[0];
        Namespace ns = nsMap.get(nsPrefix);
        if (ns == null) {
            ns = Namespace.getNamespace(nsPrefix, "no-url");
        }
        return new Attribute(localName, value, ns);
    }

    public static String toValidJSVarName(String s) {
        return s.replaceAll(":", "__");
    }

    public static void test1(String xmlFile) throws Exception {
        SAXBuilder builder = new SAXBuilder();
        Document doc = builder.build(new File(xmlFile));
        Element rootEl = doc.getRootElement();

        XML2JSONConverter converter = new XML2JSONConverter();
        JSONObject json = converter.toJSON(rootEl);

        System.out.println(json.toString(2));
        System.out.println("======================================================");

        String jsonFile = xmlFile.replaceFirst("\\.xml$", ".json");
        if (!jsonFile.equals(xmlFile)) {
            Utils.saveText(json.toString(2), jsonFile);
        }

        Element docEl = converter.toXML(json);

        XMLOutputter xmlOutputter = new XMLOutputter(Format.getPrettyFormat());

        doc = new Document();
        doc.setRootElement(docEl);
        System.out.println("==============================================");
        xmlOutputter.output(doc, System.out);
    }

    public static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("XML2JSONConverter", options);
        System.exit(1);
    }

    public static void cli(String[] args) throws Exception {
        Option help = new Option("h", "print this message");
        Option xfOption = Option.builder("f").argName("xml-file").hasArg()
                .desc("XML file to convert to JSON").required().build();
        Options options = new Options();
        options.addOption(help);
        options.addOption(xfOption);
        CommandLineParser cli = new DefaultParser();
        CommandLine line = null;
        try {
            line = cli.parse(options, args);
        } catch (Exception x) {
            System.err.println(x.getMessage());
            usage(options);
        }

        if (line.hasOption("h")) {
            usage(options);
        }
        String xmlFile = line.getOptionValue('f');

        SAXBuilder builder = new SAXBuilder();
        Document doc = builder.build(new File(xmlFile));
        Element rootEl = doc.getRootElement();

        XML2JSONConverter converter = new XML2JSONConverter();
        JSONObject json = converter.toJSON(rootEl);

        System.out.println(json.toString(2));
        System.out.println("======================================================");

        String jsonFile = xmlFile.replaceFirst("\\.xml$", ".json");
        if (!jsonFile.equals(xmlFile)) {
            Utils.saveText(json.toString(2), jsonFile);
        }
        System.out.println("wrote " + jsonFile);

    }


    public static void main(String[] args) throws Exception {
        //  test1("/tmp/open_source_brain_projects.xml");
        //testDrive();
        //   test1("/tmp/cinergi/00528801-446A-4CE2-BFDA-6F23E75820FA.xml");
        cli(args);
    }

    public static void testDrive() throws Exception {
        String xmlSource = "http://hydro10.sdsc.edu/metadata/NOAA_NGDC/053B250F-3EAB-4FA5-B7D0-52ED907A6526.xml";
        SAXBuilder builder = new SAXBuilder();
        Document doc = builder.build(xmlSource);
        Element rootEl = doc.getRootElement();


        XML2JSONConverter converter = new XML2JSONConverter();
        JSONObject json = converter.toJSON(rootEl);

        System.out.println(json.toString(2));

        String jsonFile = "/tmp/053B250F-3EAB-4FA5-B7D0-52ED907A6526.json";
        Utils.saveText(json.toString(2), jsonFile);

        //ObjectMapper mapper = new ObjectMapper();
        //mapper.registerModule( new JsonOrgModule());
        //JsonNode jsonNode = mapper.valueToTree(json);

        //Object result = JsonPath.read(jsonNode, "$._children.._name");
        //System.out.println("result:" + result);

        //  System.out.print("Press a key to continue:");
        //  System.in.read();
        List<JSONObject> foundNodes = JSONQuery.findNodeByElemName(json, "gmd:verticalElement");

        for (JSONObject js : foundNodes) {
            System.out.println(js.toString(2));
        }


        Element docEl = converter.toXML(json);

        XMLOutputter xmlOutputter = new XMLOutputter(Format.getPrettyFormat());

        doc = new Document();
        doc.setRootElement(docEl);
        System.out.println("==============================================");
        xmlOutputter.output(doc, System.out);
    }


}
