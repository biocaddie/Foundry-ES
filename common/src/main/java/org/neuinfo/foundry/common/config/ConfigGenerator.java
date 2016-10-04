package org.neuinfo.foundry.common.config;

import org.apache.commons.cli.*;
import org.jdom2.Element;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.util.*;

/**
 * Created by bozyurt on 8/8/16.
 */
public class ConfigGenerator {
    public static String HOME = System.getProperty("user.home");

    public static void loadConfigSpec(String configSpecFile, String srcCodeRoot, String profile) throws Exception {
        SystemCfg systemCfg = new SystemCfg();
        WFCfg wfCfg;
        List<ConsumerCfg> ccList = new ArrayList<ConsumerCfg>();
        String pluginDir;
        Yaml yaml = new Yaml();
        FileInputStream in = null;
        try {

            in = new FileInputStream(configSpecFile);
            Map<String, Object> map = (Map<String, Object>) yaml.load(in);
            pluginDir = (String) map.get("pluginDir");
            systemCfg.pluginDir = pluginDir;

            Map<String, String> dbMap = (Map<String, String>) map.get("database");
            if (dbMap != null) {
                if (dbMap.containsKey("host")) {
                    systemCfg.host = dbMap.get("host");
                }
                if (dbMap.containsKey("port")) {
                    systemCfg.port = Utils.getIntValue(dbMap.get("port"), systemCfg.port);
                }
                if (dbMap.containsKey("db")) {
                    systemCfg.db = dbMap.get("db");
                }
                if (dbMap.containsKey("collection")) {
                    systemCfg.collection = dbMap.get("collection");
                }
            }
            Map<String, String> mqMap = (Map<String, String>) map.get("mq");
            if (mqMap != null) {
                if (mqMap.containsKey("brokerURL")) {
                    systemCfg.brokerURL = mqMap.get("brokerURL");
                }
            }
            List<Map<String, Object>> consumers = (List<Map<String, Object>>) map.get("consumers");

            for (Map<String, Object> cMap : consumers) {
                String name = cMap.keySet().iterator().next();
                Map<String, Object> aMap = (Map<String, Object>) cMap.get(name);
                String pluginClass = (String) aMap.get("class");
                String status = (String) aMap.get("status");
                assertTrue(pluginClass != null, "The field class for the full qualified consumer plugin class name is required for the consumer " + name);
                assertTrue(status != null, "The field status for the successful output state for the consumer " + name);
                ConsumerCfg cc = new ConsumerCfg(name, pluginClass, status);
                for (String key : aMap.keySet()) {
                    if (!key.equals("class") && !key.equals("status")) {
                        cc.addParam(key, aMap.get(key).toString());
                    }
                }
                ccList.add(cc);
            }

            Map<String, Object> wfMap = (Map<String, Object>) map.get("workflow");
            String wfName = wfMap.keySet().iterator().next();
            List<String> steps = (List<String>) wfMap.get(wfName);
            wfCfg = new WFCfg(wfName);
            wfCfg.steps = steps;
            //System.out.println(systemCfg);

            Element rootEL = createConsumerConfigXML(systemCfg, ccList, wfCfg);
            String configFile = srcCodeRoot + "/consumers/src/main/resources/" + profile + "/consumers-cfg.xml";
            Utils.saveXML(rootEL, configFile);
            System.out.println("wrote " + configFile);

            rootEL = createDispatcherConfig(wfCfg, systemCfg, ccList);
            configFile = srcCodeRoot + "/dispatcher/src/main/resources/" + profile + "/dispatcher-cfg.xml";
            Utils.saveXML(rootEL, configFile);
            System.out.println("wrote " + configFile);

            rootEL = createCommonConfigXml(systemCfg);
            configFile = srcCodeRoot + "/common/src/main/resources/" + profile + "/common-cfg.xml";
            Utils.saveXML(rootEL, configFile);
            System.out.println("wrote " + configFile);

            rootEL = createManUIConfigXml(wfCfg, systemCfg, ccList);
            configFile = srcCodeRoot + "/man-ui/src/main/resources/" + profile + "/man-ui-cfg.xml";
            Utils.saveXML(rootEL, configFile);
            System.out.println("wrote " + configFile);

        } finally {
            Utils.close(in);
        }
    }

    static Element createCommonConfigXml(SystemCfg cfg) {
        Element rootEl = new Element("common-cfg");
        prepDB(cfg, rootEl);
        return rootEl;
    }


    static Element createManUIConfigXml(WFCfg wfCfg, SystemCfg cfg, List<ConsumerCfg> ccList) {
        Element rootEl = new Element("man-ui-cfg");
        prepDBMQ(cfg, rootEl);

        Map<String, ConsumerCfg> ccMap = new HashMap<String, ConsumerCfg>(11);
        for (ConsumerCfg cc : ccList) {
            ccMap.put(cc.name, cc);
        }
        String finishedStatus = getFinishedStatus(wfCfg, ccMap);
        prepWorkflow(wfCfg, ccMap, finishedStatus, rootEl);
        return rootEl;
    }


    static Element createDispatcherConfig(WFCfg wfCfg, SystemCfg cfg, List<ConsumerCfg> ccList) {
        Map<String, ConsumerCfg> ccMap = new HashMap<String, ConsumerCfg>(11);
        for (ConsumerCfg cc : ccList) {
            ccMap.put(cc.name, cc);
        }
        // String finishedStatus = getFinishedStatus(wfCfg, ccMap);
        String finishedStatus = "finished";

        Element rootEl = new Element("dispatcher-cfg");
        prepDBMQ(cfg, rootEl);
        prepWorkflow(wfCfg, ccMap, finishedStatus, rootEl);

        return rootEl;
    }

    private static String getFinishedStatus(WFCfg wfCfg, Map<String, ConsumerCfg> ccMap) {
        // get the last step status
        String lastStep = wfCfg.steps.get(wfCfg.steps.size() - 1);
        ConsumerCfg cc = ccMap.get(lastStep);
        Assertion.assertNotNull(cc);
        return cc.status;
    }

    private static void prepWorkflow(WFCfg wfCfg, Map<String, ConsumerCfg> ccMap, String finishedStatus, Element rootEl) {
        String updateOutStatus;
        ConsumerCfg firstCC = ccMap.get(wfCfg.steps.get(0));
        assertTrue(firstCC != null, "Cannot find a consumer named " + wfCfg.steps.get(0));
        updateOutStatus = firstCC.status + ".1";
        Element wfmsEl = new Element("wf-mappings");
        rootEl.addContent(wfmsEl);
        Element wfmEl = new Element("wf-mapping").setAttribute("name", wfCfg.name).setAttribute("ingestorOutStatus", "new.1")
                .setAttribute("updateOutStatus", updateOutStatus);
        wfmsEl.addContent(wfmEl);
        // FIXME: remove boilerplate steps from config not used
        wfmEl.addContent(new Element("step").setText("UUID Generation"));
        wfmEl.addContent(new Element("step").setText("Index"));

        Element wfsEl = new Element("workflows");
        rootEl.addContent(wfsEl);
        Element wfEl = new Element("workflow").setAttribute("name", wfCfg.name)
                .setAttribute("finishedStatus", finishedStatus);

        wfsEl.addContent(wfEl);
        Element routesEl = new Element("routes");
        wfEl.addContent(routesEl);


        String prevStatus = null;
        for (String step : wfCfg.steps) {
            ConsumerCfg cc = ccMap.get(step);
            assertTrue(cc != null, "Cannot find a consumer named " + step);
            Element routeEl = new Element("route");
            routesEl.addContent(routeEl);
            Element condEl = new Element("condition");
            routeEl.addContent(condEl);
            Element predEl = new Element("predicate");
            condEl.addContent(predEl);
            predEl.setAttribute("name", "processing.status")
                    .setAttribute("op", "eq");
            String value = prevStatus == null ? "new.1" : prevStatus + ".1";
            predEl.setAttribute("value", value);
            routeEl.addContent(new Element("to").setText("foundry." + cc.name + ".1"));
            prevStatus = cc.status;
        }
    }

    static Element createConsumerConfigXML(SystemCfg cfg, List<ConsumerCfg> ccList, WFCfg wfCfg) {
        Element rootEl = new Element("consumers-cfg");
        prepDBMQ(cfg, rootEl);

        rootEl.addContent(new Element("pluginDir").setText(cfg.pluginDir));
        File libDir = new File(new File(cfg.pluginDir).getParent(), "lib");
        rootEl.addContent(new Element("libDir").setText(libDir.getAbsolutePath()));

        Map<String, ConsumerCfg> ccMap = new HashMap<String, ConsumerCfg>();
        for (ConsumerCfg cc : ccList) {
            ccMap.put(cc.name, cc);
        }
        Element consumersEl = new Element("consumers");
        rootEl.addContent(consumersEl);
        String inStatus = "new.1";

        for (Iterator<String> iter =  wfCfg.steps.iterator(); iter.hasNext();) {
            String step = iter.next();
            ConsumerCfg cc = ccMap.get(step);
            assertTrue(cc != null, "Cannot find a consumer named " + step);
            Element ccEl = new Element("consumer-cfg");
            consumersEl.addContent(ccEl);
            ccEl.setAttribute("name", cc.name + ".1");
            ccEl.setAttribute("type", "generic");
            ccEl.setAttribute("listeningQueueName", "foundry." + cc.name + ".1");
            ccEl.setAttribute("inStatus", inStatus);
            if (iter.hasNext()) {
                ccEl.setAttribute("outStatus", cc.status + ".1");
            } else {
                // always make sure that the last step to have outStatus = finished
                ccEl.setAttribute("outStatus", "finished");
            }
            ccEl.addContent(new Element("pluginClass").setText(cc.pluginClass));
            if (cc.params != null) {
                Element paramsEl = new Element("params");
                ccEl.addContent(paramsEl);
                for (String name : cc.params.keySet()) {
                    String value = cc.params.get(name);
                    Element paramEl = new Element("param");
                    paramsEl.addContent(paramEl);
                    paramEl.setAttribute("name", name);
                    paramEl.setAttribute("value", value);
                }
            }
            inStatus = cc.status + ".1";
        }
        return rootEl;
    }

    private static void prepDBMQ(SystemCfg cfg, Element rootEl) {
        prepDB(cfg, rootEl);
        Element maqEl = new Element("activemq-config");
        rootEl.addContent(maqEl);
        maqEl.addContent(new Element("brokerURL").setText(cfg.brokerURL));
    }

    private static void prepDB(SystemCfg cfg, Element rootEl) {
        Element mcEl = new Element("mongo-config");
        rootEl.addContent(mcEl);
        mcEl.setAttribute("db", cfg.db);
        mcEl.setAttribute("collection", cfg.collection);
        Element serversEl = new Element("servers");
        mcEl.addContent(serversEl);
        Element serverEl = new Element("server");
        serversEl.addContent(serverEl);
        serverEl.setAttribute("host", cfg.host);
        serverEl.setAttribute("port", String.valueOf(cfg.port));
    }

    public static void assertTrue(boolean condition, String msg) {
        if (!condition) {
            throw new RuntimeException(msg);
        }
    }

    private static class SystemCfg {
        String host = "localhost";
        int port = 27017;
        String db = "discotest";
        String collection = "records";
        String brokerURL = "tcp://localhost:61616";
        String pluginDir;

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("SystemCfg{");
            sb.append("host='").append(host).append('\'');
            sb.append(", port=").append(port);
            sb.append(", db='").append(db).append('\'');
            sb.append(", collection='").append(collection).append('\'');
            sb.append(", brokerURL='").append(brokerURL).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

    private static class ConsumerCfg {
        String name;
        String pluginClass;
        String status;
        Map<String, String> params;

        public ConsumerCfg(String name, String pluginClass, String status) {
            this.name = name;
            this.pluginClass = pluginClass;
            this.status = status;
        }

        void addParam(String name, String value) {
            if (params == null) {
                params = new HashMap<String, String>(11);
            }
            params.put(name, value);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ConsumerCfg{");
            sb.append("pluginClass='").append(pluginClass).append('\'');
            sb.append(", status='").append(status).append('\'');
            sb.append(", params=").append(params);
            sb.append('}');
            return sb.toString();
        }
    }

    private static class WFCfg {
        String name;
        List<String> steps;

        public WFCfg(String name) {
            this.name = name;
        }
    }

    public static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ConfigGenerator", options);
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        Option help = new Option("h", "print this message");
        Option configFileOption = Option.builder("c").argName("cfg-spec-file").hasArg()
                .desc("Full path to the Foundry-ES config spec YAML file").build();
        Option foundryRootOption = Option.builder("f").argName("foundry-es-root-dir").hasArg().build();
        Option profileOption = Option.builder("p").argName("profile")
                .desc("Maven profile ([dev]|prod)").hasArg().build();
        configFileOption.setRequired(true);
        Options options = new Options();
        options.addOption(help);
        options.addOption(configFileOption);
        options.addOption(foundryRootOption);
        options.addOption(profileOption);
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
        String configFile = line.getOptionValue('c');
        String foundryRootDir = HOME + "/dev/java/Foundry-ES";
        if (line.hasOption('f')) {
            foundryRootDir = line.getOptionValue('f');
        }
        if (!new File(foundryRootDir).isDirectory()) {
            String fd = System.getenv("FOUNDRY_HOME");
            if (fd == null) {
                System.err.println("Please provide the root dir for Foundry-ES code via -f option or via FOUNDRY_HOME environment variable!");
                System.exit(1);
            }
            foundryRootDir = fd;
        }
        String profile = "dev";
        if (line.hasOption('p')) {
            String s = line.getOptionValue('p');
            if (s.equals("dev") || s.equals("prod")) {
                profile = s;
            }

        }
        //ConfigGenerator.loadConfigSpec(HOME + "/dev/java/Foundry-ES/bin/config.yml", foundryRootDir);
        ConfigGenerator.loadConfigSpec(configFile, foundryRootDir, profile);
    }

}
