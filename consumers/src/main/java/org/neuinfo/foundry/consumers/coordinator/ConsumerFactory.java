package org.neuinfo.foundry.consumers.coordinator;

import org.neuinfo.foundry.common.config.ConsumerConfig;
import org.neuinfo.foundry.common.config.Parameter;
import org.neuinfo.foundry.common.config.SubConsumerConfig;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.consumers.jms.consumers.*;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.*;
import org.neuinfo.foundry.consumers.plugin.IPlugin;
import org.neuinfo.foundry.consumers.plugin.Ingestable;
import org.neuinfo.foundry.consumers.plugin.Ingestor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 9/30/14.
 */
public class ConsumerFactory {
    private static ConsumerFactory instance;
    boolean collectStats = false;
    Map<String, Class<?>> registeredConsumerMap = new HashMap<String, Class<?>>();
    Map<String, Class<?>> registeredIngestorMap = new HashMap<String, Class<?>>();
    Map<String, Class<?>> registeredIngestorPluginMap = new HashMap<String, Class<?>>();


    public synchronized static ConsumerFactory getInstance() {
        return getInstance(false);
    }

    public synchronized static ConsumerFactory getInstance(boolean collectStats) {
        if (instance == null) {
            instance = new ConsumerFactory(collectStats);
            instance.collectStats = collectStats;
        }
        return instance;
    }

    private ConsumerFactory(boolean collectStats) {
        registeredConsumerMap.put("bitbucket", BitBucketConsumer.class);
        registeredConsumerMap.put("entityAnnotator", EntityAnnotationJMSConsumer.class);
        registeredConsumerMap.put("indexCheckpointer", IndexCheckpointConsumer.class);
        registeredConsumerMap.put("elasticSearchIndexer", ElasticSearchIndexerJMSConsumer.class);

        registeredIngestorMap.put("genIngestor", GenericIngestionConsumer.class);

        registeredIngestorPluginMap.put("xml", NIFXMLIngestor.class);
        registeredIngestorPluginMap.put("rss", RSSIngestor.class);
        registeredIngestorPluginMap.put("csv", NIFCSVIngestor.class);
        registeredIngestorPluginMap.put("oai", OAIIngestor.class);
        registeredIngestorPluginMap.put("waf", WAFIngestor.class);
        registeredIngestorPluginMap.put("ftp", FTPIngestor.class);
        registeredIngestorPluginMap.put("rsync", RsyncIngestor.class);
        registeredIngestorPluginMap.put("resource", ResourceIngestor.class);
        registeredIngestorPluginMap.put("aspera", AsperaIngestor.class);
        registeredIngestorPluginMap.put("web", WebIngestor.class);
        registeredIngestorPluginMap.put("disco", DISCOIngestor.class);
        registeredIngestorPluginMap.put("json2", TwoStageJSONIngestor.class);
        registeredIngestorPluginMap.put("pubmed", PubMedIngestor.class);
    }

    public IConsumer createHarvester(String type, String ingestorName, String collectionName, Map<String, String> options) throws Exception {
        Class<?> aClass = registeredIngestorMap.get(ingestorName);
        if (aClass == null) {
            throw new Exception("Not a registered harvester:" + ingestorName);
        }
        IConsumer consumer = (IConsumer) aClass.getConstructor(String.class).newInstance((String) null);

        // ingestors generate different status for each unique workflow
        consumer.setOutStatus(options.get("ingestorOutStatus"));

        consumer.setCollectionName(collectionName); // "nifRecords");

        Ingestable harvester = (Ingestable) consumer;

        Class<?> pluginClazz = registeredIngestorPluginMap.get(type.toLowerCase());
        if (pluginClazz == null) {
            throw new Exception("Harvester type '" + type + "' is not recognized!");
        }
        Ingestor plugin = (Ingestor) pluginClazz.newInstance();

        plugin.initialize(options);

        harvester.setIngestor(plugin);

        GenericIngestionConsumer gic = (GenericIngestionConsumer) consumer;
        if (collectStats) {
            // collect statistics from the consumer
            OpStatsHandler opStatsHandler = OpStatsHandler.getInstance();
            opStatsHandler.register(gic.getId(), gic.getName());
            gic.setCpListener(opStatsHandler);
        }
        return consumer;
    }


    public IConsumer createConsumer(ConsumerConfig config) throws Exception {
        Assertion.assertNotNull(config);
        if (config.getType().equals("native")) {
            String consumerName = config.getName();
            if (consumerName.indexOf(".") != -1) {
                consumerName = consumerName.substring(0, consumerName.indexOf("."));
            }
            Class<?> aClass = registeredConsumerMap.get(consumerName);

            if (aClass == null) {
                throw new Exception("Not a registered consumer:" + consumerName);
            }
            IConsumer consumer = (IConsumer) aClass.getConstructor(String.class).newInstance(config.getListeningQueueName());

            consumer.setCollectionName(config.getCollectionName());
            consumer.setInStatus(config.getInStatus());
            consumer.setOutStatus(config.getOutStatus());
            if (!config.getParameters().isEmpty()) {
                for (Parameter par : config.getParameters()) {
                    consumer.setParam(par.getName(), par.getValue());
                }
            }
            return consumer;
        } else if (config.getType().equals("generic")) {
            String pluginClass = config.getPluginClass();
            Assertion.assertNotNull(pluginClass);
            IPlugin plugin = (IPlugin) JavaPluginCoordinator.getInstance().createInstance(pluginClass);

            Map<String, String> options = new HashMap<String, String>();
            for (Parameter p : config.getParameters()) {
                options.put(p.getName(), p.getValue());
            }
            plugin.initialize(options);

            JavaPluginConsumer consumer = new JavaPluginConsumer(config.getListeningQueueName());

            consumer.setPlugin(plugin);
            //consumer.setSuccessMessageQueueName(config.getSuccessMessageQueueName());
            //consumer.setFailureMessageQueueName(config.getFailureMessageQueueName());
            consumer.setCollectionName(config.getCollectionName());
            consumer.setInStatus(config.getInStatus());
            consumer.setOutStatus(config.getOutStatus());

            if (collectStats) {
                // collect statistics from the consumer
                OpStatsHandler opStatsHandler = OpStatsHandler.getInstance();

                opStatsHandler.register(consumer.getId(), consumer.getName());
                consumer.setCpListener(opStatsHandler);
            }
            return consumer;
        } else if (config.getType().equals("composite")) {
            List<SubConsumerConfig> subConfigList = config.getSubConfigList();
            Assertion.assertTrue(subConfigList != null && !subConfigList.isEmpty());
            List<IPlugin> plugins = new ArrayList<IPlugin>();
            JavaPluginCoordinator jpCoordinator = JavaPluginCoordinator.getInstance();
            for (SubConsumerConfig scc : subConfigList) {
                IPlugin plugin = (IPlugin) jpCoordinator.createInstance(scc.getPluginClass());
                Map<String, String> options = new HashMap<String, String>();
                for (Parameter p : config.getParameters()) {
                    options.put(p.getName(), p.getValue());
                }
                plugin.initialize(options);
                plugins.add(plugin);
            }
            CompositeJavaPluginConsumer consumer = new CompositeJavaPluginConsumer(config.getListeningQueueName());
            for (IPlugin plugin : plugins) {
                consumer.addPlugin(plugin);
            }
            consumer.setCollectionName(config.getCollectionName());
            consumer.setInStatus(config.getInStatus());
            consumer.setOutStatus(config.getOutStatus());
            return consumer;
        }

        return null;
    }


}
