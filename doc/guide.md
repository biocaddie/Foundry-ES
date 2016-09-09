User Guide
==========

Quick Start (on tavi.neuinfo.org)
-----------

MongoDB cluster, ActiveMQ and Elasticsearch can be started as follows

    sudo start_mongo_cluster.sh
    sudo activemq start
    sudo elasticsearch start

To stop them

    sudo stop_mongo_cluster.sh
    sudo activemq stop
    sudo elasticsearch stop


### Running the demo 

start with a clean slate

     demo_initiate.sh

Start dispatcher in its own terminal

     demo_dispatcher.sh

Start consumers in their own terminals. You can exit a consumer by pressing Enter.

     demo_annotate_entities.sh
     demo_checkpoint_consumer.sh
     demo_index.sh

Start ingestion process (In the example below the batchID is `20140605` and 20 documents will be ingested. 
Default is ten documents. 

    demo_cinergi_ingestor_cli.sh -b 20140605 -n 20


To query the indexed documents for keywords

    curl -XGET localhost:9200/cinergi/_search -d '{"query":{"match":{"keywords.term":"Temperature"}}}'


### Scripts for demo 

To start dispatcher

    dispatcher.sh  

To stop dispatcher, use `Ctrl-C`.

To ingest test data 

    doc_ingestor_cli.sh <batchNum>

To see usage info

    ./doc_ingestor_cli.sh 
    usage: CINERGIIngestorCLI
     -n <numDocs>                 number of documents to WAF documents to
                                  index [1-100] default:10
     -b <batchId e.g. 20140528>   batchId in YYYYMMDD format
     -h                           print this message
    
Index checkpoint consumer

    index_cp_consumer.sh

Elasticsearch indexing consumer index_doc.sh 

    ./index_doc.sh 
    usage: ElasticSearchIndexerJMSConsumer
    -p <index-path>   Elasticsearch index-path for REST api
    -c <command>      command (one of [i,d] where d is for delete index)
    -h                print this message
    -u <server-url>   Elasticsearch server url such as
                      'http://localhost:9200'

   
### Dispatcher Configuration

The dispatcher is configured via the `dispatcher-cfg.xml` file residing in 
`$FOUNDRY_HOME/dispatcher/src/main/resources`.

```xml
    <dispatcher-cfg>
       <mongo-config db="discotest" collection="records">
         <servers>
            <server host="burak.crbs.ucsd.edu" port="27017"/>
            <server host="burak.crbs.ucsd.edu" port="27018"/>
         </servers>
       </mongo-config>
       <activemq-config>
          <brokerURL>tcp://localhost:61616</brokerURL>
       </activemq-config>
       <checkpoint-file>/var/burak/foundry/mongo-dispatcher-cp.xml</checkpoint-file>
       <queues>
           <queue name="foundry.indexCheckpoint" headerFields="History.batchId,SourceInfo.SourceID"></queue>
      </queues>
      <routes>
         <route>
            <condition>
                <predicate name="processing.status" op="eq" value="new"/>
            </condition>
            <to>foundry.new</to>
         </route>
         <route>
            <condition>
                <predicate name="processing.status" op="eq" value="index_cp"/>
            </condition>
            <to>foundry.indexCheckpoint</to>
         </route>
         <route>
            <condition>
                <predicate name="processing.status" op="eq" value="index"/>
            </condition>
            <to>foundry.index</to>
         </route>
      </routes>
    </dispatcher-cfg>
```

### Configuration of XML document ingestor
All ingestors are configured via the `ingestor-cfg.xml` file residing in 
`$FOUNDRY_HOME/ingestor/src/main/resources` directory.
```xml
    <ingestor-cfg>
      <mongo-config db="discotest">
        <servers>
          <server host="burak.crbs.ucsd.edu" port="27017"/>
          <server host="burak.crbs.ucsd.edu" port="27018"/>
        </servers>
      </mongo-config>
      <source nifId="nlx_152590">
         <xml-file path="/tmp/open_source_brain_projects.xml" rootEl="projects" docEl="project"/>
      </source>
    </ingestor-cfg>
```
Here the data to ingest for source with nifId `nlx_152590` is read from 
the XML file `/tmp/open_source_brain_projects.xml` where each document to be ingested are rooted at the tag `project` under the `projects` tag.

### Configuration of Consumers
All consumers are configured via the `consumers-cfg.xml` file residing in 
`$FOUNDRY_HOME/consumers/src/main/resources` directory.
```xml
    <consumers-cfg>
      <mongo-config db="discotest" collection="records">
        <servers>
          <server host="burak.crbs.ucsd.edu" port="27017"/>
          <server host="burak.crbs.ucsd.edu" port="27018"/>
        </servers>
      </mongo-config>
      <activemq-config>
         <brokerURL>tcp://localhost:61616</brokerURL>
      </activemq-config>
    </consumers-cfg>
```

