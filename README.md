Foundry
=======

[![Build Status](https://travis-ci.org/biocaddie/Foundry-ES.svg?branch=master)](https://travis-ci.org/biocaddie/Foundry-ES)

Further Documentation
---------------------

 * [Developer Guide](doc/dev_guide.md)
 * [Document Ingestion](doc/doc_ingestion.md)


Getting the code
----------------

    cd $HOME
    git clone https://<username>@github.com/SciCrunch/Foundry-ES.git
    cd $HOME/Foundry-ES

Building
--------

Before you start the build process, you need to install three libraries 
from `dependencies` directory to your local maven repository
    
    cd $HOME/Foundry_ES/dependencies
    ./install_prov_xml_2mvn.sh
    ./install_prov_model_2mvn.sh
    ./install_prov_json__2mvn.sh

Afterwards

    mvn -Pdev clean install 

Here dev profile is used. There are production `prod` and `dev` profiles for differrent configurations for development and production environments.

The configuration files are located under each sub-project. For example, 
the configuration files for the dispatcher component are located under
`$HOME/Foundry-ES/dispatcher/src/main/resources`.

```
$HOME/Foundry-ES/dispatcher/src/main/resources
├── dev
│   └── dispatcher-cfg.xml
└── prod
    └── dispatcher-cfg.xml
```

When you use `-Pdev` argument, configuration file from the `dev` directory is 
included in the jar file.

All subsystem configuration files are generated from a master configuration file in YAML format. 
An example master configuration file can be found at `$HOME/Foundry-ES/bin/config.yml.example`.
Once you create a master config file named say `config.yml` run the following to generate all configuration files for the subsystems (for dev profile)

```
cd $HOME/Foundry-ES/bin
./config_gen.sh -c config.yml  -f $HOME/Foundry-ES -p dev

```

```
./config_gen.sh -h 
usage: ConfigGenerator
 -c <cfg-spec-file>         Full path to the Foundry-ES config spec YAML
                            file
 -f <foundry-es-root-dir>
 -h                         print this message
 -p <profile>               Maven profile ([dev]|prod)

```
After each configuration file generation you need to run maven to move the configs to their target locations

    mvn -Pdev install 

MongoDB
--------

The system uses MongoDB as its backend. Both 2.x and 3.x versions of MongoDB are tested with the system. If you are using MongoDB 3.x, preferred storage engine is wiredTiger.

ActiveMQ
--------

* Download and unpack [Apache ActiveMQ 5.10.0 Release](http://activemq.apache.org/activemq-5100-release.html) to a directory of your choosing (`$MQ_HOME`).

* To start message queue server at default port `61616`, go to `$MQ_HOME/bin` directory and run
```
    activemq start 
```
* To stop the activemq server
```
    activemq stop
```

Running the system
------------------

The system consists of a dispatcher, a consumer head and a CLI manager interface. 
The dispatcher listens to the MongoDB changes and using 
its configured workflow dispatches messages to the message queue for the 
listening consumer head(s). The consumer head coordinates a set of configured 
consumers that do a prefined operation of a document indicated by the message 
they receive from the dispatcher and ingestors. The ingestors are specialized 
consumers that are responsible for the retrieval of the original data as 
configured by harvest descriptor JSON file of the corresponding source. 
They are triggered by the manager application. 

## Initial Setup

Before any processing the MongoDB needs to be populated with the source descriptors using
the `$HOME/Foundry_ES/bin/ingest_src_cli.sh`. 

```
./ingest_src_cli.sh -h
usage: SourceIngestorCLI
 -c <config-file>        config-file e.g. ingestor-cfg.xml (default)
 -d                      delete the source given by the source-json-file
 -h                      print this message
 -j <source-json-file>   harvest source description file
 -u                      update the source given by the source-json-file
```

Example source descriptors are under `$HOME/Foundry_ES/consumers/etc`.
An example usage for inserting PDB source descriptor document to the `sources` collection is show below

```
./ingest_src_cli.sh -j $HOME/Foundry_ES/consumers/etc/pdb_rsync_gen.json
```
## Resource descriptor JSON file Generation

Once the transformation script is finalized for a resource, its resource descriptor 
JSON file needs to be regenerated. Also for new resources a new resource descriptor 
JSON file is needed. A resource descriptor JSON file is generated via the `$HOME/Foundry_ES/bin/source_desc_gen.sh` script.

```
./source_desc_gen.sh
usage: SourceDescFileGeneratorCLI
 -s <source>                       source name (top level element) in the
                                   source-descriptor-cfg-file [e.g. pdb, dryad]
 -c <source-descriptor-cfg-file>   Full path to the source descriptor
                                   config params YAML file
 -h                                print this message
```

where the resource descriptor config params are read from a YAML file. There is an example YAML file (`$HOME/Foundry_ES/bin/source-desc-cfg.yml.example`). You can copy 
it to `source-desc-cfg.yml` file and change the paths (for transformation script files) there to match your local system. An example run for dryad is shown below;
```
./source_desc_gen.sh -s dryad -c source-desc-cfg.yml
```
The generated resource descriptor file is written to `/tmp` directory.

## Dispatcher

The script for the dispatcher component `dispatcher.sh` is located in 
`$HOME/Foundry_ES/bin`. By default it uses `dispatcher-cfg.xml` file for the 
profile specified during the build. This needs to run in its own process. 
To stop it, use `Ctrl-C`.

```
 ./dispatcher.sh -h
usage: Dispatcher
 -c <config-file>   config-file e.g. dispatcher-cfg.xml (default)
 -h                 print this message
```


## Consumer Head
The script for the consumer head component `consumer_head.sh` is located in 
`$HOME/Foundry_ES/bin`. By default it uses `consumers-cfg.xml` file for the 
profile specified during the build. For production use you need to specify 
 `-f` option. This needs to run in its own process. To stop it, use `Ctrl-C`.


```
./consumer_head.sh -h
usage: ConsumerCoordinator
 -c <config-file>          config-file e.g. consumers-cfg.xml (default)
 -cm                       run in consumer mode (no ingestors)
 -f                        full data set default is 100 documents
 -h                        print this message
 -n <max number of docs>   Max number of documents to ingest
 -p                        send provenance data to prov server
 -t                        run ingestors in test mode
```

## Manager

Manager is an interactive command line application for sending ingestion start messages for resources to the consumer head(s). 
It also have some convenience functions to cleanup MongoDB data for a given resource and delete ElasticSearch indices. 
By default manager app uses `dispatcher-cfg.xml` file for the profile specified 
during the build. 

```
./manager.sh -h
usage: ManagementService
 -c <config-file>   config-file e.g. dispatcher-cfg.xml (default)
 -h                 print this message
```

```
Foundry:>> help
Available commands
	help - shows this message.
	ingest <sourceID>
	h - show all command history
	delete <url> - [e.g. http://52.32.231.227:9200/geo_20151106]
	dd <sourceID>  - delete docs for a sourceID
	cdup <sourceID>  - clean duplicate files from GridFS for a sourceID
	trigger <sourceID> <status-2-match> <queue-2-send> [<new-status> [<new-out-status>]] (e.g. trigger nif-0000-00135 new.1 foundry.uuid.1)
	run <sourceID> status:<status-2-match> step:<step-name> [on|to_end] (e.g. run nif-0000-00135 status:new.1 step:transform)
	index <sourceID> <status-2-match> <url> (e.g. index biocaddie-0006 transformed.1 http://52.32.231.227:9200/geo_20151106/dataset)
	list - lists all of the existing sources.
	status [<sourceID>] - show processing status of data source(s)
	ws - show configured workflow(s)
	exit - exits the management client.
Foundry:>> 

```



