#!/bin/bash

mvn -f ../common/pom.xml exec:java -Dexec.mainClass="org.neuinfo.foundry.common.ingestion.SourceIngestorCLI" -Dexec.args="$*"

#java -cp foundry-ingestor-1.0-SNAPSHOT-prod.jar org.neuinfo.foundry.ingestor.SourceIngestorCLI  $*
