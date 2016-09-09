#!/bin/sh
mvn -q -f ../common/pom.xml exec:java -Dexec.mainClass="org.neuinfo.foundry.common.ingestion.HarvestDescriptorConfigCLI" -Dexec.args="$*"
