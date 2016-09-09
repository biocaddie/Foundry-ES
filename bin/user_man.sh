#!/bin/bash

mvn -q -f ../common/pom.xml exec:java -Dexec.mainClass="org.neuinfo.foundry.common.ingestion.UserManCLI" -Dexec.args="$*"

