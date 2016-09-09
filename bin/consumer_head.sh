#!/bin/bash

mvn -f ../consumers/pom.xml exec:java -Dexec.mainClass="org.neuinfo.foundry.consumers.coordinator.ConsumerCoordinator" -Dexec.args="$*"
#java -cp foundry-consumers-1.0-SNAPSHOT-prod.jar org.neuinfo.foundry.consumers.coordinator.ConsumerCoordinator $*
