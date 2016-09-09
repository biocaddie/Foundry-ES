#!/bin/bash
#mvn -f ../consumers/pom.xml  -DskipTests=false -Dtest="org.neuinfo.foundry.consumers.SourceDescFileGeneratorTest#testLincsDSResultsGeneration" test
mvn -q -f ../common/pom.xml exec:java -Dexec.mainClass="org.neuinfo.foundry.common.transform.SourceDescFileGeneratorCLI" -Dexec.args="$*"
