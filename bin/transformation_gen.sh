#!/bin/bash
mvn -q -f ../common/pom.xml exec:java -Dexec.mainClass="org.neuinfo.foundry.common.transform.IdentityTransformationGenerator" -Dexec.args="$*"
