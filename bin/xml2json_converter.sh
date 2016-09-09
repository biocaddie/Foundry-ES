#!/bin/bash

mvn -q -f ../consumers/pom.xml exec:java -Dexec.mainClass="org.neuinfo.foundry.common.util.XML2JSONConverter" -Dexec.args="$*"
