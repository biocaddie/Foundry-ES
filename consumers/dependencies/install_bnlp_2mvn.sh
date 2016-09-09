#!/bin/sh
mvn install:install-file -Dfile=$HOME/dev/java/Foundry/consumers/dependencies/lib/bnlpkit-0.5.10.jar -DgroupId=bnlp -DartifactId=bnlpkit -Dversion=0.5.10 -Dpackaging=jar
