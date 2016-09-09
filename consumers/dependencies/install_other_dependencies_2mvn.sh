#!/bin/sh

mvn install:install-file -Dfile=$HOME/dev/java/Foundry/consumers/dependencies/lib/opennlp-maxent-3.0.3.jar -DgroupId=bnlp -DartifactId=opennlp-maxent -Dversion=3.0.3 -Dpackaging=jar

mvn install:install-file -Dfile=$HOME/dev/java/Foundry/consumers/dependencies/lib/opennlp-tools-1.5.3.jar -DgroupId=bnlp -DartifactId=opennlp-tools -Dversion=1.5.3 -Dpackaging=jar

mvn install:install-file -Dfile=$HOME/dev/java/Foundry/consumers/dependencies/lib/jsvmlight.jar -DgroupId=bnlp -DartifactId=jsvmlight -Dversion=0.1 -Dpackaging=jar

mvn install:install-file -Dfile=$HOME/dev/java/Foundry/consumers/dependencies/lib/commons-cli-1.3-SNAPSHOT.jar -DgroupId=commons-cli -DartifactId=commons-cli -Dversion=1.3-SNAPSHOT -Dpackaging=jar


mvn install:install-file -Dfile=$HOME/dev/java/Foundry/consumers/dependencies/lib/bnlpkit-cinergi-models-0.1.jar -DgroupId=bnlp -DartifactId=bnlpkit-cinergi-models -Dversion=0.1 -Dpackaging=jar

