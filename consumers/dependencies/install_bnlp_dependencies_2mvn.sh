#!/bin/sh

mvn install:install-file -Dfile=$HOME/dev/java/Foundry/consumers/dependencies/lib/commonlib-1.4.jar -DgroupId=bnlp -DartifactId=commonlib -Dversion=1.4 -Dpackaging=jar

mvn install:install-file -Dfile=$HOME/dev/java/Foundry/consumers/dependencies/lib/mallet.jar -DgroupId=bnlp -DartifactId=mallet -Dversion=1.0 -Dpackaging=jar

mvn install:install-file -Dfile=$HOME/dev/java/Foundry/consumers/dependencies/lib/mallet-deps.jar -DgroupId=bnlp -DartifactId=mallet-deps -Dversion=1.0 -Dpackaging=jar

mvn install:install-file -Dfile=$HOME/dev/java/Foundry/consumers/dependencies/lib/guilib_0.4.jar -DgroupId=bnlp -DartifactId=guilib -Dversion=0.4 -Dpackaging=jar

mvn install:install-file -Dfile=$HOME/dev/java/Foundry/consumers/dependencies/lib/jcckit.jar -DgroupId=jcckit -DartifactId=jcckit -Dversion=1.1 -Dpackaging=jar


