#!/bin/sh
mvn install:install-file -Dfile=$PWD/lib/bnlpkit-models-0.6.jar -DgroupId=bnlp -DartifactId=bnlpkit-models -Dversion=0.6 -Dpackaging=jar

