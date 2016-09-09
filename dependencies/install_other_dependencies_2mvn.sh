#!/bin/sh

mvn install:install-file -Dfile=$PWD/lib/jsvmlight.jar -DgroupId=bnlp -DartifactId=jsvmlight -Dversion=0.1 -Dpackaging=jar

