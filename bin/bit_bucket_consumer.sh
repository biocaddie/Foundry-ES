#!/bin/bash

java -cp foundry-consumers-1.0-SNAPSHOT-prod.jar org.neuinfo.foundry.consumers.jms.consumers.BitBucketConsumer $*
