#!/bin/bash
pushd `dirname $0` > /dev/null
base=$(pwd -P)
popd > /dev/null


# Copy the cloud foundry properties file 
cp $base/../conf/application-cf.properties $base/../src/main/resources/application.properties

# Build Spring-boot JAR
mvn clean package

# Path to output JAR
jarfile=$base/../target/piazzaServiceController-1.0.0.BUILD-SNAPSHOT.jar

# Gather some data about the repo
source $base/vars.sh

# Do we have this artifact in s3? If not, upload it.
aws s3 ls $S3URL || aws s3 cp $jarfile $S3URL

# success if we have an artifact stored in s3.
aws s3 ls $S3URL
