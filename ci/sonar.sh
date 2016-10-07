#!/bin/bash -ex

pushd `dirname $0`/.. > /dev/null
root=$(pwd -P)
popd > /dev/null

cd mainServiceController

mvn clean org.jacoco:jacoco-maven-plugin:prepare-agent install -Pcoverage-per-test
