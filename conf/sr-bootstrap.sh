#!/usr/bin/env bash

# Update repository to fetch latest OpenJDK
sudo add-apt-repository -y ppa:openjdk-r/ppa
sudo apt-get -y update

# Install required packages
sudo apt-get -y install openjdk-8-jdk maven

# Build Service Controller application
cd /vagrant/servicereg
mvn clean package

# Run the Service Controller 
java -jar target/piazzaServiceController*.jar
