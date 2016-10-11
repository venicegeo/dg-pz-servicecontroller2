#!/usr/bin/env bash

# Install software requirements
# See: https://docs.mongodb.org/v3.0/tutorial/install-mongodb-on-ubuntu/
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
echo "deb http://repo.mongodb.org/apt/ubuntu "$(lsb_release -sc)"/mongodb-org/3.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.0.list
sudo apt-get update
sudo apt-get install -y mongodb-org

# Ensure Mongo starts with each bootup
sudo update-rc.d mongod defaults

# Update configuration files
sudo service mongod stop
sudo cp /vagrant/mongodb.conf /etc/mongod.conf

# Restart Mongo
sudo service mongod start 
