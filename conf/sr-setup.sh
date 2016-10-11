#! /bin/bash
#########################################
## Setup service resgitry dev environment
#########################################


if [ $(uname) == "Darwin" ]
then
   # Commands specific for Mac OS X
   echo "Vagrant setup for Mac OS X"
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ] 
then
   # Commands specific for Linux environments
   echo "Vagrant setup for Linux"
elif [ "$(expr substr $(uname -s) 1 6)" == "CYGWIN_NT" ] 
then
   # Commands specific for Cygwin
   export VAGRANT_DETECTED_OS=cygwin
   echo "Vagrant setup for CygWin Environments"
fi
vagrant up
