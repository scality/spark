#!/bin/bash

if [[ -z "${SUDO}" ]] ; then
    if [[ $EUID -ne 0 ]]; then
        echo -e "This script is not being run as root. We will preface each"
        echo -e "command with sudo. If this user does not have sudo permissions"
        echo -e "extract the offline archive as root."
        export sudo=sudo
    else
        export sudo=''
    fi
else
    echo "SUDO Enironment variable set. Using sudo to execute all commands."
fi

${sudo} cd ../ && mv ./staging /var/tmp/
${sudo} tar -C /root -x -v -f /var/tmp/staging/spark-repo.tar spark/ansible
${sudo} echo -e "spark-offline-archive.run has extracted itself.\n"
${sudo} echo -e "To continue with the staged deployment cd into ~/spark/ansible"
${sudo} echo -e"and execute run.yml without the staging tag."
