#!/bin/bash

[[ -z "${DOCKER_RUNTIME}" ]] && export DOCKER_RUNTIME=docker

[[ -n "${1}" ]] && export TAG=$1
echo "TAG: ${TAG}"

if [ -n "${STAGING_HOST_IP}" ] ; then
  sed -i "s/#staging_host ansible_host={{IP_ADDRESS}}/staging_host ansible_host=${STAGING_HOST_IP}/" inventory.sample
  sed -i "s/^localhost docker=podman/staging_host docker=${DOCKER_RUNTIME}"
fi

if [[ -z "${TAG}" ]] ; then
  echo "ERROR: No subcommand found."
  echo "Subcommands available: [stage|deploy]"
elif [[ "${TAG}" != "stage" ]] && [[ "${TAG}" != "deploy" ]] ; then
  echo "ERROR: Unrecognized subcommand found."
  echo "Subcommands available: [stage|deploy]"
elif [ -z "${REGISTRY_USER}" ] ||  [ -z "${REGISTRY_PASSWORD}" ] ; then
  echo "ERROR: REGISTRY_USER and REGISTRY_PASSWORD not defined."
else
  ansible-playbook -i inventory.sample run.yml \
    -e "registry_user=${REGISTRY_USER}" \
    -e "registry_password=${REGISTRY_PASSWORD}" \
    --tags "${TAG}"
fi
