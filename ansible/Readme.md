# Using Ansible & Containers to deploy the Spark Orphan Tool


## Building the Spark offline archive

Customer environments which are dark sites or do not have internet access from
the RING platform will need to have the tool staged (copied over FTP or other
methods) for deployment. This can be achieved by using Ansible directly (given 
the correct version and modules), or by using a prebuilt container with Ansible 
and mapping the ssh-agent and keys into the container.

### Requirements

Ansible relies on SSH connection. Whatever machine the staging host is run on
(even localhost) must be accessible via ssh. The script also will install 
makeself package to build and archive which means the ssh key must work for
the root user.

* Have your SSH key setup in github and authorized/enabled for SSO. 
  * Should be able to `git clone git@github.com:scality/spark.git` using the ssh
  key without errors.

#### The spark-deployment container requirements
The spark-deployment container has Ansible and modules built in, there is no 
need to install Ansible or any Ansible modules. An ssh agent and volume mappings
allow the container to ssh to systems with the same experience as the user from
the container host. 

* Use an SSH Agent with the SSH key added that can connect to the inventory host
in the `[staging]` group
* Set the REGISTRY_USER variable to match a registry.scality.com API user 
* Set the REGISTRY_PASSWORD variable to matching API password

When using docker set environment variables in all upper case. Inside the 
inventory file you will see the same variables in all lower case.
 
#### The spark-deployment container

##### Pulling the spark-deployment image from registry
```commandline
[docker|podman] pull registry.scality.com/spark/spark-deployment:latest
```

##### Building the spark-deployment image locally

If you do not have access to registry.scality.com you an build the image.

```commandline
cd spark/ansible
docker build . -f Containerfile -t registry.scality.com/spark/spark-deployment:latest
```

##### Generating the spark offline archive with the spark-deployment container 
```commandline
[docker|podman] run --privileged \
  --rm \
  --net host \
  -i -t \
  --userns=keep-id \
  --volume /var/tmp:/var/tmp:rw \
  -e "SSH_AUTH_SOCK=/ssh-agent" \
  --volume ${SSH_AUTH_SOCK}:/ssh-agent \
  -e "REGISTRY_USER=Trevor_Benson" \
  -e "REGISTRY_PASSWORD=<CLI/API_KEY>" \
  -v ~/.ssh:/ansible/.ssh:rw \
  registry.scality.com/spark/spark-deployment:latest \
  stage
```

```asciidoc
TAG: stage
[WARNING]: Ansible is being run in a world writable directory (/ansible), ignoring it as an ansible.cfg source. For more information
see https://docs.ansible.com/ansible/devel/reference_appendices/config.html#cfg-in-world-writable-dir

PLAY [staging] *************************************************************************************************************************

TASK [Gathering Facts] *****************************************************************************************************************
[DEPRECATION WARNING]: Distribution fedora 35 on host localhost should use /usr/bin/python3, but is using /usr/bin/python for backward 
compatibility with prior Ansible releases. A future Ansible release will default to using the discovered platform python for this host.
 See https://docs.ansible.com/ansible-core/2.11/reference_appendices/interpreter_discovery.html for more information. This feature will
 be removed in version 2.12. Deprecation warnings can be disabled by setting deprecation_warnings=False in ansible.cfg.
ok: [localhost]

TASK [stage-spark-cluster : Remove staging directory and old content] ******************************************************************
changed: [localhost] => (item=/var/tmp/staging/)
ok: [localhost] => (item=/var/tmp/staging/../spark)

TASK [stage-spark-cluster : Create empty staging directory] ****************************************************************************
changed: [localhost]

TASK [stage-spark-cluster : Ensure makeself is installed] ******************************************************************************
ok: [localhost]

TASK [stage-spark-cluster : Ensure makeself is installed for debian] *******************************************************************
skipping: [localhost]

TASK [stage-spark-cluster : Archive the spark repository into the staging directory] ***************************************************
changed: [localhost]

TASK [stage-spark-cluster : Login to the registry registry.scality.com] ****************************************************************
changed: [localhost]

TASK [stage-spark-cluster : Pull containers from registry registry.scality.com] ********************************************************
changed: [localhost] => (item=registry.scality.com/spark/spark-master:latest)
changed: [localhost] => (item=registry.scality.com/spark/spark-worker:latest)
changed: [localhost] => (item=registry.scality.com/s3utils/s3utils:1.12.5)

TASK [stage-spark-cluster : Save the images into the staging directory] ****************************************************************
changed: [localhost] => (item=registry.scality.com/spark/spark-master:latest)
changed: [localhost] => (item=registry.scality.com/spark/spark-worker:latest)
changed: [localhost] => (item=registry.scality.com/s3utils/s3utils:1.12.5)

TASK [stage-spark-cluster : Generate setup.sh for makeself] ****************************************************************************
changed: [localhost]

TASK [stage-spark-cluster : Create SFX] ************************************************************************************************
changed: [localhost]

TASK [stage-spark-cluster : Cleanup staging temp files] ********************************************************************************
changed: [localhost] => (item=/var/tmp/staging/)
ok: [localhost] => (item=/var/tmp/staging/../spark)

TASK [stage-spark-cluster : debug] *****************************************************************************************************
ok: [localhost] => {
    "msg": [
        "The staging archive is built and available on the staging host, localhost:/var/tmp/spark-offline-archive.run.",
        "Place this file on the customer supervisor. It is self extracting and will generate an archive",
        "inside /var/tmp/staging/. The spark repository will be extracted to /root/spark.",
        "The spark ansible playbooks can then be used to configure and deploy the spark master, spark",
        "workers, s3utils and an nginx sidecar if your architecture requires it.",
        "",
        "Don't forget to configure your inventory to match the customer environment, then run:",
        "",
        "# ACTIVATE A VENV CONTAINING ANSIBLE #",
        "source /srv/scality/s3/s3-offline/venv/bin/activate",
        "cd /root/spark/ansible",
        "ansible-playbook -i inventory run.yml"
    ]
}

PLAY [sparkmaster:sparkworkers] ********************************************************************************************************

TASK [Gathering Facts] *****************************************************************************************************************
ok: [stor-03]
ok: [stor-02]
ok: [stor-04]
ok: [stor-01]
ok: [stor-06]
ok: [stor-05]

PLAY RECAP *****************************************************************************************************************************
localhost                  : ok=12   changed=9    unreachable=0    failed=0    skipped=1    rescued=0    ignored=0   
stor-01                    : ok=1    changed=0    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
stor-02                    : ok=1    changed=0    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
stor-03                    : ok=1    changed=0    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
stor-04                    : ok=1    changed=0    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
stor-05                    : ok=1    changed=0    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   
stor-06                    : ok=1    changed=0    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   

```


The resulting SFX/SEA (Self eXtracting File / Self Extracting Archive) will be 
written to ``/var/tmp/spark-offline-archive.run``. Get it staged onto the 
supervisor and execute it. It will provide all the components to continue with
a fully offline deployment of the spark tools.

#### The Spark Ansible run.yml playbook requirements
The run.yml playbook uses Ansible, the community.general Ansible modules and/or
the local Container Runtime Engine (docker or podman) on the staging host.

Ansible is configured to expect ssh keys to connect with inventory hosts.
The `--ask-pass` flag can be passed to `ansible-playbook` to request the 
password for the user instead, or set in persistently in `ansible.cfg`.

* Define a registry user who can pull spark images:
  * Inside `inventory` it will be `registry_user`
  * As a command line variable it will be `REGISTRY_USER`
* Define the password for the user:
  * Inside `inventory` it will be `registry_password`
  * As a command line variable it will be `REGISTRY_PASSWORD`

#### Using the Spark Ansible run.yml playbook

If you define the registry user and password in the inventory, then run:

```commandline
ansible-playbook -i inventory --tags stage run.yml 
```

Otherwise define them on the CLI

```commandline
ansible-playbook -i inventory --tags stage -e "REGISTRY_USER=User_Name" -e "REGISTRY_PASSWORD=asjdfaklsjflkajshdf" run.yml 
```

