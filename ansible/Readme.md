# Using Ansible & Containers to deploy the Spark Orphan Tool

Automated deployment with Ansible is possible to ease the efforts required
to deploy and configure the cluster. This guide intends to use the Ansible 
virtual environment of S3C Federation. It is built around recent Ansible 2.9
which means you may require an updated S3C virtual environment to use these 
plabooks for deployment.

For customer environments which do not have internet access from the RING 
platform will need to have the tool staged (copied over FTP or other methods) 
for deployment can use the Spark offline archive. It will stage Ansible into
``/root/spark/ansible`` for deployment if the Federation version on the 
supervisor is new enough. If not, inside ``/var/tmp/staging`` are the tarballs
of the container images and git repository. They can be deployed with the 
manual steps of the TSKB.

## Deploying Spark Ansible via run.yml playbook 

### Requirements
The run.yml playbook uses Ansible and the community.general Ansible modules for
Docker to load container images.   
``config-SAMPLE.yml`` 

Ansible is configured to expect ssh keys to connect with inventory hosts.
The `--ask-pass` flag can be passed to `ansible-playbook` to request the 
password for the user instead, or set in persistently in `ansible.cfg`.

* The inventory defines the following groups:
  * sparkmaster
  * sparkworkers
  * supervisor
    * Used to provide a ``config-SAMPLE.yml`` Spark config for review.
    * Requirement can be disabled by using ``--skip-tags run::config-sample``
    which disbles generating a sample spark config. This option requires the
    user to build their own ``config.yml`` from the ``config-template.yml``.
* Define a registry user who can pull spark images:
  * Inside `inventory` it will be `registry_user`
  * As a command line variable it will be `REGISTRY_USER`
* Define the password for the user:
  * Inside `inventory` it will be `registry_password`
  * As a command line variable it will be `REGISTRY_PASSWORD`


### Offline mode deployment


Inside the http://github.com/scality/spark repository ansible directory is a
``spark-offline-archive.run``. As the archive is a large binary/tarball it is
stored with Git LFS. To download via git requires installing Git LFS in addition
to git on your laptop.

1. Install Git LFS on your laptop
2. Download the available offline archive from github LFS
   1. Git clone the repository
      ```commandline
      git clone git@github.com:scality/spark.git
      ```
   2. Change directories into the repo
      ```commandline
      cd ./spark
      ```
   3. Pull the additional LFS files
      ```commandline
      git lfs pull
      ```
3. Verify you have the self extracting archive
   ```commandline
   ls ./ansible/spark-offline-archive.run
   ```
   ```text
   -rwxrwxr-x. 1 user user 3403251996 May 12 08:37 ./ansible/spark-offline-archive.run
   ```
4. Copy the offline archive to the supervisor via available methods.
5. On the supervisor extract the offline archive
   ```commandline
   chmod +x ./spark-offline-archive.run
   ./spark-offline-archive.run
   ```
6. Generate an inventory file from the inventory.sample
7. Deploy the spark cluster
   ```commandline
   ansible-playbook -i inventory --tags deploy run.yml 
   ```

## Online mode deployment

The primary mode of deployment is Offline. The online mode has more requirements
which are often not met by the customers network connectivity.

Online mode presumes that:
1. the supervisor and all members of groups sparkmaster and sparkworkers have 
internet access
2. the host the spark ansible ``run.yml`` playbook is run from either has an ssh 
agent, or was forwarded an agent on connection, which includes an ssh key that 
provides access to the GitHub Scality repositories (can git clone). 
3. the registry user and password are defined in inventory, or being set on the 
CLI when executing the deployment.

```commandline
ansible-playbook -i inventory --tags deploy -e "REGISTRY_USER=User_Name" -e "REGISTRY_PASSWORD=asjdfaklsjflkajshdf" run.yml 
```

## Building the Spark offline archive

### Using the Ansible playbook 

Requirements:
* Ansible 2.9 (built and tested against)
* Ansible Galaxy community.general collection
* A host in the ``[staging]`` group with internet access
* SSH Agent/Keys which provide access to the Scality GitHub repository
* Defining the ghcr.io credentials in inventory or command line

When registry_user and registry_password (lowercase) Ansible variables are 
defined in the inventory file: 

```commandline
ansible-playbook -i inventory --tags stage run.yml 
```

When REGISTRY_USER and REGISTRY_PASSWORD (UPPERCASE) Docker environment 
variables are defined on the command line: 

```commandline
ansible-playbook -i inventory --tags stage -e "REGISTRY_USER=User_Name" -e "REGISTRY_PASSWORD=asjdfaklsjflkajshdf" run.yml 
```

### Using the spark-deployment container
The spark-deployment container has Ansible and modules built in, there is no 
need to install Ansible or any Ansible modules. An ssh agent and volume mappings
allow the container to ssh to systems with the same experience as the user from
the container host. 

* Use an SSH Agent with the SSH key added that can connect to the inventory host
in the `[staging]` group
* Set the REGISTRY_USER variable to your GitHub username. 
* Set the REGISTRY_PASSWORD variable to a [GitHub PAT](https://github.com/settings/tokens) with `packages:read` permissions.

When using docker set environment variables in all upper case. Inside the 
inventory file you will see the same variables in all lower case.
 
#### The spark-deployment container

Using this method currently requires Podman as the Docker runtime currently does
not provide the ``keep-id`` User Namespace required to properly pass along SSH
Agent and/or Keys to the container.

1. Pulling the spark-deployment image from registry
   ```commandline
   [docker|podman] pull ghcr.io/scality/spark/spark-deployment:latest
   ```

2. The spark-deployment image

   * Pull a published image if there are no changes to spark/ansible

     ```commandline
     [docker|podman] pull ghcr.io/scality/spark/spark-deployment:latest
     ```

   * Build the image from scratch if spark/ansible has been modified

     ```commandline
     cd spark/ansible
     [docker|podman] build . -f Containerfile -t ghcr.io/scality/spark/spark-deployment:latest
     ```

3. Generate the offline archive 
```commandline
[docker|podman] run --privileged \
  --rm \
  --net host \
  -i -t \
  --userns=keep-id \
  --volume /var/tmp:/var/tmp:rw \
  -e "SSH_AUTH_SOCK=/ssh-agent" \
  --volume ${SSH_AUTH_SOCK}:/ssh-agent \
  -e "REGISTRY_USER=User_Name" \
  -e "REGISTRY_PASSWORD=<CLI/API_KEY>" \
  -v ~/.ssh:/ansible/.ssh:rw \
  ghcr.io/scality/spark/spark-deployment:latest \
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

TASK [stage-spark-cluster : Login to the registry ghcr.io] ****************************************************************
changed: [localhost]

TASK [stage-spark-cluster : Pull containers from registry ghcr.io] ********************************************************
changed: [localhost] => (item=ghcr.io/scality/spark/spark-master:latest)
changed: [localhost] => (item=ghcr.io/scality/spark/spark-worker:latest)
changed: [localhost] => (item=ghcr.io/scality/s3utils:1.14.6)

TASK [stage-spark-cluster : Save the images into the staging directory] ****************************************************************
changed: [localhost] => (item=ghcr.io/scality/spark/spark-master:latest)
changed: [localhost] => (item=ghcr.io/scality/spark/spark-worker:latest)
changed: [localhost] => (item=ghcr.io/scality/s3utils:1.14.6)

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
a fully offline deployment of the spark tools either via Ansible or manually.


