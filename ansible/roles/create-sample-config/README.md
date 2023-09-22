Role Name
=========

The create-sample-config role creates a sample configuration file for the spark master. It uses host delegation to query the supervisor for pillar values. This appears to cause a failure within jinja templates and cause all hosts of a hostgroup to have the fqdn of the delegation host. Unsure if this is a known issue, so simply making the config sample a dedicated role that runs after run-spark-cluster role.

Requirements
------------

The [supervisor] group in the ansible playbook must have the supervisor defined and be the only host in the group. This is because the role uses host delegation to query the supervisor for pillar values.

Role Variables
--------------

A description of the settable variables for this role should go here, including any variables that are in defaults/main.yml, vars/main.yml, and any variables that can/should be set via parameters to the role. Any variables that are read from other roles and/or the global scope (ie. hostvars, group vars, etc.) should be mentioned here as well.

Dependencies
------------

A list of other roles hosted on Galaxy should go here, plus any details in regards to parameters that may need to be set for other roles, or variables that are used from other roles.

Example Playbook
----------------

Including an example of how to use your role (for instance, with variables passed in as parameters) is always nice for users too:

    - hosts: servers
      roles:
         - { role: username.rolename, x: 42 }

License
-------

BSD

Author Information
------------------

An optional section for the role authors to include contact information, or a website (HTML is not allowed).
