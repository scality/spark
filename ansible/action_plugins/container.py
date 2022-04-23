"""
Wrapper around docker_container module and manifests + systemd unit
to manage containers.
This allows to use a single interface whether using the old docker_container
module or the new approach with container manifests and systemd.
"""
import os.path
import shlex
import tempfile

import yaml
from ansible import constants as C
from ansible.errors import AnsibleAction
from ansible.errors import AnsibleActionFail
from ansible.errors import AnsibleUndefinedVariable
from ansible.executor.module_common import get_action_args_with_defaults
from ansible.module_utils._text import to_bytes
from ansible.parsing.yaml.dumper import AnsibleDumper
from ansible.parsing.yaml.objects import AnsibleUnicode
from ansible.plugins.action import ActionBase
from ansible.utils.display import Display
from manifest import ContainerManifest
from manifest import PodManifest


class ActionModule(ActionBase):
    """`container` action plugin"""

    SUPPORTED_MODULES = [
        'auto',
        'docker',
        'systemd',
    ]

    # If the following keys are not set during the plugin call,
    # it will try to retrieve them from the following variables
    # from the task vars.
    DEFAULTS_FROM_TASK_VARS = {
        'log_directory': 'container_plugin_defaults.log_directory',
        'manifest_dir': 'container_plugin_defaults.manifest_dir',
        'manifest_owner': 'container_plugin_defaults.manifest_owner',
        'manifest_group': 'container_plugin_defaults.manifest_group',
        'manifest_mode': 'container_plugin_defaults.manifest_mode',
        'namespace': 'container_plugin_defaults.namespace',
        'network_mode': 'container_plugin_defaults.network_mode',
        'pull': 'container_plugin_defaults.pull',
        'systemd_template': 'container_plugin_defaults.systemd_template',
        'use': 'container_plugin_defaults.use',
        'manage_container_dir':
            'container_plugin_defaults.manage_container_dir',
    }

    DOCKER_UNUSED_ARGUMENTS = [
        'log_directory',
        'log_path',
        'manifest_dir',
        'manage_container_dir',
        'manifest_owner',
        'manifest_group',
        'manifest_mode',
        'namespace',
        'post_start',  # TODO: Handle post_start for Docker
        'systemd_template',
    ]
    CTR_MANIFEST_ARGUMENTS = [
        'capabilities',
        'cap_drop',
        'command',
        'entrypoint',
        'env',
        'log_path',
        'privileged',
        'readonly',
        'user',
        'volumes',
    ]
    POD_MANIFEST_ARGUMENTS = [
        'log_directory',
        'network_mode',
        'ports',
        'privileged',
    ]

    def _docker_module_args(self):
        module_args = self._module_args

        for param in self.DOCKER_UNUSED_ARGUMENTS:
            module_args.pop(param, None)

        return module_args

    def docker(self):
        module_name = 'docker_container'
        module_args = get_action_args_with_defaults(
            module_name,
            self._docker_module_args(),
            self._task.module_defaults,
            self._templar,
        )
        self._display.vvvv("Running module '{0}'".format(module_name))
        self._result = self._execute_module(
            module_name=module_name,
            module_args=module_args,
            task_vars=self._task_vars,
        )

    def _write_content_to_tmp_file(self, content):
        tmp_fd, tmp_filename = tempfile.mkstemp(dir=C.DEFAULT_LOCAL_TMP)
        tmp_file = os.fdopen(tmp_fd, 'wb')
        content = to_bytes(content)
        try:
            tmp_file.write(content)
        except Exception as err:
            os.remove(tmp_filename)
            raise Exception(err)
        finally:
            tmp_file.close()
        return tmp_filename

    def _systemd_generate_ctr_manifest(self):
        ctr_manifest_args = {'mounts': []}
        for arg in self.CTR_MANIFEST_ARGUMENTS:
            if self._module_args.get(arg) is not None:
                ctr_manifest_args[arg] = self._module_args[arg]

        def to_list(value):
            if value is None:
                return []
            if not isinstance(value, list):
                value = [value]
            return value

        ctr_manifest_args['drop_capabilities'] = to_list(
            ctr_manifest_args.pop('cap_drop', None),
        )
        ctr_manifest_args['add_capabilities'] = to_list(
            ctr_manifest_args.pop('capabilities', None),
        )

        for volume in ctr_manifest_args.pop('volumes', []):
            mount = {}
            mount['host_path'], mount['container_path'], rights = (
                volume.split(':', 2) + ['']
            )[:3]
            for right in rights.split(','):
                if right == 'ro':
                    mount['readonly'] = True
            ctr_manifest_args['mounts'].append(mount)

        if ctr_manifest_args.get('user'):
            user, _, group = ctr_manifest_args.pop('user').partition(':')
            if user:
                ctr_manifest_args['user'] = user
            if group:
                ctr_manifest_args['group'] = group

        if not ctr_manifest_args.get('log_path'):
            ctr_manifest_args['log_path'] = \
                '{0}.log'.format(self._module_args['name'])

        def sh_to_list(value):
            if isinstance(value, (AnsibleUnicode, str)):
                value = shlex.split(value)
            return value

        ctr_manifest_args['args'] = sh_to_list(
            ctr_manifest_args.pop('command', None),
        )
        ctr_manifest_args['command'] = sh_to_list(
            ctr_manifest_args.pop('entrypoint', None),
        )

        return ContainerManifest(
            self._module_args['name'],
            self._module_args['image'],
            **ctr_manifest_args
        ).get()

    def _systemd_generate_pod_manifest(self):
        pod_manifest_args = {}
        for arg in self.POD_MANIFEST_ARGUMENTS:
            if self._module_args.get(arg) is not None:
                pod_manifest_args[arg] = self._module_args[arg]

        mapping_keys = ('container_port', 'host_port', 'host_ip')
        for port in pod_manifest_args.pop('ports', []):
            indice = 0
            mapping = {}
            for data in port.split(':', 2)[::-1]:
                if mapping_keys[indice] in ("container_port", "host_port"):
                    mapping[mapping_keys[indice]] = int(data)
                else:
                    mapping[mapping_keys[indice]] = data
                indice += 1

            pod_manifest_args.setdefault('port_mappings', []).append(mapping)

        return PodManifest(
            self._module_args['name'],
            self._module_args['namespace'],
            **pod_manifest_args
        ).get()

    def _systemd_remove_manifests(self):
        module_name = 'file'
        module_args = {'state': 'absent'}
        module_res = []

        for kind in ('pod', 'ctr', 'poststart'):
            module_args['path'] = self._systemd_manifest_path(kind)
            module_res.append(
                self._execute_module(
                    module_name=module_name,
                    module_args=module_args,
                    task_vars=self._task_vars,
                ),
            )

        return module_res

    def _systemd_manifest_path(self, kind):
        filename = '{0}-{1}.yml'.format(
            kind,
            self._module_args['name'],
        )
        return os.path.join(self._module_args['manifest_dir'], filename)

    def _systemd_copy_manifests(self):
        module_name = 'copy'
        module_res = []
        module_args = {}

        for arg in ('group', 'mode', 'owner'):
            manifest_arg = 'manifest_{0}'.format(arg)
            if manifest_arg in self._module_args:
                module_args[arg] = self._module_args[manifest_arg]

        module_args = get_action_args_with_defaults(
            module_name,
            module_args,
            self._task.module_defaults,
            self._templar,
        )

        for kind, manifest in self._result['manifests'].items():
            if not manifest:
                continue
            tmp_file = self._write_content_to_tmp_file(
                yaml.dump(
                    manifest,
                    Dumper=AnsibleDumper,
                    default_flow_style=False,
                ),
            )
            client_temp_file = '/tmp/manifest-{0}'.format(
                os.path.basename(tmp_file))
            self._transfer_file(tmp_file, client_temp_file)
            module_args['src'] = client_temp_file
            module_args['dest'] = self._systemd_manifest_path(kind)
            module_res.append(
                self._execute_module(
                    module_name=module_name,
                    module_args=module_args,
                    task_vars=self._task_vars,
                ),
            )

        return module_res

    def _manage_container_ctr(self, state):

        """
        For state 'absent', calling local manage-container.py to stop
        container + pod before removing the systemd unit file
        """

        manifests_path = {}
        manage_container_path = \
            self._module_args['manage_container_dir'] + '/manage-container.py'
        module_name = 'command'

        if state == 'absent':
            for kind in ('pod', 'ctr'):
                manifests_path[kind] = self._systemd_manifest_path(kind)
            cmd = "{} remove -p {} -c {}".format(
                manage_container_path,
                manifests_path['pod'],
                manifests_path['ctr']
                )
            module_args = {
                '_raw_params': cmd,
                'removes': manifests_path['ctr'],
            }

        return self._execute_module(
            module_name=module_name,
            module_args=module_args,
            task_vars=self._task_vars,
        )

    def _systemd_manage_container(self, state, enabled=True):
        module_name = 'systemd'

        manifests_changed = any(
            f.get('changed') for f in self._result.get('files', {})
        )
        if (state not in ("stopped", "absent")) and (
                self._module_args.get('restart') or
                self._module_args.get('recreate') or
                (state == 'started' and manifests_changed)):
            state = 'restarted'

        if state == "absent":
            self._result['command'] = self._manage_container_ctr(state)
            self._raise_if_failed('command')
            state = "stopped"

        unit_name = self._module_args['systemd_template']
        if not self._module_args.get('detach', True):
            unit_name += '-oneshot'
            enabled = False

        module_args = {
            'name': '{0}@{1}.service'.format(
                unit_name,
                self._module_args['name'],
            ),
            'state': state,
            'enabled': enabled,
        }
        return self._execute_module(
            module_name=module_name,
            module_args=module_args,
            task_vars=self._task_vars,
        )

    def _raise_if_failed(self, module):
        results = self._result[module]
        if not isinstance(results, list):
            results = [results]

        for result in results:
            if result.get('failed', False):
                raise AnsibleAction(result={'failed': True})

    def _compute_changed_status(self):
        for module in ('systemd', 'files'):
            results = self._result.get(module, [])
            if not isinstance(results, list):
                results = [results]
            if any(result.get('changed', False) for result in results):
                self._result['changed'] = True

    def systemd(self):
        state = self._module_args.pop('state', 'started')
        # If no image is provided we do not try to generate manifests
        # and only handle start/stop.
        image = self._module_args.get('image')

        if state in ('present', 'started') and image:
            ctr = self._systemd_generate_ctr_manifest()
            pod = self._systemd_generate_pod_manifest()
            self._result['manifests'] = {
                'ctr': ctr,
                'pod': pod,
                'poststart': self._module_args.get('post_start'),
            }
            self._result['files'] = self._systemd_copy_manifests()
            self._raise_if_failed('files')

        if state in ('started', 'stopped'):
            self._result['systemd'] = self._systemd_manage_container(state)
            self._raise_if_failed('systemd')

        if state == 'absent':
            self._result['systemd'] = self._systemd_manage_container(
                state, enabled=False,
            )
            self._raise_if_failed('systemd')
            self._result['files'] = self._systemd_remove_manifests()
            self._raise_if_failed('files')

        self._compute_changed_status()

    def auto(self):
        """
        Try to guess which module to use depending on the OS family and
        the OS version.
        If OS family is RedHat and the version is greater or equal than
        8, we use the new systemd approach, otherwise we rely on the former one
        using `docker_container` module.
        """
        if self._task.delegate_to:
            os_family = self._templar.template(
                "{{ hostvars['%s']['ansible_facts']['os_family'] }}" %
                self._task.delegate_to,
            )
            os_major_version = self._templar.template(
                "{{ hostvars['%s']['ansible_facts']['os_major_version'] }}" %
                self._task.delegate_to,
            )
        else:
            os_family = self._templar.template(
                '{{ ansible_facts.os_family }}',
            )
            os_major_version = self._templar.template(
                '{{ ansible_facts.distribution_major_version }}',
            )

        if os_family == 'RedHat' and int(os_major_version) >= 8:
            self.systemd()
        else:
            self.docker()

    def _get_defaults_from_task_vars(self):
        """Retrieve the action plugin defaults"""
        for arg, var in self.DEFAULTS_FROM_TASK_VARS.items():
            if arg in self._module_args:
                continue
            keys = var.split('.')
            value = self._task_vars
            for key in keys:
                if not isinstance(value, dict) or key not in value:
                    break
                value = value.get(key)
            else:
                try:
                    self._module_args[arg] = self._templar.template(value)
                except AnsibleUndefinedVariable:
                    raise AnsibleActionFail(
                        "Default '{0}' for module argument '{1}' use an "
                        'undefined variable'.format(var, arg),
                    )

    def run(self, tmp=None, task_vars=None):
        """Action plugin entrypoint"""
        self._result = super(ActionModule, self).run(tmp, task_vars)
        del tmp

        self._display = Display()
        self._task_vars = task_vars
        self._module_args = self._task.args.copy()
        self._get_defaults_from_task_vars()
        module = self._module_args.pop('use', 'auto')

        if module not in self.SUPPORTED_MODULES:
            raise AnsibleActionFail(
                "Unsupported module '{0}'.".format(module),
            )

        try:
            getattr(self, module)()
        except AnsibleAction as exc:
            self._result.update(exc.result)

        return self._result
