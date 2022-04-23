"""
Wrapper around docker_image and ctr_image modules to manage container images.
This allows to use a single interface whether using the old docker_image
module or the new ctr_image.
"""

from ansible.errors import AnsibleActionFail
from ansible.executor.module_common import get_action_args_with_defaults
from ansible.plugins.action import ActionBase
from ansible.utils.display import Display


class ActionModule(ActionBase):
    """
    Action plugin to present a single container_image interface for both
    docker_image and ctr_image modules, allowing to handle container
    images the same way when using docker or containerd.
    """
    SUPPORTED_MODULES = [
        'auto',
        'ctr',
        'docker',
    ]
    DOCKER_UNUSED_ARGUMENTS = [
        'delete_all_refs',
        'digest',
        'digests',
        'names',
    ]
    CTR_ARGUMENTS = [
        'delete_all_refs',
        'digest',
        'digests',
        'name',
        'names',
        'namespace',
        'state',
    ]

    def docker(self):
        """Wrapper around docker_image module."""
        module_name = 'docker_image'
        module_args = self._module_args

        for arg in self.DOCKER_UNUSED_ARGUMENTS:
            if arg in module_args:
                module_args.pop(arg)

        module_args = get_action_args_with_defaults(
            module_name,
            module_args,
            self._task.module_defaults,
            self._templar,
        )
        self._display.vvvv("Running module '{0}'".format(module_name))
        return self._execute_module(
            module_name=module_name,
            module_args=module_args,
            task_vars=self._task_vars,
        )

    def ctr(self):
        """Wrapper around ctr_image custom module."""
        module_name = 'ctr_image'
        module_args = {}

        for arg in self.CTR_ARGUMENTS:
            if arg in self._module_args:
                module_args[arg] = self._module_args[arg]

        module_args = get_action_args_with_defaults(
            module_name,
            module_args,
            self._task.module_defaults,
            self._templar,
        )
        self._display.vvvv("Running module '{0}'".format(module_name))
        return self._execute_module(
            module_name=module_name,
            module_args=module_args,
            task_vars=self._task_vars,
        )

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
                self._task.delegate_to
            )
            os_major_version = self._templar.template(
                "{{ hostvars['%s']['ansible_facts']['os_major_version'] }}" %
                self._task.delegate_to
            )
        else:
            os_family = self._templar.template(
                '{{ ansible_facts.os_family }}'
            )
            os_major_version = self._templar.template(
                '{{ ansible_facts.distribution_major_version }}'
            )

        if os_family == 'RedHat' and int(os_major_version) >= 8:
            return self.ctr()
        return self.docker()

    def run(self, tmp=None, task_vars=None):
        """Action plugin entrypoint"""
        self._result = super(ActionModule, self).run(tmp, task_vars)
        del tmp

        self._display = Display()
        self._task_vars = task_vars
        self._module_args = self._task.args.copy()
        module = self._module_args.pop('use', 'auto')

        if module not in self.SUPPORTED_MODULES:
            raise AnsibleActionFail(
                "Unsupported module '{0}'.".format(module)
            )

        self._result.update(getattr(self, module)())

        return self._result
