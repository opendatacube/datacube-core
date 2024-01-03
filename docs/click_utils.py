# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2024 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
from docutils.nodes import literal_block, section, title, make_id
from sphinx.domains import Domain
from docutils.parsers.rst import Directive
import importlib

import click


class ClickHelpDirective(Directive):
    has_content = True
    required_arguments = 1

    def run(self):
        root_cmd = self.arguments[0]

        env = self.state.document.settings.env

        group = find_script_callable_from_env(root_cmd, env)

        return [generate_help_text(group, [root_cmd])]


def find_script_callable_from_env(name, env):
    commands = env.config.click_utils_commands

    module, function_name = commands[name].split(':')
    module = importlib.import_module(module)
    return getattr(module, function_name)


def find_script_callable(name):
    try:
        from importlib_metadata import entry_points
    except ModuleNotFoundError:
        from importlib.metadata import entry_points
    return list(entry_points(
        group='console_scripts', name=name))[0].load()


def generate_help_text(command, prefix):
    ctx = click.Context(command)
    help_opts = command.get_help_option(ctx).opts
    full_cmd = ' '.join(prefix)
    block = section(None,
                    title(None, full_cmd),
                    ids=[make_id(full_cmd)], names=[full_cmd])
    if help_opts:
        h = "$ {} {}\n".format(full_cmd, help_opts[0]) + command.get_help(ctx)
        block.append(literal_block(None, h, language='console'))

    if isinstance(command, click.core.MultiCommand):
        for c in command.list_commands(ctx):
            c = command.resolve_command(ctx, [c])[1]
            block.append(generate_help_text(c, prefix+[c.name]))

    return block


def make_block(command, opt, content):
    h = "$ {} {}\n".format(command, opt) + content
    return section(None,
                   title(None, command),
                   literal_block(None, h, language='console'),
                   ids=[make_id(command)], names=[command])


class DatacubeDomain(Domain):
    name = 'datacube'
    label = 'Data Cube'
    directives = {
        'click-help': ClickHelpDirective,
    }


def setup(app):
    app.add_config_value('click_utils_commands', {}, 'html')

    app.add_domain(DatacubeDomain)
    return {
        'parallel_read_safe': False,
        'parallel_write_safe': False,
    }
