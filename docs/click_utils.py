import pkg_resources
from docutils.nodes import literal_block
from sphinx.domains import Domain
from sphinx.util.compat import Directive
import importlib

import click


class ClickHelpDirective(Directive):
    has_content = True
    required_arguments = 1

    def run(self):
        root_cmd = self.arguments[0]

        env = self.state.document.settings.env

        group = find_script_callable_from_env(root_cmd, env)

        return list(generate_help_texts(group, [root_cmd]))


def find_script_callable_from_env(name, env):
    commands = env.config.click_utils_commands

    module, function_name = commands[name].split(':')
    module = importlib.import_module(module)
    return getattr(module, function_name)


def find_script_callable(name):
    return list(pkg_resources.iter_entry_points(
        'console_scripts', name))[0].load()


def generate_help_texts(command, prefix):
    ctx = click.Context(command)
    help_opts = command.get_help_option(ctx).opts
    if help_opts:
        yield make_block(
            ' '.join(prefix),
            help_opts[0],
            command.get_help(ctx),
        )

    if isinstance(command, click.core.MultiCommand):
        for c in command.list_commands(ctx):
            c = command.resolve_command(ctx, [c])[1]
            prefix.append(c.name)
            for h in generate_help_texts(c, prefix):
                yield h
            prefix.pop()


def make_block(command, opt, content):
    h = "$ {} {}\n".format(command, opt) + content
    return literal_block(h, h, language='console')


class DatacubeDomain(Domain):
    name = 'datacube'
    label = 'Data Cube'
    directives = {
        'click-help': ClickHelpDirective,
    }


def setup(app):
    app.add_config_value('click_utils_commands', {}, 'html')

    app.add_domain(DatacubeDomain)
