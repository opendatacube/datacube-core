import os
import sys

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
sys.path.insert(0, os.path.abspath('..'))
sys.path.insert(0, os.path.abspath('.'))
print(sys.path)

on_rtd = os.environ.get('READTHEDOCS', None) == 'True'

# -- RTD Debugging
if on_rtd:
    import subprocess

    subprocess.call('which java', shell=True)
    subprocess.call('java -version', shell=True)
    subprocess.call('plantuml -v', shell=True)

# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
# needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx_autodoc_typehints',
    'sphinx.ext.graphviz',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    'sphinx.ext.extlinks',
    'sphinx.ext.mathjax',
    'sphinx_click.ext',
    'sphinxcontrib.plantuml',
    'click_utils',
    'sphinx.ext.napoleon',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix of source filenames.
source_suffix = ['.rst', '.md']

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = u'Open Data Cube'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = "1.8"
# The full version, including alpha/beta/rc tags.
# FIXME: obtain real version by running git
release = version+"-FIXME"

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
# today = ''
# Else, today_fmt is used as the format for a strftime call.
# today_fmt = '%B %d, %Y'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = []

# If true, '()' will be appended to :func: etc. cross-reference text.
add_function_parentheses = True

# If true, sectionauthor and moduleauthor directives will be shown in the
# output. They are ignored by default.
# show_authors = False

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'friendly'

autosummary_generate = True

extlinks = {'issue': ('https://github.com/opendatacube/datacube-core/issues/%s', 'issue '),
            'pull': ('https://github.com/opendatacube/datacube-core/pulls/%s', 'PR ')}
intersphinx_mapping = {
    'python': ('https://docs.python.org/', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable/', None),
    'numpy': ('https://docs.scipy.org/doc/numpy/', None),
    'xarray': ('https://xarray.pydata.org/en/stable/', None),
}

graphviz_output_format = 'svg'

# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
if on_rtd:
    html_theme = 'default'
else:
    import sphinx_rtd_theme

    html_theme = 'sphinx_rtd_theme'
    html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

html_theme_options = {
    'collapse_navigation': False,
    'logo_only': True,
}


html_logo = '_static/odc-logo-central-blue.svg'
html_static_path = ['_static']

# The name of an image file (within the static path) to use as favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
# html_favicon = None

# If not '', a 'Last updated on:' timestamp is inserted at every page bottom,
# using the given strftime format.
html_last_updated_fmt = '%b %d, %Y'


# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
html_show_sphinx = False

# Output file base name for HTML help builder.
htmlhelp_basename = 'ODCdoc'

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
    ('index', 'ODC.tex', u'Open Data Cube Documentation', 'Open Data Cube', 'manual')
]

plantuml_output_format = 'svg'
plantuml_latex_output_format = 'pdf'

numfig = True


def setup(app):
    # Fix bug where code isn't being highlighted
    app.add_stylesheet('pygments.css')
    app.add_stylesheet('custom.css')


# Clean up generated documentation files that RTD seems to be having trouble with
if on_rtd:
    import shutil

    shutil.rmtree('./dev/generate', ignore_errors=True)
