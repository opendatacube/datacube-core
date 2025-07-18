# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import os
import sys

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
sys.path.insert(0, os.path.abspath(".."))
sys.path.insert(0, os.path.abspath("."))
on_rtd = os.environ.get("READTHEDOCS", None) == "True"

# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
# needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx_autodoc_typehints",
    "sphinx.ext.graphviz",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.extlinks",
    "sphinx.ext.mathjax",
    "sphinx_click.ext",
    "click_utils",
    # 'autodocsumm',
    "nbsphinx",
    # 'sphinx.ext.napoleon',
    "sphinx_design",
    # 'sphinx.ext.autosectionlabel',
    "sphinx_toolbox.more_autodoc.autonamedtuple",
    "IPython.sphinxext.ipython_console_highlighting",  # Highlights notebook cells
]

# Sphinx 8.1.3 does not manage to resolve the cyclic JsonLike type alias, so mock it.
autodoc_mock_imports = ["datacube.utils.json_types"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# The suffix of source filenames.
source_suffix = {".rst": "restructuredtext", ".md": "restructuredtext"}

# The master toctree document.
master_doc = "index"

# General information about the project.
project = "Open Data Cube"

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = "1.9"
# The full version, including alpha/beta/rc tags.
# FIXME: obtain real version by running git
release = version

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
# today = ''
# Else, today_fmt is used as the format for a strftime call.
# today_fmt = '%B %d, %Y'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ["README.rst", ".condaenv", ".direnv", "_build"]

# If true, '()' will be appended to :func: etc. cross-reference text.
add_function_parentheses = True

# If true, sectionauthor and moduleauthor directives will be shown in the
# output. They are ignored by default.
show_authors = False

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "friendly"

autosummary_generate = True
autoclass_content = "both"

autodoc_default_options = {"autosummary": True, "inherited-members": True}

extlinks = {
    "issue": ("https://github.com/opendatacube/datacube-core/issues/%s", "issue %s"),
    "pull": ("https://github.com/opendatacube/datacube-core/pull/%s", "PR %s"),
}

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "xarray": ("https://docs.xarray.dev/en/stable/", None),
}

graphviz_output_format = "svg"

# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "pydata_sphinx_theme"

html_theme_options = {
    "navigation_depth": 2,  # defaults to 4
    "show_toc_level": 2,
    # "header_links_before_dropdown": 3,
    # "navbar_align": "left",
    "show_prev_next": False,
    "collapse_navigation": True,
    "use_edit_page_button": True,
    # "footer_items": ["odc-footer"],
    "secondary_sidebar_items": ["page-toc", "edit-this-page"],
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/opendatacube/datacube-core",
            "icon": "fab fa-github",
        },
        {
            "name": "Discord",
            "url": "https://discord.com/invite/4hhBQVas5U",
            "icon": "fab fa-discord",
        },
    ],
}

html_context = {
    "github_user": "opendatacube",
    "github_repo": "datacube-core",
    "github_version": "develop",
    "doc_path": "docs",
}

html_logo = "_static/odc-logo-horizontal.svg"
html_static_path = ["_static"]

# The name of an image file (within the static path) to use as favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
# html_favicon = None

# If not '', a 'Last updated on:' timestamp is inserted at every page bottom,
# using the given strftime format.
html_last_updated_fmt = "%b %d, %Y"


# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
html_show_sphinx = False

# Output file base name for HTML help builder.
htmlhelp_basename = "ODCdoc"

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
    ("index", "ODC.tex", "Open Data Cube Documentation", "Open Data Cube", "manual")
]

numfig = True


def setup(app):
    # Fix bug where code isn't being highlighted
    app.add_css_file("pygments.css")
    app.add_css_file("custom.css")

    app.add_object_type(
        "confval",
        "confval",
        objname="configuration value",
        indextemplate="pair: %s; configuration value",
    )


# Clean up generated documentation files that RTD seems to be having trouble with
if on_rtd:
    import shutil

    shutil.rmtree("./dev/generate", ignore_errors=True)
