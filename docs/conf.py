# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2020 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import os
import sys

from bs4 import BeautifulSoup as bs

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
sys.path.insert(0, os.path.abspath('..'))
sys.path.insert(0, os.path.abspath('.'))
print(sys.path)
on_rtd = os.environ.get('READTHEDOCS', None) == 'True'

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
    'click_utils',
    'autodocsumm',
    'sphinx.ext.napoleon'
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
release = version

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
# today = ''
# Else, today_fmt is used as the format for a strftime call.
# today_fmt = '%B %d, %Y'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ['README.rst']

# If true, '()' will be appended to :func: etc. cross-reference text.
add_function_parentheses = True

# If true, sectionauthor and moduleauthor directives will be shown in the
# output. They are ignored by default.
show_authors = False

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'friendly'

autosummary_generate = True
autoclass_content = "both"

autodoc_default_options = {
    'autosummary': True,
    'inherited-members': True
}

extlinks = {'issue': ('https://github.com/opendatacube/datacube-core/issues/%s', 'issue '),
            'pull': ('https://github.com/opendatacube/datacube-core/pulls/%s', 'PR ')}

intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable/', None),
    'numpy': ('https://docs.scipy.org/doc/numpy/', None),
    'xarray': ('https://xarray.pydata.org/en/stable/', None),
}

graphviz_output_format = 'svg'

# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
if on_rtd:
    html_theme = 'pydata_sphinx_theme'
else:
    html_theme = 'pydata_sphinx_theme'

html_theme_options = {
    "navigation_depth": 1,
    "show_prev_next": False,
    "collapse_navigation": True,
    "use_edit_page_button": True,
    "footer_items": ["odc-footer"],
    "page_sidebar_items": [
        "page-toc",
        "autoclass_page_toc",
        "autosummary_page_toc",
        "edit-this-page"
    ],
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/opendatacube/datacube-core",
            "icon": "fab fa-github",
        },
        {
            "name": "Slack",
            "url": "http://slack.opendatacube.org/",
            "icon": "fab fa-slack",
        },
    ],
}

html_context = {
    "github_user": "opendatacube",
    "github_repo": "datacube-core",
    "github_version": "develop",
    "doc_path": "docs",
}

html_logo = '_static/odc-logo-horizontal.svg'
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

numfig = True

def custom_page_funcs(app, pagename, templatename, context, doctree):

    def get_autosummary_toc():
        soup = bs(context["body"], "html.parser")

        class_sections = soup.find(class_='class')
        if class_sections != None:
            return ""

        matches = soup.find_all('dl')
        if matches == None or len(matches) == 0:
            return ""

        out = {
            'title': '',
            'menu_items': []
        }

        #  remove the class dt
        pyclass = matches.pop(0)
        pyclass = pyclass.find('dt')
        if pyclass != None:
            out['title'] = pyclass.get('id')

        for match in matches:
            match_dt = match.find('dt')
            link = match.find(class_="headerlink")
            if link != None:
                out['menu_items'].append({
                    'title': match_dt.get('id'),
                    'link': link['href']
                })

        return out

    def get_class_toc():
        soup = bs(context["body"], "html.parser")

        class_sections = soup.find_all(class_='autosummary')
        if class_sections == None or len(class_sections) == 0:
            return ""

        out = {
            'title': '',
            'menu_items': []
        }
        class_title = soup.find(class_='class')
        if class_title == None:
            return ""

        pyclass = class_title.find('dt')
        if pyclass != None:
            out['title'] = pyclass.get('id')

        for section in class_sections:
            out_section = {
                'title': '',
                'menu_items': []
            }
            out_section['title'] = section.find_previous_sibling('p').text.replace(':','')
            matches = section.find_all('tr')
            for match in matches:
                link = match.find(class_="internal")
                
                if link != None:
                    title = link['title']
                    if title != None:
                        title = title.replace(out['title'], '')
                    out_section['menu_items'].append({
                        'title': title,
                        'link': link['href']
                    })
            if len(out_section['menu_items']) > 0:
                out['menu_items'].append(out_section)

        # print(out)
        return out

    context['get_class_toc'] = get_class_toc
    context['get_autosummary_toc'] = get_autosummary_toc



def setup(app):
    # Fix bug where code isn't being highlighted
    app.add_css_file('pygments.css')
    app.add_css_file('custom.css')

    app.connect("html-page-context", custom_page_funcs)


# Clean up generated documentation files that RTD seems to be having trouble with
if on_rtd:
    import shutil

    shutil.rmtree('./dev/generate', ignore_errors=True)
