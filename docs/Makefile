# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    = -j auto
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = OpenDataCubeDocs
SOURCEDIR     = .
BUILDDIR      = _build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)


.PHONY: help Makefile livehtml clean plantuml_diags

PLANTUML_FILES = $(shell find . -type f -name '*.plantuml')
SVG_FILES = $(PLANTUML_FILES:.plantuml=.svg)

plantuml_diags: $(SVG_FILES)

%.svg: %.plantuml
	plantuml -tsvg "$<"
# Append trailing newline to keep pre-commit happy
	@echo >> "$@"

# Make sure that source files don't fall through to the catch-all
%.plantuml: ;

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%:: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

livehtml:
#	sphinx-autobuild -b html --host 0.0.0.0 --port 8123 --watch "../datacube" --re-ignore "dev/api/generate/.*\.rst" $(SPHINXOPTS) . $(BUILDDIR)/html
	sphinx-autobuild -b html --host 0.0.0.0 --port 8123 --watch "../datacube" --ignore _build/ --re-ignore ".*/generate/.*" $(SPHINXOPTS) . $(BUILDDIR)/html

clean:
	rm -rf _build/
	rm -rf dev/api/generate data-access-analysis/apis/generate api/indexed-data/generate api/geometry/generate
