##
# Project Title
#
# @file
# @version 0.1

.DEFAULT: plantuml_files
.PHONY: plantuml_files clean
PLANTUML_FILES = $(shell find . -type f -name '*.plantuml')
SVG_FILES = $(PLANTUML_FILES:.plantuml=.svg)
plantuml_files: $(SVG_FILES)

%.svg: %.plantuml
	@mkdir -p "$(@D)"
	plantuml -tsvg "$<"

clean:
	rm -f $(SVG_FILES)
# end
