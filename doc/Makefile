# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = source
BUILDDIR      = build
SPHINXBUILD   = sphinx-build
SPHINXAPIDOC  = sphinx-apidoc
GENERATEDDIR  = source/generated
CODEDIR       = ../src/blp/
EXCLUDE       = ../src/blp/test/

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

clean:
	rm -rf "$(BUILDDIR)" "$(GENERATEDDIR)"

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXAPIDOC) --separate -o "$(GENERATEDDIR)" "$(CODEDIR)" $(EXCLUDE)
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)