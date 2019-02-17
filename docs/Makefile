SPHINXBUILD := python3 -m sphinx
SOURCEDIR   := .
BUILDDIR    := _build
SPHINXOPTS  := -W

.PHONY: help clean

help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS)
	@echo "  clean       to remove generated output"

clean:
	rm -rf "$(BUILDDIR)"

%:
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS)
