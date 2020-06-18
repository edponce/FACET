PKGDIR  := facet
TESTDIR := tests
PYTHON  := python3
DOCDIR  := doc

.PHONY: help build bdist sdist clean doc clean_doc

help:
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  build       to make package distribution"
	@echo "  bdist       to make wheel binary distribution"
	@echo "  sdist       to make source distribution"
	@echo "  clean       to remove build, cache, and temporary files"
	@echo "  doc         to make HTML documentation"
	@echo "  clean_doc   to remove documentation"

build:
	$(PYTHON) setup.py build
	@echo "Setup build finished."

bdist:
	$(PYTHON) setup.py bdist_wheel
	@echo "Setup wheel binary distribution finished."

sdist:
	$(PYTHON) setup.py sdist
	@echo "Setup source distribution finished."

clean:
	find . \( -name __pycache__ -o -name "*.py?" \) -print0 | xargs -0 rm -rf
	rm -rf flake8.out
	rm -rf *.egg-info .eggs
	rm -rf .tox
	rm -rf .coverage .coverage.* coverage.* htmlcov
	rm -rf .pytest_cache
	rm -rf dist build

doc:
	$(MAKE) -C "$(DOCDIR)" html

clean_doc:
	$(MAKE) -C "$(DOCDIR)" clean
	rm -rf "$(DOCDIR)/html"
