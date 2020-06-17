PKGDIR  := facet
TESTDIR := tests
PYTHON  := python3
DOCDIR  := docs

.PHONY: help wheel build clean docs

help:
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  build       to make package distribution"
	@echo "  wheel       to make wheel binary distribution"
	@echo "  clean       to remove build, cache, and temporary files"

build:
	$(PYTHON) setup.py build
	@echo "Setup build finished."

wheel:
	$(PYTHON) setup.py bdist_wheel
	@echo "Setup wheel distribution finished."

clean:
	rm -rf flake8.out
	rm -rf *.egg-info .eggs
	rm -rf .tox
	rm -rf .coverage .coverage.* coverage.* htmlcov
	rm -rf "$(PKGDIR)"/__pycache__ "$(PKGDIR)"/*.pyc
	rm -rf "$(TESTDIR)"/__pycache__ "$(TESTDIR)"/*.pyc
	rm -rf dist build

docs:
	$(MAKE) -C "$(DOCDIR)" html

clean_docs:
	$(MAKE) -C "$(DOCDIR)" clean
