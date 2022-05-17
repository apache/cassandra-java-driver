# You can set these variables from the command line.
POETRY        = poetry
SPHINXOPTS    =
SPHINXBUILD   = $(POETRY) run sphinx-build
PAPER         =
BUILDDIR      = _build
SOURCEDIR     = _source

# Internal variables
PAPEROPT_a4     = -D latex_paper_size=a4
PAPEROPT_letter = -D latex_paper_size=letter
ALLSPHINXOPTS   = -d $(BUILDDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) $(SOURCEDIR)
TESTSPHINXOPTS  = $(ALLSPHINXOPTS) -W --keep-going
I18NSPHINXOPTS  = $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) $(SOURCEDIR)

# Windows variables
ifeq ($(OS),Windows_NT)
    POETRY = $(APPDATA)\Python\Scripts\poetry
endif

define javadoc
    cd .. && ./docs/_utils/javadoc.sh
endef

.PHONY: all
all: dirhtml

# Setup commands
.PHONY: setupenv
setupenv:
	pip install -q poetry

.PHONY: setup
setup:
	$(POETRY) install
	$(POETRY) update
	cp -TLr source $(SOURCEDIR)
	cd $(SOURCEDIR) && find . -name README.md -execdir mv '{}' index.md ';'

# Clean commands
.PHONY: pristine
pristine: clean
	git clean -dfX

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)/*
	rm -rf $(SOURCEDIR)/*

# Generate output commands
.PHONY: dirhtml
dirhtml: setup
	@$(javadoc)
	$(SPHINXBUILD) -b dirhtml $(ALLSPHINXOPTS) $(BUILDDIR)/dirhtml
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

.PHONY: singlehtml
singlehtml: setup
	@$(javadoc)
	$(SPHINXBUILD) -b singlehtml $(ALLSPHINXOPTS) $(BUILDDIR)/singlehtml
	@echo
	@echo "Build finished. The HTML page is in $(BUILDDIR)/singlehtml."

.PHONY: multiversion
multiversion: setup
	$(POETRY) run ./_utils/multiversion.sh
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

.PHONY: epub
epub: setup
	$(SPHINXBUILD) -b epub $(ALLSPHINXOPTS) $(BUILDDIR)/epub
	@echo
	@echo "Build finished. The epub file is in $(BUILDDIR)/epub."

.PHONY: epub3
epub3:setup
	$(SPHINXBUILD) -b epub3 $(ALLSPHINXOPTS) $(BUILDDIR)/epub3
	@echo
	@echo "Build finished. The epub3 file is in $(BUILDDIR)/epub3."

.PHONY: dummy
dummy: setup
	$(SPHINXBUILD) -b dummy $(ALLSPHINXOPTS) $(BUILDDIR)/dummy
	@echo
	@echo "Build finished. Dummy builder generates no files."

# Preview commands
.PHONY: preview
preview: setup
	$(POETRY) run sphinx-autobuild -b dirhtml $(ALLSPHINXOPTS) $(BUILDDIR)/dirhtml --port 5500 --re-ignore 'api/*'

.PHONY: multiversionpreview
multiversionpreview: multiversion
	$(POETRY) run python -m http.server 5500 --directory $(BUILDDIR)/dirhtml

# Test commands
.PHONY: test
test: setup
	$(SPHINXBUILD) -b dirhtml $(TESTSPHINXOPTS) $(BUILDDIR)/dirhtml
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

.PHONY: linkcheck
linkcheck: setup
	$(SPHINXBUILD) -b linkcheck $(SOURCEDIR) $(BUILDDIR)/linkcheck