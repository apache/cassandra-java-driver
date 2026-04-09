set -e
# Set up python environment
#pyenv local 3.10.1
#pip install mkdocs mkdocs-material mkdocs-awesome-pages-plugin mkdocs-macros-plugin

# In some bash/zsh environments, the locale is not set correctly, which causes mkdocs to fail.
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8

# Build Javadoc
mvn clean install -DskipTests # or guava-shaded can not be found
# mvn javadoc:javadoc -pl core,query-builder,mapper-runtime
mvn javadoc:aggregate

# Build manual with API references
mkdocs build -s # strict, so it fails with warning. You can also use `mkdocs serve` to preview
