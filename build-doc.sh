set -e
# Set up python environment
#pyenv local 3.10.1
#pip install mkdocs mkdocs-material pip install mkdocs-awesome-pages-plugin

# In some bash/zsh environments, the locale is not set correctly, which causes mkdocs to fail.
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8

# Build Javadoc
mvn clean install -DskipTests # or guava-shaded can not be found
# mvn javadoc:javadoc -pl core,query-builder,mapper-runtime
mvn javadoc:aggregate

# Substitute the reference.yaml. I didn't find a better alternative.
sed -i '' "/<SUBSTITUE_ME>/{
    r core/src/main/resources/reference.conf
    d
}" manual/core/configuration/reference/README.md

# Build manual with API references
mkdocs build # or `mkdocs serve` to preview

# revert the substitution
sed -i '' "/{
    r core/src/main/resources/reference.conf
    d
}/<SUBSTITUE_ME>" manual/core/configuration/reference/README.md