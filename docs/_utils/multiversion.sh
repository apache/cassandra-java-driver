#! /bin/bash	

cd .. && sphinx-multiversion docs/source docs/_build/dirhtml \
    --pre-build './docs/_utils/javadoc.sh' \
    --pre-build "find . -mindepth 2 -name README.md -execdir mv '{}' index.md ';'"
