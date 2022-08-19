#! /bin/bash	

cd .. && sphinx-multiversion docs/source docs/_build/dirhtml \
    --pre-build "find . -mindepth 2 -name README.md -execdir mv '{}' index.md ';'" \
    --post-build './docs/_utils/javadoc.sh'
