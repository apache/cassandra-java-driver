#!/bin/bash

# Install dependencies
mvn install -DskipTests -T 1C

# Define output folder
OUTPUT_DIR="docs/_build/dirhtml/api"
if [[ "$SPHINX_MULTIVERSION_OUTPUTDIR" != "" ]]; then
    OUTPUT_DIR="$SPHINX_MULTIVERSION_OUTPUTDIR/api"
    echo "HTML_OUTPUT = $OUTPUT_DIR" >> doxyfile
fi

# Generate javadoc
mvn javadoc:javadoc -T 1C
[ -d $OUTPUT_DIR ] && rm -r $OUTPUT_DIR
mkdir -p "$OUTPUT_DIR"
mv -f core/target/site/apidocs/* $OUTPUT_DIR
