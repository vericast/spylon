#!/bin/bash
mkdir _static
TARGET_FILE="index.rst"
cp ../readme.rst $TARGET_FILE
echo "
Complete Reference (API)
========================

Contents:

.. toctree::
   :maxdepth: 2

   modules
   spylon
   spylon.simple
   spylon.spark
" >> $TARGET_FILE
