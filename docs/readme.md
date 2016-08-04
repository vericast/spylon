# How to build the `spylon` docs


```bash
# Clone the spylon directory somewhere
git clone https://github.com/maxpoint/spylon

# Create a conda environment to build the docs
conda create -n spylon-docs spylon sphinx numpydoc -c conda-forge

# Activate the newly created conda environment
source activate spylon-docs

# Install sphinxcontrib-napoleon to convert numpy-style doc strings
# to rst syntax
pip install sphinxcontrib-napoleon

# Remove the spylon conda package
conda remove spylon --force

# install spylon from source
cd spylon
pip install -e .

# navigate to the docs dir
cd docs

# make the docs!
make html

# open the built html in a browser
google-chrome _build/html/index.html
```

