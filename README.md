[![PyPI version](https://badge.fury.io/py/blp.svg)](https://badge.fury.io/py/blp)
[![Conda](https://img.shields.io/conda/v/conda-forge/blp)](https://anaconda.org/conda-forge/blp)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.5187880.svg)](https://doi.org/10.5281/zenodo.5187880)

# About

`blp` provides a simple pythonic interface to the `blpapi` package. `blp` is the next iteration of the
[pdblp](https://github.com/matthewgilbert/pdblp) package. `blp` was designed with the following in mind

1. Explicit separation of session management, event parsing and event aggregation
2. Extensibility

# Installation

You can install from PyPI using

```
pip install blp
```

or from conda-forge using

```
conda install -c conda-forge blp
```

# Getting Started

Reading the docs is the best way to get started, take a look at
[Quickstart](https://matthewgilbert.github.io/blp/quickstart.html) or if you are having issues
[Troubleshooting](https://matthewgilbert.github.io/blp/quickstart.html#Troubleshooting) 

# Contributing

Please refer to [Contributing](https://matthewgilbert.github.io/blp/contributing.html) for guidelines on contributing. 

### Bloomberg Documentation

For documentation on the Bloomberg API check out the Developer's Guide. For documentation on relevant Bloomberg fields
for accessing data, check out the Reference Guide: Services and Schemas. To access these, from a  Bloomberg Terminal
go `WAPI <GO>` -> `API Developer's Guide`.
