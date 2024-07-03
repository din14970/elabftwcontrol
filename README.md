<p align="left">
<a href="https://github.com/din14970/elabftwcontrol/actions"><img alt="Actions Status" src="https://github.com/din14970/elabftwcontrol/workflows/build/badge.svg"></a>
<a href="https://pypi.org/project/elabftwcontrol/"><img alt="PyPI" src="https://img.shields.io/pypi/v/elabftwcontrol.svg?style=flat"></a>
<a href='https://coveralls.io/github/din14970/elabftwcontrol?branch=master'><img src='https://coveralls.io/repos/github/din14970/elabftwcontrol/badge.svg?branch=master' alt='Coverage Status' /></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
</p>

# Elabftwcontrol

Elabftwcontrol is a library and CLI tool for interacting with the popular electronic lab notebook (ELN) software [eLabFTW](https://www.elabftw.net/).
It works by implementing higher level abstractions on top of the basic [api](https://github.com/elabftw/elabapi-python/).
It is a 3rd party tool not officially supported by eLabFTW maintainers.
Elabftwcontrol should not to be confused with [elabctl](https://github.com/elabftw/elabctl), the official tool for managing an eLabFTW installation.

Things elabftwcontrol aims to facilitate:
* downloading data from experiments and items in various (tabular) formats
* uploading or mutating experiments and items
* managing templates, item categories and other eLabFTW resources declaratively with yaml files (partially implemented)

⚠️ Elabftwcontrol is currently a work in progress and the API may be prone to change at any time

## Installation
You can install elabftwcontrol with pip (if you will use it as a library) or pipx (if you will use it via the CLI):

```
$ pip install elabftwcontrol

or

$ pipx install elabftwcontrol
```

## Usage

### CLI
Use the `elabftwctl` command to interact with eLabFTW. Check out the options via `--help`.

### Python
Documentation and examples are a work in progress, at this moment you will have to look through the code.
