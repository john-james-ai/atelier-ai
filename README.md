# Atelier AI

[![PyPI](https://img.shields.io/pypi/v/atelier-ai?color=blue)](https://pypi.python.org/pypi/atelier-ai/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/atelier-ai?style=flat-square)](https://pypi.python.org/pypi/atelier-ai/)
[![PyPI - License](https://img.shields.io/pypi/l/atelier-ai?style=flat-square)](https://pypi.python.org/pypi/atelier-ai/)
[![PyPI - Coverage](coverage.svg)](https://pypi.python.org/pypi/atelier-ai/)
[![PyPI - Implementation](https://img.shields.io/pypi/implementation/atelier-ai)](https://pypi.python.org/pypi/atelier-ai/)
[![PyPI - Maintainability](https://img.shields.io/codeclimate/maintainability-percentage/john-james-ai/atelier-ai)](https://pypi.python.org/pypi/atelier-ai/)
---

**Documentation**: [https://john-james-ai.github.io/atelier-ai](https://john-james-ai.github.io/atelier-ai)

**Source Code**: [https://github.com/john-james-ai/atelier-ai](https://github.com/john-james-ai/atelier-ai)

**PyPI**: [https://pypi.org/project/atelier-ai/](https://pypi.org/project/atelier-ai/)

---

Atelier AI: Studio for AI Designers

## Installation

```sh
pip install atelier-ai
```

## Development

* Clone this repository
* Requirements:
  * [Poetry](https://python-poetry.org/)
  * Python 3.7+
* Create a virtual environment and install the dependencies

```sh
poetry install
```

* Activate the virtual environment

```sh
poetry shell
```

### Testing

```sh
pytest
```

### Documentation

The documentation is automatically generated from the content of the [docs directory](./docs) and from the docstrings
 of the public signatures of the source code. The documentation is updated and published as a [Github project page
 ](https://pages.github.com/) automatically as part each release.

### Releasing

Trigger the [Draft release workflow](https://github.com/john-james-ai/atelier-ai/actions/workflows/draft_release.yml)
(press _Run workflow_). This will update the changelog & version and create a GitHub release which is in _Draft_ state.

Find the draft release from the
[GitHub releases](https://github.com/john-james-ai/atelier-ai/releases) and publish it. When
 a release is published, it'll trigger [release](https://github.com/john-james-ai/atelier-ai/blob/master/.github/workflows/release.yml) workflow which creates PyPI
 release and deploys updated documentation.

### Pre-commit

Pre-commit hooks run all the auto-formatters (e.g. `black`, `isort`), linters (e.g. `mypy`, `flake8`), and other quality
 checks to make sure the changeset is in good shape before a commit/push happens.

You can install the hooks with (runs for each commit):

```sh
pre-commit install
```

Or if you want them to run only for each push:

```sh
pre-commit install -t pre-push
```

Or if you want e.g. want to run all checks manually for all files:

```sh
pre-commit run --all-files
```

