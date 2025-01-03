# AsyncArrowReader Prototype

Setup Guide

## Prerequisites

* Python 3.8 or later
* CMake 3.15 or later
* C++ compiler supporting C++11 or later

## Setup

## System Dependencies

On Debian/Ubuntu:

```shell
sudo apt-get update
sudo apt-get install -y     \
    python3-dev             \
    cmake                   \
    build-essential         \
    libarrow-dev
```

Note: See [Install Apache Arrow](https://arrow.apache.org/install/) for instructions on installing Arrow C++ on Linux.

Or on macOS:

```shell
brew install apache-arrow cmake curl
```

### Python dependencies

Python dependencies are defined in [pyproject.toml](./pyproject.toml) and are handled automatically when you run`pip install .`.

The prototype depends on the following packages:

* pyarrow
* aiohttp
* nanobind (build only)
* scikit-build-core (build only)

### Docker

As an alternative to the above steps, a [Dockerfile](./Dockerfile) is provided which builds and runs the prototype.

```sh
docker build -t prototype .
docker run prototype
```

## Build

```shell
pip install .

# Or install in such a way that VSCode/clangd gives nice autocomplete
pip install "scikit-build-core>=0.5.0" "nanobind>=1.5.0" # required for --no-build-isolation
pip install -e . -v --config-settings=build-dir=build --no-build-isolation
```

## Run

```shell
python3 example.py
```
