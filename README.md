# AsyncArrowReader Prototype

Setup Guide

## Prerequisites

* Python 3.8 or later
* CMake 3.15 or later
* C++ compiler supporting C++11 or later

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

## Python dependencies

```shell
pip install      \
    pyarrow      \
    nanobind     \
    pytest       \
    aiohttp     \
    scikit-build-core
```

## Build

```shell
pip install .

# Or install in such a way that VSCode/clangd gives nice autocomplete
pip install -e . -v --config-settings=build-dir=build --no-build-isolation
```

## Run

```shell
python3 example.py
```
