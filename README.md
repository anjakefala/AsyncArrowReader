Setup Guide

# Prerequisites

* Python 3.8 or later
* CMake 3.15 or later
* C++ compiler supporting C++11 or later

# System Dependencies

sudo apt-get update
sudo apt-get install -y     \
    python3-dev             \
    cmake                   \
    build-essential         \
    libcurl4-openssl-dev    \
    libarrow-dev            \

# Python dependencies

pip install      \
    pyarrow      \
    nanobind     \
    pytest

# Build
pip3 install -e . -v

# Run
pytest . -svvv
