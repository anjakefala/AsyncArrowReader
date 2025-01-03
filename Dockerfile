FROM ubuntu:latest

RUN apt-get update && \
  apt-get install -y \
  build-essential \
  cmake \
  python3-dev \
  python3-pip \
  python3-venv \
  wget

# See https://arrow.apache.org/install/
RUN apt install -y -V ca-certificates lsb-release wget
RUN wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
RUN apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
RUN apt update && \
  apt install -y -V libarrow-dev

WORKDIR /work
ADD . /work

RUN python3 -m venv venv
RUN venv/bin/python -m pip install .

ENTRYPOINT [ "./venv/bin/python" ]
CMD [ "example.py" ]
