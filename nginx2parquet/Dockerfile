FROM ubuntu:trusty

ARG TERM=linux
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update                                          \
    && apt-get install --assume-yes --no-install-recommends \
      bison                                                 \
      ca-certificates                                       \
      cmake                                                 \
      flex                                                  \
      g++                                                   \
      git                                                   \
      libboost-dev                                          \
      libboost-filesystem-dev                               \
      libboost-program-options-dev                          \
      libboost-regex-dev                                    \
      libboost-system-dev                                   \
      libboost-test-dev                                     \
      libpython2.7-dev                                      \
      libssl-dev                                            \
      libtool                                               \
      make                                                  \
      pkg-config                                            \
      python-pip                                            \
      && rm -rf /var/lib/apt/lists/*
RUN pip install -U pip
RUN pip install \
      cython    \
      pandas	\
      pyarrow	\
      docopt

WORKDIR /root

COPY ng2pq.py .
RUN python ng2pq.py -h
