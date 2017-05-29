FROM ubuntu:trusty
MAINTAINER Minq <minq1205@gmail.com>

# Update package info
RUN apt-get update -y

# System packages required
RUN apt-get install -y libmemcached-dev python-pip build-essential libpq-dev python-dev libtiff5-dev libjpeg8-dev zlib1g-dev libfreetype6-dev liblcms2-dev libwebp-dev tcl8.6-dev tk8.6-dev python-tk libffi-dev openssh-client

# Install tox
RUN pip install -U tox

# Numpy preinstall
RUN pip install numpy==1.12.0
