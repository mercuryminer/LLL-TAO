#
# Build Nexus/LISP docker image based on a Debian Linux kernel. This
# Dockerfile is for the Tritium release of the TAO. The docker image name
# should be called "tritium". To build, use the "docker build" command in
# the LLL-TAO git repo directory:
#
#     cd LLL-TAO
#     docker build -t tritium .
#
# If you don't have docker on your MacOS system, download it at:
#
#     https://download.docker.com/mac/stable/Docker.dmg
#
# Once you install it you can run docker commands in a Terminal window.
#
# For instructions on how to use this Dockerfile, see ./docs/how-to-docker.md.
#

#
# We prefer Debian as a base OS. (Ubuntu 18.04 is more suitable for ssl version)
#
FROM ubuntu:18.04

#
# Install Nexus dependencies.
#
RUN apt-get update && apt-get -yq install \
    git build-essential libboost-all-dev libssl-dev libminiupnpc-dev p7zip-full libdb-dev libdb++-dev

#
# Put user in the /nexus
#
EXPOSE 8080
WORKDIR /nexus

#
# Put Nexus source-tree in docker image and build it..
#
RUN mkdir /nexus
RUN mkdir /nexus/build
COPY ./makefile.cli /nexus
COPY ./src /nexus/src/

ENV NEXUS_DEBUG 0
RUN cd /nexus; make -j 8 -f makefile.cli ENABLE_DEBUG=$NEXUS_DEBUG

#
# Copy Nexus startup files.
#
COPY config/run-nexus /nexus/run-nexus
COPY config/nexus.conf /nexus/nexus.conf.default
COPY config/curl-nexus /nexus/curl-nexus
COPY config/nexus-save-data /nexus/nexus-save-data

#
# Startup nexus.
#
ENV RUN_NEXUS   /nexus/run-nexus



    
