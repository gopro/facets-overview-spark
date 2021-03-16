FROM ubuntu:18.04
WORKDIR /
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install --assume-yes  \
            software-properties-common \
            wget \
            maven

# Install OpenJDK-8
RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add -
RUN add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ && \
    apt-get update && \
    apt-get install -y adoptopenjdk-8-hotspot && \
    apt-get clean

# Setup JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/adoptopenjdk-8-hotspot-amd64/
RUN export JAVA_HOME

WORKDIR /
