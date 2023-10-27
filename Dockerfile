FROM ubuntu:18.04 as base

ARG USERNAME=java-driver-user
ARG USER_UID=1000
ARG USER_GID=$USER_UID

USER root
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

RUN apt-get update && apt-get install git python3.8 python3-pip maven openjdk-8-jdk -y

ARG github_access_token

RUN git clone https://${github_access_token}@github.com/riptano/ccm-private.git /ccm-private

WORKDIR /ccm-private
RUN pip3 install -r ./requirements.txt

RUN python3 ./setup.py install
RUN update-alternatives --set java /usr/lib/jvm/java-8-openjdk-arm64/jre/bin/java

USER $USERNAME
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-arm64/
RUN export JAVA_HOME

RUN mkdir /home/$USERNAME/java-driver

RUN git clone https://${github_access_token}@github.com/hhughes/java-driver.git /home/$USERNAME/java-driver

WORKDIR /home/$USERNAME/java-driver
RUN mvn dependency:go-offline

WORKDIR /home/$USERNAME
RUN rm -rf /home/$USERNAME/java-driver

from base as build

ARG build_id=0

RUN echo "Build ID: ${build_id}"

RUN mkdir /home/$USERNAME/java-driver

WORKDIR /home/$USERNAME/java-driver
COPY ./. /home/$USERNAME/java-driver/

USER root
RUN chown -R $USERNAME:$USERNAME /home/$USERNAME
USER $USERNAME
