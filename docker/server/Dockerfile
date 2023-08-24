FROM    debian:bookworm-slim

ENV     DEBIAN_FRONTEND noninteractive
ARG	GITREPO=conversence/hyperknowledge
ARG	GITBRANCH=main

RUN apt-get update && apt-get install -y \
            apt-utils \
            apt-transport-https \
            ca-certificates \
            locales \
            python3 \
            python3-pip \
            python3-virtualenv \
            python3-setuptools \
            git \
            postgresql-client \
            net-tools && \
            apt-get upgrade
WORKDIR /opt
RUN     cd /opt ; set -x ; git clone -b $GITBRANCH https://github.com/$GITREPO.git
WORKDIR /opt/hyperknowledge
ENV LC_ALL="C.UTF-8"
ENV LC_CTYPE="C.UTF-8"
ENV PRODUCTION=1
RUN python3 -mvirtualenv venv && \
    . venv/bin/activate && \
    pip install --upgrade pip && \
    pip install -e .
CMD cd /opt/hyperknowledge && . venv/bin/activate && \
    python scripts/initial_setup.py --app_name HyperKnowledge --host postgres -p postgres --no-create-development --no-create-test --store-password && \
    python scripts/db_updater.py -d production init && \
    python scripts/db_updater.py -d production deploy && \
    uvicorn --host 0.0.0.0 hyperknowledge.eventdb.server:app
