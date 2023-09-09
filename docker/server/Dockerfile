FROM    debian:bookworm-slim

ENV     DEBIAN_FRONTEND noninteractive
ARG	GITREPO=conversence/hyperknowledge
ARG	GITBRANCH=main

RUN apt-get update && apt-get install -y \
            apt-utils \
            apt-transport-https \
            ca-certificates \
            python3.11 \
            python3.11-venv \
            postgresql-client && \
            apt-get upgrade
WORKDIR /opt

# copy files needed for pip-install process
COPY ./pyproject.toml /opt/hyperknowledge/pyproject.toml
COPY ./setup.cfg /opt/hyperknowledge/setup.cfg

WORKDIR /opt/hyperknowledge
ENV LC_ALL="C.UTF-8"
ENV LC_CTYPE="C.UTF-8"
ENV PRODUCTION=1
RUN python3.11 -mvenv venv && \
    . venv/bin/activate && \
    pip install --upgrade pip && \
    pip install -e .

# copy files needed for setup and running
COPY ./deploy /opt/hyperknowledge/deploy
COPY ./revert /opt/hyperknowledge/revert
COPY ./hyperknowledge /opt/hyperknowledge/hyperknowledge
COPY ./scripts /opt/hyperknowledge/scripts

# placed late, so that changed values (eg. from Tilt) don't cause earlier commands to rerun
ARG postgres_host=postgres

# CMD can't insert values directly from an ARG (run-time vs build-time), so store it in an ENV
ENV postgres_host_env=${postgres_host}

CMD . venv/bin/activate && \
    python scripts/initial_setup.py --app_name HyperKnowledge --host ${postgres_host_env} -p postgres --no-create-development --no-create-test --store-password && \
    python scripts/db_updater.py -d production init && \
    python scripts/db_updater.py -d production deploy && \
    gunicorn -b 0.0.0.0:8000 -k uvicorn.workers.UvicornWorker hyperknowledge.eventdb.server:app
