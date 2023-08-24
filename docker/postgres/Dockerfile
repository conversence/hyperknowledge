FROM postgres:15.4-alpine
COPY docker-healthcheck /usr/local/bin/
RUN apk add curl tar make bash && \
  curl -L  https://github.com/michelp/pgjwt/tarball/master -o pgjwt.tgz && \
  tar zxf pgjwt.tgz && \
  cd michelp-pgjwt-* && \
  make install && \
  cd .. && \
  rm -rf michelp-pgjwt-* && \
  curl -L -O https://github.com/fboulnois/pg_uuidv7/releases/download/v1.1.1/pg_uuidv7.tar.gz && \
  tar zxf pg_uuidv7.tar.gz && \
  mv pg_uuidv7.so `pg_config --pkglibdir` && \
  mv pg_uuidv7*.sql pg_uuidv7.control `pg_config --sharedir`/extension && \
  rm pg_uuidv7.tar.gz
HEALTHCHECK CMD ["docker-healthcheck"]
