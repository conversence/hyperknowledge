FROM postgres:16.2-alpine as build
RUN apk add curl tar make bash clang15 llvm15 musl-dev && \
  curl -L  https://github.com/michelp/pgjwt/tarball/master -o pgjwt.tgz && \
  tar zxf pgjwt.tgz && \
  mv michelp-pgjwt-* pgjwt && \
  cd pgjwt && \
  make && \
  cd .. && \
  curl -L https://github.com/fboulnois/pg_uuidv7/tarball/master -o pg_uuidv7.tgz && \
  tar zxf pg_uuidv7.tgz && \
  mv fboulnois-pg_uuidv7-* uuidv7 && \
  cd uuidv7 && \
  make && \
  /usr/lib/llvm15/bin/llvm-lto -thinlto -thinlto-action=thinlink -o pg_uuidv7.index.bc pg_uuidv7.bc

FROM postgres:16.2-alpine
COPY docker-healthcheck /usr/local/bin/
COPY --from=build /pgjwt /pgjwt
COPY --from=build /uuidv7 /uuidv7
RUN /usr/local/lib/postgresql/pgxs/config/install-sh -c -d '/usr/local/share/postgresql/extension' && \
  /usr/local/lib/postgresql/pgxs/config/install-sh -c -d '/usr/local/lib/postgresql' && \
  /usr/local/lib/postgresql/pgxs/config/install-sh -c -d '/usr/local/lib/postgresql/bitcode' && \
  cd /pgjwt && \
  /usr/bin/install -c -m 644 ./pgjwt.control ./pgjwt--*.sql '/usr/local/share/postgresql/extension/' && \
  cd /uuidv7 && \
  /usr/bin/install -c -m 644 ./pg_uuidv7.index.bc '/usr/local/lib/postgresql/bitcode' && \
  /usr/bin/install -c -m 644 ./pg_uuidv7.control ./sql/pg_uuidv7--*.sql '/usr/local/share/postgresql/extension/' && \
  /usr/bin/install -c -m 755 ./pg_uuidv7.so '/usr/local/lib/postgresql/' && \
  cd / && rm -rf /pgjwt /uuidv7
  HEALTHCHECK CMD ["docker-healthcheck"]
