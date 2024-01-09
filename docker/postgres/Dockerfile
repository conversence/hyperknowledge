FROM postgres:16.1-alpine as build
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

FROM postgres:16.1-alpine
COPY docker-healthcheck /usr/local/bin/
COPY --from=build /pgjwt /pgjwt
COPY --from=build /uuidv7 /uuidv7
RUN apk add make && \
  cd /pgjwt && \
  make install && \
  cd /uuidv7 && \
  mkdir -p /usr/local/lib/postgresql/bitcode && \
  /usr/bin/install -c -m 644 pg_uuidv7.index.bc /usr/local/lib/postgresql/bitcode && \
  make -k install || true
HEALTHCHECK CMD ["docker-healthcheck"]
