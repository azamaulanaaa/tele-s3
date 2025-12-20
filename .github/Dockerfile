FROM gcr.io/distroless/cc-debian12

ARG BINARY_NAME

COPY ${BINARY_NAME} /usr/local/bin/app

ENTRYPOINT ["/usr/local/bin/app"]
