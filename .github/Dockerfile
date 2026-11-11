FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f

ARG BINARY_NAME

WORKDIR /data

COPY --chown=nonroot:nonroot ${BINARY_NAME} /usr/local/bin/tele-s3

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/tele-s3"]
