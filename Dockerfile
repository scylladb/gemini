FROM golang:1.23-bookworm AS build

ENV GO111MODULE=on
ENV CGO_ENABLED=0
ENV GOAMD64=v3
ENV GOARM64=v8.0,crypto
ENV CFLAGS="-O3"
ENV CXXFLAGS="-O3"

WORKDIR /gemini

COPY . .

RUN apt-get update \
    && apt-get install -y build-essential ca-certificates \
    && make build

FROM busybox AS production

WORKDIR /gemini

COPY --from=build /gemini/bin/gemini .

ENV PATH="/gemini:${PATH}"

ENTRYPOINT ["gemini"]


FROM busybox AS production-goreleaser

WORKDIR /gemini

COPY gemini .

ENV PATH="/gemini:${PATH}"

ENTRYPOINT ["gemini"]
