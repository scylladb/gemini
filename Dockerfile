FROM golang:1.23-bookworm AS build
ENV GO111MODULE=on
ENV GOAMD64=v3
ENV GOARM64=v8.3,crypto
ENV CFLAGS="-O3"
ENV CXXFLAGS="-O3"

WORKDIR /gemini

COPY . .

RUN apt-get update \
    && apt-get upgrade -y  \
    && apt-get install -y build-essential ca-certificates libc-dev \
    && make build

FROM busybox AS production

WORKDIR /

COPY --from=build /gemini/bin/gemini /usr/local/bin/gemini

ENV PATH="/usr/local/bin:${PATH}"

ENTRYPOINT ["gemini"]

FROM busybox AS  production-goreleaser

WORKDIR /

COPY gemini /usr/local/bin/gemini

ENV PATH="/usr/local/bin:${PATH}"

ENTRYPOINT ["gemini"]
