FROM golang:1.24-bookworm AS build

ENV GO111MODULE=on
ENV GOAMD64=v3
ENV GOARM64=v8.3,crypto
ENV CFLAGS="-O3"
ENV CXXFLAGS="-O3"
ENV DEBIAN_FRONTEND="noninteractive"
ENV TZ="UTC"

WORKDIR /gemini

COPY . .

RUN apt-get update \
    && apt-get upgrade -y  \
    && apt-get install -y build-essential ca-certificates libc-dev \
    && make build

FROM debian:12-slim AS base-production

WORKDIR /

ENV DEBIAN_FRONTEND="noninteractive"
ENV GODEBUG="default=go1.24,netdns=go,gctrace=0,cgocheck=0,disablethp=0,panicnil=0,http2client=1,http2server=1,asynctimerchan=0,madvdontneed=1"
ENV PATH="/usr/local/bin:${PATH}"

RUN apt-get update && apt-get upgrade -y \
	&& apt-get install -y ca-certificates \
    && apt-get autoremove -y \
    && apt-get clean \
	&& rm -rf /var/lib/apt/lists/*

EXPOSE 6060
EXPOSE 2112

ENTRYPOINT ["gemini"]

FROM base-production AS production

COPY --from=build /gemini/bin/gemini /usr/local/bin/gemini
COPY --from=build /gemini/version.json /version.json

FROM base-production AS production-goreleaser

COPY gemini /usr/local/bin/gemini

RUN gemini --version

FROM build AS debug

ENV GODEBUG="default=go1.24,gctrace=1,cgocheck=1,disablethp=0,panicnil=0,http2client=1,http2server=1,asynctimerchan=0,madvdontneed=0"
ENV PATH="/gemini/bin:${PATH}"

RUN apt-get install -y gdb gcc iputils-ping mlocate vim \
    && make debug-build \
    && go install github.com/go-delve/delve/cmd/dlv@latest \
    && updatedb

EXPOSE 6060
EXPOSE 2121
EXPOSE 2345

ENTRYPOINT [ \
    "dlv", "exec", "--log", "--listen=0.0.0.0:2345", "--allow-non-terminal-interactive", \
    "--headless", "--api-version=2", "--accept-multiclient", \
    "/gemini/bin/gemini", "--" \
    ]
