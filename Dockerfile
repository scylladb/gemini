FROM golang:1.23 AS builder

ENV GO111MODULE=on

WORKDIR /gemini

COPY . .

RUN apt-get update && apt-get install -y libc-dev build-essential \
    && make build

FROM busybox AS production

WORKDIR /gemini

COPY --from=builder /gemini/bin/gemini .

ENV PATH="/gemini:${PATH}"

ENTRYPOINT ["gemini"]
