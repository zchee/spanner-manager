# syntax=docker.io/docker/dockerfile-upstream:master-labs
# check=error=true

FROM golang:1.26-alpine AS build

ENV GOEXPERIMENT=runtimefreegc,sizespecializedmalloc,goroutineleakprofile,runtimesecret

RUN apk add --no-cache \
    curl \
    git

WORKDIR /go/src/github.com/zchee/spanner-manager

COPY --link go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY --link . .

RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -o /out/spanner-manager .

FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.source=https://github.com/zchee/spanner-manager

COPY --link --from=build /out/spanner-manager /usr/local/bin/spanner-manager

ENTRYPOINT ["spanner-manager"]
