FROM golang:1-alpine AS builder
WORKDIR /app
# Fetch dependencies first; they are less susceptible to change on every build
# and will therefore be cached for speeding up the next build.
COPY go.* .
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY . ./
# CGO_ENABLED=0 == Don't depend on libc (bigger but more independent binary)
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    env CGO_ENABLED=0 go build ./cmd/cascade-registry

FROM scratch
WORKDIR /app

COPY --from=builder /app/cascade-registry .

ENTRYPOINT ["./cascade-registry"]
