# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/minikv-node ./cmd/minikv-node
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/minikv-coord ./cmd/minikv-coord
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/loadgen ./cmd/loadgen

# Final stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates curl

RUN addgroup -g 10001 minikv && \
    adduser -u 10001 -G minikv -D -s /bin/sh minikv

COPY --from=builder /out/minikv-node /usr/local/bin/minikv-node
COPY --from=builder /out/minikv-coord /usr/local/bin/minikv-coord
COPY --from=builder /out/loadgen /usr/local/bin/loadgen

RUN mkdir -p /data && chown -R minikv:minikv /data

USER minikv
WORKDIR /home/minikv

EXPOSE 7000 7001 8080

CMD ["minikv-node", "--help"]
