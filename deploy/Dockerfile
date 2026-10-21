# Multi-stage production Dockerfile for GoQueue (QueueCraft)

# ================= Stage 1: Build Binaries =================
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install git and ca-certificates for fetching dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Cache go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build statically-linked, stripped production binaries
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /bin/goqueue-server ./cmd/goqueue-server
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /bin/goqueue-cli ./cmd/goqueue-cli
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /bin/goqueue-loadgen ./cmd/goqueue-loadgen

# ================= Stage 2: Final Runtime Image =================
FROM alpine:3.20 AS runner

RUN apk add --no-cache ca-certificates tzdata curl

# Non-root service user for container security
RUN addgroup -S goqueue && adduser -S goqueue -G goqueue

WORKDIR /home/goqueue

# Copy compiled binaries from builder stage
COPY --from=builder /bin/goqueue-server /usr/local/bin/goqueue-server
COPY --from=builder /bin/goqueue-cli /usr/local/bin/goqueue-cli
COPY --from=builder /bin/goqueue-loadgen /usr/local/bin/goqueue-loadgen

USER goqueue

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=3s --retries=3 \
  CMD curl -f http://localhost:8080/healthz || exit 1

ENTRYPOINT ["goqueue-server"]
