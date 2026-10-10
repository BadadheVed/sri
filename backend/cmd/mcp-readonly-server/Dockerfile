# backend/cmd/mcp-readonly-server/Dockerfile
# Build context must be backend/ (the module root), e.g.:
#   docker build -f backend/cmd/mcp-readonly-server/Dockerfile -t sage-mcp-readonly-server backend/

FROM golang:1.26-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/mcp-readonly-server ./cmd/mcp-readonly-server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/mcp-readonly-server /sage-mcp-readonly-server
EXPOSE 8091
USER nonroot:nonroot
ENTRYPOINT ["/sage-mcp-readonly-server"]
