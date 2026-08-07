# Build context must be backend/ (the module root), e.g.:
#   docker build -f backend/cmd/mcp-execute-server/Dockerfile -t sage-mcp-execute-server backend/

FROM golang:1.26-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/mcp-execute-server ./cmd/mcp-execute-server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/mcp-execute-server /sage-mcp-execute-server
EXPOSE 8090
USER nonroot:nonroot
ENTRYPOINT ["/sage-mcp-execute-server"]
