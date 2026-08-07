# Build context must be backend/ (the module root), e.g.:
#   docker build -f backend/cmd/backend/Dockerfile -t sage-backend backend/

FROM golang:1.26-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/backend ./cmd/backend

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/backend /sage-backend
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/sage-backend"]
