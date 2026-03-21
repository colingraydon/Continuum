FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o continuum ./cmd/continuum

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/continuum .
EXPOSE 8080
CMD ["./continuum"]