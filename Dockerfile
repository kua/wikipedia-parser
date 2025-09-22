# syntax=docker/dockerfile:1

FROM golang:1.23 AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /wikiparser ./cmd/wikiparser

FROM gcr.io/distroless/base-debian12:nonroot
WORKDIR /app
COPY --from=build /wikiparser /app/wikiparser
COPY config.docker.yml /app/config.yml
VOLUME ["/data"]
EXPOSE 8081
ENTRYPOINT ["/app/wikiparser"]
CMD ["-config", "/app/config.yml"]
