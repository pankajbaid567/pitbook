# Multi-stage build: static Go binary, non-root runtime.
FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/pitbook ./cmd/pitbook
RUN mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/pitbook /pitbook
COPY --from=build --chown=nonroot:nonroot /data /data
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["/pitbook"]
# Demo defaults; override with `docker run ... /pitbook --wal sync --data /data ...`.
# Segments are small and snapshots frequent so the object-store mirror stays cheap.
CMD ["--demo", "--data", "/data", "--snapshot-every", "5000", "--segment-bytes", "262144", "--bot-rate", "10"]
