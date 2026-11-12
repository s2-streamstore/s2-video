FROM golang:1.26.11-alpine3.24@sha256:f496f8c537ed506626ee1a6913afb32c45068a48697163c088f604cc775a0c38 AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -o server .

FROM alpine:3.24.4@sha256:0f49f331d9212aff11b932863be4eb01cdf962969aa86dab7d1ee21ac6f5f52b
RUN apk upgrade --no-cache && apk add --no-cache ca-certificates ffmpeg
COPY --from=build /app/server /server
COPY static/ /static/
WORKDIR /
ENV ADDR=:8080
EXPOSE 8080
# The server holds an S2 access token; keep it off root.
USER 65532:65532
CMD ["/server"]
