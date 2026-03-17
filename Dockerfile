FROM golang:1.25-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o server .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates ffmpeg
COPY --from=build /app/server /server
COPY static/ /static/
WORKDIR /
ENV ADDR=:8080
EXPOSE 8080
CMD ["/server"]
