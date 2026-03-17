# s2-video

A video conferencing demo app built on [S2](https://s2.dev) streams. 

## Requirements

- Go 1.22+
- An S2 account and a basin
- ffmpeg (for MP4 export)

## Running locally

```sh
export S2_ACCESS_TOKEN=<your token>
export S2_BASIN=<your basin>
go run .
```

Then open `http://localhost:8080`.

## Deploying to Fly.io

```sh
fly deploy
fly secrets set S2_ACCESS_TOKEN=<your token> S2_BASIN=<your basin>
```
