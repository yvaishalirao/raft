FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/raftnode ./cmd/node

FROM alpine:3.20
RUN apk add --no-cache netcat-openbsd
COPY --from=build /out/raftnode /usr/local/bin/raftnode
EXPOSE 7000 8000
ENTRYPOINT ["raftnode"]
