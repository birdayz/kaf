FROM golang:latest AS builder

WORKDIR /gnomock/
ADD go.mod .
ADD go.sum .
RUN go mod download
RUN go mod verify
ADD . .
RUN CGO_ENABLED=0 GOARCH=amd64 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o /gnomockd ./server

FROM scratch

COPY --from=builder /gnomockd /gnomockd
ENTRYPOINT ["/gnomockd"]
