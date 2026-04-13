FROM golang:1.26.2@sha256:fcdb3e42c5544e9682a635771eac76a698b66de79b1b50ec5b9ce5c5f14ad775 AS buildstage

# Set destination for COPY
WORKDIR /app

# Download Go modules
COPY go.mod go.sum ./
RUN go mod download
COPY . ./

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-w -s" -o /kaf ./cmd/kaf 

FROM scratch

WORKDIR /

COPY --from=buildstage /kaf /bin/kaf

USER 1001

# Run
ENTRYPOINT ["/bin/kaf"]
