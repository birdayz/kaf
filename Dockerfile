FROM golang:1.25.3@sha256:6ea52a02734dd15e943286b048278da1e04eca196a564578d718c7720433dbbe AS buildstage

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
