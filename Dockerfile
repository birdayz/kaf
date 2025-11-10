FROM golang:1.25.4@sha256:6ca9eb0b32a4bd4e8c98a4a2edf2d7c96f3ea6db6eb4fc254eef6c067cf73bb4 AS buildstage

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
