FROM golang:1.22 as BuildStage

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

COPY --from=BuildStage /kaf /bin/kaf

USER 1001

# Run
ENTRYPOINT ["/bin/kaf"]
