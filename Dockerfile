FROM golang:1.24.2@sha256:d9db32125db0c3a680cfb7a1afcaefb89c898a075ec148fdc2f0f646cc2ed509 AS buildstage

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
