
#--------------------------------
# Stage 1 - Builder
#--------------------------------
FROM golang:1.15 AS builder

WORKDIR /app

# Get dependencies
COPY go.mod .
COPY go.sum .
RUN go mod download 

# Copy the code
COPY *.go ./
COPY kvrpc ./kvrpc
COPY pb ./pb

# Build the app
RUN CGO_ENABLED=0 go build -ldflags '-w -extldflags "-static"' -o ./build/kvrpc .

#--------------------------------
# Stage 2 - Deployment container
#--------------------------------
FROM scratch

# Copy the compiled app
COPY --from=builder /app/build/kvrpc /kvrpc

EXPOSE 9000

ENTRYPOINT ["/kvrpc"]