# frontend build
FROM node:22-alpine AS frontend
WORKDIR /web
COPY web/package*.json ./
RUN npm ci
COPY web ./
RUN npm run build

# backend build
FROM golang:1.24-alpine AS backend
WORKDIR /app
RUN apk add --no-cache ca-certificates
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN rm -rf internal/ui/dist && mkdir -p internal/ui/dist
COPY --from=frontend /web/dist ./internal/ui/dist
RUN CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -o /out/mq-lens ./cmd/mq-lens

# runtime
FROM gcr.io/distroless/static-debian12
WORKDIR /app
COPY --from=backend /out/mq-lens /app/mq-lens
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app/mq-lens"]
