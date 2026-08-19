FROM golang:1.26-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGET
RUN test -n "$TARGET"
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/app ./$TARGET

FROM alpine:3.22

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=build /out/app /app/app

ENTRYPOINT ["/app/app"]