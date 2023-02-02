FROM golang:1.20rc3-alpine3.17

WORKDIR /job

RUN apk add --update

COPY ./job .

RUN go run main.go

CMD ["echo", "hello"]