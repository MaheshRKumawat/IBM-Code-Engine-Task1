FROM golang:1.20rc3-alpine3.17

WORKDIR /job

RUN apk add --update

COPY . .

RUN go build -o /main-file

CMD ["/main-file"]