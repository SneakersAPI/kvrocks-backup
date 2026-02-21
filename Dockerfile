FROM golang:1.26-alpine 

WORKDIR /app

COPY . .

RUN go build -o /bin/kvrocks-backup .

ENTRYPOINT ["kvrocks-backup"]