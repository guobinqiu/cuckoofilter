FROM golang:1.16
ENV GO111MODULE=on
ENV GOPROXY=https://goproxy.cn
ADD . /go/src/cuckoofilter/
WORKDIR /go/src/cuckoofilter/
RUN go build cuckoofilter_server/main.go
CMD ["/bin/sh", "-c", "./main"]