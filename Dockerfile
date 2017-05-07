FROM golang:1.8.1
COPY ./vendor /go/src/
RUN go get github.com/onsi/ginkgo
RUN go get github.com/onsi/gomega
WORKDIR /go/src/github.com/sendgrid/go-solr