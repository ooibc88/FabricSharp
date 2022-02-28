# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

ARG GO_VER
ARG ALPINE_VER

FROM alpine:${ALPINE_VER} as peer-base
RUN apk add --no-cache tzdata
# set up nsswitch.conf for Go's "netgo" implementation
# - https://github.com/golang/go/blob/go1.9.1/src/net/conf.go#L194-L275
# - docker run --rm debian:stretch grep '^hosts:' /etc/nsswitch.conf
RUN echo 'hosts: files dns' > /etc/nsswitch.conf

FROM golang:${GO_VER}-alpine${ALPINE_VER} as golang
RUN apk add --no-cache \
	bash \
	gcc \
	g++ \
	git \
	make \
	boost-dev \
	gflags-dev \
	musl-dev

RUN echo 'http://nl.alpinelinux.org/alpine/edge/testing' >> /etc/apk/repositories
RUN echo 'http://nl.alpinelinux.org/alpine/v3.10/main' >> /etc/apk/repositories
RUN apk update

RUN apk add --no-cache \ 
#   crypto++-dev=8.6.0-r0 \
  protobuf-dev=3.6.1-r1 \
  czmq-dev=4.1.1-r2

ADD . $GOPATH/src/github.com/hyperledger/fabric
WORKDIR $GOPATH/src/github.com/hyperledger/fabric

ENV USTORE_HOME=$GOPATH/src/github.com/hyperledger/fabric/ustore_home
ENV LD_LIBRARY_PATH=${USTORE_HOME}/lib:${LD_LIBRARY_PATH}
ENV LIBRARY_PATH=${USTORE_HOME}/lib:${LIBRARY_PATH}
ENV CPLUS_INCLUDE_PATH=${USTORE_HOME}/include:/usr/local/include/:${CPLUS_INCLUDE_PATH}

FROM golang as peer
ARG GO_TAGS
RUN make peer GO_TAGS=${GO_TAGS}

FROM peer-base

RUN apk add --no-cache \
	boost-dev \
	gflags-dev \
	musl-dev

RUN echo 'http://nl.alpinelinux.org/alpine/edge/testing' >> /etc/apk/repositories
RUN echo 'http://nl.alpinelinux.org/alpine/v3.10/main' >> /etc/apk/repositories
RUN apk update

RUN apk add --no-cache \ 
#   crypto++-dev=8.6.0-r0 \
  protobuf-dev=3.6.1-r1 \
  czmq-dev=4.1.1-r2

ENV FABRIC_CFG_PATH /etc/hyperledger/fabric
VOLUME /etc/hyperledger/fabric
VOLUME /var/hyperledger

COPY --from=peer /go/src/github.com/hyperledger/fabric/ustore_home /usr/ustore_home
COPY --from=peer /go/src/github.com/hyperledger/fabric/build/bin /usr/local/bin
COPY --from=peer /go/src/github.com/hyperledger/fabric/sampleconfig/msp ${FABRIC_CFG_PATH}/msp
COPY --from=peer /go/src/github.com/hyperledger/fabric/sampleconfig/core.yaml ${FABRIC_CFG_PATH}

ENV USTORE_HOME=/usr/ustore_home
ENV LD_LIBRARY_PATH=${USTORE_HOME}/lib:${LD_LIBRARY_PATH}

RUN ["mkdir", "-p", "/ustore_data"]

EXPOSE 7051
CMD ["peer","node","start"]
