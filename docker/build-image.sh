#!/usr/bin/env bash

REGISTRY="bnp.me/"

docker build -f Dockerfile-spark-${VERSION} -t ${REGISTRY}bn-spark-operator/spark:v${VERSION} .  && \
    docker build -f spark-operator/Dockerfile-sko-spark-template -t ${REGISTRY}bn-spark-operator/spark-operator:v1beta2-1.3.8-${VERSION} --build-arg VERSION=${VERSION} --build-arg REGISTRY=${REGISTRY} . && \
    docker push ${REGISTRY}bn-spark-operator/spark:v${VERSION} && \
    docker push ${REGISTRY}bn-spark-operator/spark-operator:v1beta2-1.3.8-${VERSION}

# build bytenative spark operator
# docker build -f Dockerfile -t ${REGISTRY}bn-spark-operator/bn-spark-operator:v1-0.1.0-${VERSION} .