#!/usr/bin/env bash

# Rust build from local
# curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# source "$HOME/.cargo/env"
# cargo build --bins -r
# copy the bin for docker image building
# cp ./target/release/operator-bin ./docker

# with k3s & disabled traefik due to resource limitation
# curl -sfL https://get.k3s.io | sh -s - --write-kubeconfig-mode 644 --disable traefik
# mkdir ~/.kube
# ln -s /etc/rancher/k3s/k3s.yaml ~/.kube/config

# with minikube default
minikube start --cpus=4 --memory=12gb
# NOTE: reuse the Docker daemon inside minikube cluster for build images into minikube
eval $(minikube docker-env)

# jars needed with Spark Dockerfiles
curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar --output ./docker/jars/aws-java-sdk-bundle-1.11.1026.jar
curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar --output ./docker/jars/hadoop-aws-3.3.4.jar
curl https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.11.0/jmx_prometheus_javaagent-0.11.0.jar --output ./docker/jars/jmx_prometheus_javaagent-0.11.0.jar
# build 3 docker images
docker build -f ./docker/Dockerfile-spark-3.4.1 -t bnp.me/bn-spark-operator/spark:v3.4.1 ./docker
docker build -f ./docker/spark-operator/Dockerfile-sko-spark-template -t bnp.me/bn-spark-operator/spark-operator:v1-0.1.0-3.4.1 --build-arg VERSION=3.4.1 --build-arg REGISTRY=bnp.me/ ./docker
docker build -f ./docker/Dockerfile-with-builder -t bnp.me/bn-spark-operator/bn-spark-operator:v1-0.1.0-3.4.1 ./docker

# install operators with helm
# create a namespace for spark operators
kubectl create namespace spark-operator
# create a namespace for your spark jobs, or use the same nameapsce with spark operators as default.
kubectl create namespace sparkjobs
# create an s3 secret for spark-operator
kubectl create secret generic s3-connection --from-literal=accessKey=minio --from-literal=secretKey=miniopass -n spark-operator
helm upgrade spark-runner deploy/helm/spark-operator -i --namespace spark-operator --create-namespace --set logLevel=3 --set sparkJobNamespace=sparkjobs

# RUN Spark-PI example
kubectl apply -f examples/pi-job-example.yaml

# Set up template and resources
# kubectl apply -f examples/spark-template-341.yaml

# Should setup Minio as S3 storage first
# and upload jars, csv and sql files
# Run Sql job example
# kubectl apply -f examples/job-spark-sqlfile-341-example.yaml

# Start up Session example
# kubectl apply -f examples/session-spark-sql-341-example.yaml


# Stop operators
# helm uninstall spark-runner --namespace spark-operator