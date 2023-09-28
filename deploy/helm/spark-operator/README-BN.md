# bytenative spark-operator

A Helm chart for Bytenative's Spark on Kubernetes operator

## Introduction

This operator was based on google's [Kubernetes Operator for Apache Spark](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator), added helm configurations to deploy an extra bn-spark-operator. 

This chart was from spark-on-k8s-operator `v1.27.1`` with helm package version `v1beta2-1.3.8-3.1.1`, and with new configurations Bytenative operator. It will bootstraps both spark-on-k8s-operator and bytenative deployments using the [Helm](https://helm.sh) package manager.

## Prerequisites

- Helm >= 3
- Kubernetes >= 1.16

## Previous Helm Chart

- deploy/helm/spark-operator-1.1.27.tgz