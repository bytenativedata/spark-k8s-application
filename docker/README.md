### Bytenative Operator Image
- Dockerfile

```SH
docker build -f Dockerfile -t bnp.me/bn-spark-operator/bn-spark-operator:v1-0.1.0 .
```

### Spark Images
- Dockerfile-3.4.1
- Dockerfile-3.3.3
- Dockerfile-3.2.4
- Dockerfile-3.2.4-hadoop3
- Dockerfile-3.1.3
- Dockerfile-3.1.1
- Dockerfile-3.1.1-hadoop3

```SH
docker build -f Dockerfile-spark-3.4.1 -t bnp.me/bn-spark-operator/spark:v3.4.1 .
```

### Google's Spark Operator Images 
These dockerfiles were from and updated base on [spark-operator:v1beta2-1.3.8-3.1.1](ghcr.io/googlecloudplatform/spark-operator).

- spark-operator/Dockerfile-sko-spark-template

```SH
docker build -f spark-operator/Dockerfile-sko-spark-template -t bnp.me/bn-spark-operator/spark-operator:v1-0.1.0-3.4.1 --build-arg VERSION=3.4.1 --build-arg REGISTRY=bnp.me/ .
```

### With images on hub.docker.com
- beking_cn/bn-spark-operator:v1-0.1.0
- beking_cn/spark:v3.4.1
- beking_cn/spark-operator:v1-0.1.0-3.4.1


### Required Jars
- spark-oper-sql_3.1.1-0.1.0.jar
- aws-java-sdk-bundle-1.11.1026.jar (download from mvnrepository)
- hadoop-aws-3.3.4.jar (download from mvnrepository)
- 