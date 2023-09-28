FROM ubuntu:22.04

ENV LANG en_US.utf8

ARG spark_uid=185

RUN groupadd --system --gid=${spark_uid} spark && \
    useradd --system --uid=${spark_uid} --gid=spark spark


RUN sed -i 's#http://archive.ubuntu.com#http://mirrors.aliyun.com#g' /etc/apt/sources.list && \
    apt-get update && \
    apt install -y gnupg2 wget bash tini libc6 libpam-modules krb5-user libnss3 procps net-tools gosu && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd && \
    rm -rf /var/cache/apt/* && \
    rm -rf /var/lib/apt/lists/*

COPY operator-bin /usr/bin/
COPY entrypoints/spark-operator-entrypoint.sh /usr/bin/entrypoint.sh
RUN chmod a+x /usr/bin/operator-bin && \
    chmod a+x /usr/bin/entrypoint.sh
ENTRYPOINT ["/usr/bin/entrypoint.sh"]