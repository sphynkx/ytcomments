FROM fedora:40

RUN tee /etc/yum.repos.d/mongodb.repo << "EOF" > /dev/null \
    [mongodb-org-7.0] \
    name=MongoDB Repository \
    baseurl=https://repo.mongodb.org/yum/redhat/9/mongodb-org/7.0/x86_64/ \
    gpgcheck=1 \
    enabled=1 \
    gpgkey=https://www.mongodb.org/static/pgp/server-7.0.asc \
EOF && \
    rpm --import https://www.mongodb.org/static/pgp/server-7.0.asc && \
    dnf install -y mongodb-org && \
    dnf install -y python3 python3-pip git && \
    dnf clean all

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir -r install/requirements.txt

# Data dir for MongoDB
RUN mkdir -p /data/db
VOLUME /data/db

RUN chmod +x install/docker/replica-init.sh

EXPOSE 27017 50051

CMD sh -c "mongod --bind_ip_all --replSet ytcommentsReplicaSet --dbpath /data/db & ./run.sh"