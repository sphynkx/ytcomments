FROM fedora:40

RUN dnf -y update && \
    dnf -y install python3 python3-pip git mongodb-org && \
    dnf -y clean all

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r install/requirements.txt

# Data dir for MongoDB
RUN mkdir -p /data/db
VOLUME /data/db

RUN chmod +x install/docker/replica-init.sh

EXPOSE 27017 50051

CMD sh -c "mongod --bind_ip_all --replSet ytcommentsReplicaSet --dbpath /data/db & ./run.sh"