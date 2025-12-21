FROM fedora:38

COPY install/docker/mongodb.repo /etc/yum.repos.d/mongodb.repo

RUN rpm --import https://www.mongodb.org/static/pgp/server-7.0.asc && \
    dnf install -y mongodb-org && \
    dnf install -y python3 python3-pip git nc psmisc mc && \
    dnf clean all


WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r install/requirements.txt

RUN mkdir -p /data/db

RUN sed -i 's/\r$//' install/docker/replica-init.sh && chmod +x install/docker/replica-init.sh

EXPOSE 27017
EXPOSE 50051

##CMD sh -c "mongod --bind_ip_all --replSet ytcommentsReplicaSet --dbpath /data/db --noauth & while ! nc -z 127.0.0.1 27017; do sleep 1; done && bash install/docker/replica-init.sh && PID=\$(pgrep mongod) && kill \$PID && exec mongod --bind_ip_all --replSet ytcommentsReplicaSet --dbpath /data/db --auth"
CMD ["sh"]

