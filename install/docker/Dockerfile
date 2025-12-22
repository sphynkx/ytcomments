FROM fedora:39

COPY install/docker/mongodb.repo /etc/yum.repos.d/mongodb.repo

RUN dnf install -y mongodb-org && \
dnf install -y https://github.com/mongodb-js/mongosh/releases/download/v2.5.2/mongodb-mongosh-shared-openssl11-2.5.2.x86_64.rpm && \
    dnf install -y python3 python3-pip git nc psmisc procps-ng mc net-tools && \
    dnf clean all


WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r install/requirements.txt

RUN mkdir -p /data/db

RUN sed -i 's/\r$//' install/docker/replica-init.sh && chmod +x install/docker/replica-init.sh

EXPOSE 27017
EXPOSE 50051

CMD sh -c "bash install/docker/replica-init.sh && exec python3 main.py"
