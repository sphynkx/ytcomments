FROM fedora:40

COPY install/docker/mongodb.repo /etc/yum.repos.d/mongodb.repo

RUN rpm --import https://www.mongodb.org/static/pgp/server-7.0.asc && \
    dnf install -y mongodb-org && \
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
