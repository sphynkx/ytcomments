New generation of ytcomments service. Previous version (mongodb based) - see in the [separate repo](https://github.com/sphynkx/ytcomments_mongodb) (deprecated).


## Install DB
```bash
wget https://packages.couchbase.com/releases/7.6.1/couchbase-server-community-7.6.1-linux.x86_64.rpm
dnf -y install couchbase-server-community-7.6.1-linux.x86_64.rpm
```
Open http://localhost:8091 and create new bucket (`ytcomments`), create admin user, set all other options.
You may also try to test request (in "Query"):
```sql
SELECT c.*
FROM `ytcomments`.`_default`.`_default` c
WHERE c.type="comment" AND c.thread_id="thread::demo"
ORDER BY c.created_at;
```
Result will empty for now..


## Install service
__NOTE__: `couchbase` python module supports python 3.10 or lower.. Either need to build wheel manually.. So use `uv`..
```bash
dnf -y install uv grpcurl
cd /opt
git clone https://github.com/sphynkx/ytcomments
cd ytcomments
uv venv --python 3.10 .venv
source .venv/bin/activate
uv pip install -r install/requirements.txt
```

Optionally set in `.env`:
```conf
CB_CONNSTR="couchbase://127.0.0.1"
CB_USERNAME="admin"
CB_PASSWORD="SECRET"
CB_BUCKET="comments"
```
Run:
```bash
uvicorn main:app --reload --port 8800
```
Check health: http://localhost:8800/api/health

Check via reflections:
```bash
grpcurl -plaintext 127.0.0.1:9093 list
grpcurl -plaintext 127.0.0.1:9093 describe ytcomments.v1.YtComments
```

Use: http://localhost:8800/ Add some branch of comments.. In DB console repeat Query (as above)..


## Run as systemd service
```bash
cp install/ytcomments.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now ytcomments.service
```
