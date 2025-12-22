YTComments is a supplemental service for [YurTube engine](https://github.com/sphynkx/yurtube) to communicate with MongoDB and 
support comments functionality of main app. Service based on gRPC+protobuf.

## MongoDB install and config
```bash
dnf install mongodb-org
systemctl enable mongod.service
systemctl start mongod.service
```
and check is run successfuly:
```bash
systemctl status mongod.service
```
__Note__: Due to local hardware reasons this project was use MongoDB v.4.2 but will work with more new version also. To install old version you need to create repo-file `/etc/yum.repos.d/mongodb-org-4.2.repo `:
```conf
[mongodb-org-4.2]
name=MongoDB Repository
##baseurl=https://repo.mongodb.org/yum/redhat/$releasever/mongodb-org/4.2/x86_64/
baseurl=https://repo.mongodb.org/yum/redhat/8/mongodb-org/4.2/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://www.mongodb.org/static/pgp/server-4.2.asc
```
and install MongoDB as above.


### DB configuration
Modify `/etc/mongod.conf` - find and replace the `#security:` section with:
```conf
security:
  authorization: "disabled"
```
and restart service:
```bash
systemctl restart mongod
```
Next:
```bash
cp install/mongo_setup.js-sample install/mongo_setup.js
mkdir -p /etc/mongodb
openssl rand -base64 756 > /etc/mongodb/keyfile
chown mongod:mongod /etc/mongodb/keyfile
chmod 600 /etc/mongodb/keyfile
```

Edit `conf.py` and `install/mongo_setup.js` - set username and password for MongoDB. Apply creds to DB and restart:
```
mongo < install/mongo_setup.js
```
Dont forget to delete `install/mongo_setup.js`.

Modify `/etc/mongod.conf` again - find and replace the `security:` section with:
```conf
security:
  authorization: "enabled"
  keyFile: /etc/mongodb/keyfile
```
and restart service and check health:
```bash
systemctl restart mongod
mongosh -u yt_user -p 'SECRET' --authenticationDatabase yt_comments
```


### MongoDB tuning
Optional recommendations in case then service periodically falls down. 

Modify `/etc/mongod.conf`, set `storage` section so:
```conf
storage:
  dbPath: /var/lib/mongo
  journal:
    enabled: true
#  engine:
  wiredTiger:
    engineConfig:
      cacheSizeGB: 1.5
```
Find unit file `mongod.service` (mostly in `/usr/lib/systemd/system/`). In the `[Service]` section add:
```conf
Restart=on-failure
RestartSec=10s
StartLimitBurst=5
StartLimitIntervalSec=60s
OOMScoreAdjust=-900
MemoryAccounting=true
```
Then copy it to `/etc/systemd/system` and update systemd configuration:
```bash
cp /usr/lib/systemd/system/mongod.service /etc/systemd/system
systemctl daemon-reload
systemctl restart mongod
systemctl status mongod.service
```

## Service install, config and run
```bash
cd /opt
git clone https://github.com/sphynkx/ytcomments
cd ytcomments
python3 -m venv .venv
chmod a+x install/pipinstall.sh
install/pipinstall.sh
chmod a+x run.sh
cp install/ytcomments.service /etc/systemd/system
systemctl daemon-reload
```
Make sure that the `services/ytcomments/ytcomments.proto` is same as one for `YurTube` app. Otherwise run `gen_proto.sh` to regenerate protobuf files.


### Service configuration
```bash
cp install/.env-sample .env
```
Modify `.env` - at least set your passwords.


### Run
As systemd service:
```bash
systemctl restart mongod
systemctl status mongod.service
```
Manually:
```bash
cd /opt/ytcomments
./run.sh
```


## Docker install
```bash
git clone https://github.com/sphynkx/ytcomments
cd ytcomments
/install/docker
cp install/mongo_setup.js-sample install/mongo_setup.js
cp install/.env-sample .env
```
You need to modify some files. 

At `install/mongo_setup.js` set your passwords for __yt_user__ (must be same as for mongodb user, communicating with YurTube app) and __admin__. If you will replicate DB between different host you must set the same creds for the both users for all involved hosts.

Modify `.env` file at the root of app, set appropriate params to connect to DB.

In above section "DB configuration" there was command to create `/etc/mongodb/keyfile`. You need copy this file from `/etc/mongodb/keyfile` of server where it was created and place into your local `install/docker`. after that - run container building:
```bash
cd install/docker
docker-compose up -d --build
docker ps
```
If container is up - check heath:
```bash
telnet 127.0..0.1 27017
telnet 127.0..0.1 9093
mongosh -u yt_user -p 'SECRET' --authenticationDatabase yt_comments
```


### Troubleshooting
Check build process:
```bash
docker-compose logs -f
```
If ports are unavailable - go into container:
```bash
docker exec -it ytcomments_service bash
ps aux | grep mongo
less /data/db/mongod.log
less /data/db/debug_replica_init.log
mongod --config /etc/mongod.conf 2>&1>/dev/null &
```
etc..

Try to rebuild container:
```bash
docker-compose down
docker builder prune -a -f
volume rm docker_mongo-data
docker-compose up -d --build
```

## Useful commands

### Grpcurl
Service supports reflections. At first install `grpcurl` utility:
```bash
dnf install grpcurl
```
Now useful commnads for diagnostics and health tests:
```bash
grpcurl -plaintext 127.0.0.1:9093 list
grpcurl -plaintext 127.0.0.1:9093 describe ytcomments.v1.YtComments
```
View top comments and replies for some video:
```bash
grpcurl -plaintext -d '{"video_id":"cCEm3bF65lHD","page_size":10,"include_deleted":false,"sort":"NEWEST_FIRST"}' 127.0.0.1:9093 ytcomments.v1.YtComments/ListTop
grpcurl -plaintext -d '{"parent_id":"<cid>","page_size":50,"include_deleted":false,"sort":"OLDEST_FIRST"}' 127.0.0.1:9093 ytcomments.v1.YtComments/ListReplies
```


### Mongosh
Connect to MongoDB:
```bash
mongosh -u yt_user -p 'SECRET' --authenticationDatabase yt_comments
```
and:
```javascript
show dbs
use yt_comments
```
Next - misc commands for command shell:
```javascript
show collections
```
All root documents (every document is video):
```javascript
db.video_comments_root.find({}, {video_id:1, "totals.comments_count_total":1}).pretty()
```

In case of `video_id` is unknown, you may print all videos with comments:
```javascript
db.video_comments_root.find({}, {video_id:1}).forEach(d => print(d.video_id));
```

One root document for concrete video:
```javascript
db.video_comments_root.findOne({video_id: "YOUR_VIDEO_ID"})
```

List of all `comment_id` and `author_name` for video:
```javascript
var doc = db.video_comments_root.findOne({video_id: "YOUR_VIDEO_ID"});
if (doc && doc.comments) {
  Object.keys(doc.comments).forEach(function(cid){
    var c = doc.comments[cid];
    print(cid, c.author_name, c.author_uid, c.created_at, "likes="+c.likes, "dislikes="+c.dislikes);
  });
} else {
  print("No root doc or no comments");
}
```

Print comments texts (chunk -> `local_id`):
```javascript
db.video_comments_chunks.find({video_id: "YOUR_VIDEO_ID"}).forEach(function(ch){
  print("Chunk", ch._id);
  Object.keys(ch.texts || {}).forEach(function(lid){
    print("  ", lid, "=>", ch.texts[lid]);
  });
});
```

Select root document by `video_id`:
```javascript
db.video_comments_root.find({video_id: "VIDEO_ID"}).pretty()
```

One root document:
```javascript
db.video_comments_root.findOne({video_id: "VIDEO_ID"})
```

Check comment text (knowing `local_id` and `chunk_id`):
```javascript
let croot = db.video_comments_root.findOne({video_id: "VIDEO_ID"});
let cref = croot.comments["COMMENT_ID"].chunk_ref;
let chunk = db.video_comments_chunks.findOne({_id: ObjectId(cref.chunk_id)});
chunk.texts[cref.local_id];
```

Total amount of visible comments:
```javascript
db.video_comments_root.aggregate([
  {$match:{video_id:"VIDEO_ID"}},
  {$project:{visibleCount:{
    $size: {
      $filter:{
        input:{ $objectToArray:"$comments"},
        as:"c",
        cond:{ $eq:["$$c.v.visible", true] }
      }
    }
  }}}
])
```

Remove all comments tree (and all its chunks) for video with `video_id`:
```javascript
const r = db.video_comments_root.findOne({ video_id: "VIDEO_ID" });
if (r) {
  const ids = [];
  for (const [cid, meta] of Object.entries(r.comments || {})) {
    if (meta && meta.chunk_ref && meta.chunk_ref.chunk_id) {
      ids.push(meta.chunk_ref.chunk_id);
    }
  }
  // Remove root
  db.video_comments_root.deleteOne({ video_id: "VIDEO_ID" });
  // Remove chunks
  ids.forEach(id => db.video_comments_chunks.deleteOne({ _id: id }));
  print("Deleted root and", ids.length, "chunks");
} else {
  print("Root not found");
}
```


## Databases replication
To syncronize data between different service instances you may use replication mechanism.

Assumed that at all sides the MongoDB has been previously configured correctly, users have been created (see `install/mongo_setup.js-sample`) and a working database `yt_comments` has been created.

Replication occurs from the main service instance (role __PRIMARY__, let's call it a side __FROM__) to others (role __SECONDARY__, let's call them a side __TO__) that require updating.

__NOTE__: Replication process requires close versions of MongoDB at all sides - ±1 between major version number.


### Preparations on "FROM" side
First, you need to prepare and test the configuration on the __FROM__ side. 

Check the `Replica configuration` section in the `.env` file - uncomment and configure options this way:
```conf
MONGO_ROLE=PRIMARY
REPLICA_SET_NAME=ytcommentsReplicaSet
MONGO_HOSTS=TO_side1:27017,TO_side2:27017
```

The configuration in `/etc/mongodb.conf` should contain the following options:
```conf
security:
  authorization: "enabled"
  keyFile: /etc/mongodb/keyfile

replication:
  replSetName: "ytcommentsReplicaSet"
```
A pre-generated `/etc/mongodb/keyfile` must be present on the system. If it is missing or needs to be regenerated, this can be done with the command:```bash
openssl rand -base64 756 > /etc/mongodb/keyfile
chown mongod:mongod /etc/mongodb/keyfile
chmod 600 /etc/mongodb/keyfile
```
After that, the updated `keyfile` must be copied to all __TO__ sides and placed there in `/etc/mongodb/keyfile`. Also set access rights and owner to this for on all __TO__ sides.

__Important__: Make sure each MongoDB node uses the same `/etc/mongodb/keyfile` file. Configuration errors can cause nodes to fail to connect!!

Restart MongoDB, check its functionality and port availability (especially if the replication options were enabled in the configuration):
```bash
systemctl restart mongod
systemctl status mongod
ps aux | grep mongo
telnet 127.0.0.1 27017
mongosh -u yt_user -p 'SECRET' --authenticationDatabase yt_comments
```

MongoDB may become unavailable after enabling replication. Check the logs for replication messages:
```bash
cat /var/log/mongodb/mongod.log | grep "Replication"
```
 The database may have entered recovery mode, in which case you'll need to switch it to replication mode:
```bash
mongosh -u admin -p 'SuperSecretPassword' --authenticationDatabase admin
```
and then:
```javascript
rs.initiate({
  _id: "ytcommentsReplicaSet",
  members: [
    { _id: 0, host: "127.0.0.1:27017" }
  ]
})

rs.status()
```
Make sure the server shows up as __PRIMARY__ and restart MongoDB again.

If you see "state: RECOVERING" - probably you forgot set `replication` section in `/etc/mongodb.conf`. Configure it as described above.

It is also advisable to immediately check the availability of all __TO__ sides:
```bash
mongosh --host TO_side_IP --port 27017 -u admin -p 'SuperSecretPassword' --authenticationDatabase admin
```


### Preparations on "TO" side
The receiving sides also need to carry out similar checks and preparations.

Make sure that the `keyfile` was copied from __FROM__ side and placed correctly on the __TO__ side (as `/etc/mongodb/keyfile`).

Make similar changes to the configuration files. In `/etc/mongodb.conf`, make the same changes. In the `env` file:
```conf
MONGO_ROLE=SECONDARY
REPLICA_SET_NAME=ytcommentsReplicaSet
MONGO_HOSTS=FROM_side2:27017
```
Restart MongoDB and make sure that everything is OK.

Check local database on __TO__ side. Connect to it:
```bash
mongosh -u admin -p 'SuperSecretPassword' --authenticationDatabase admin
```
Check database - collection must exist and be empty:
```javascript
use yt_comments
show collections
```

Potential problems with MongoDB are resolved in a similar manner to that described above.

Also check the availability of the __FROM__ side:
```bash
mongosh --host FROM_side_IP --port 27017 -u admin -p 'SuperSecretPassword' --authenticationDatabase admin
```

__Note__: Replicating data on the `SECONDARY` nodes may take time. If you want to check the data on them, run:
```javascript
db.getMongo().setSecondaryOk()
```


### Perform replication process
Now need to add __TO__ sides to replica set. Go to __FROM__ side ( __PRIMARY__ ):
```bash
mongosh -u admin -p 'SuperSecretPassword' --authenticationDatabase admin
```
and add __TO__ side:
```javascript
rs.add("TO_sideIP:27017")
rs.status()
```
You could see it as __SECONDARY__.

__Note__: MongoDB in __SECONDARY__ state may not be available for read. In case of this issue try to call:
```javascript
db.getMongo().setSecondaryOk()
```

Connect to __TO__ side:
```bash
mongosh --host 192.168.7.116 --port 27017 -u admin -p 'SECRET' --authenticationDatabase admin
```
and:
```javascript
rs.status()
```
You may see JSON with two members, with states 'PRIMARY' and 'SECONDARY'. If so - the replication process is on. Need to wait.

Otherwise you may see something as:
```javascript
rs.status()
MongoServerError[NotYetInitialized]: no replset config has been received
```
This means that the command for synchronization has not yet arrived from __PRIMARY__ side.

In this case you need to wait some minutes, check network availability of __FROM__ side - login into shell and ping the __FROM__.

Also try to reinitialize replication process. At the __FROM__ side (on __PRIMARY__ - don't get confused!!) login as mongo-admin and run in the mongodb shell:
```javascript
rs.reconfig({
  _id: "ytcommentsReplicaSet",
  members: [
    { _id: 0, host: "FROM_side_IP:27017" },
    { _id: 1, host: "TO_side_IP:27017" }
  ]
})
```

Then relogin to __TO__ side as admin and check status again. You may see JSON with states as mentioned above. Also check the "health" for all members in this JSON - it have to be __1__.
After replication check:
```javascript
use yt_comments
show collections
```
You may see replicated collections:
* video_comments_chunks
* video_comments_root

Now try to view content:
```javascript
db.getMongo().setSecondaryOk()
use yt_comments
db.video_comments_root.findOne()
```
You may see some of documents in JSON format.

Monitor replication progress:
```javascript
rs.printReplicationInfo() // At finish oplog for all nodes will have same values
rs.printSecondaryReplicationInfo() // At finish replLag will be 0 secs
rs.status() // At finish optime will be same for all nodes (in members), all have "health: 1"
```

Finish replication. Login as mongo-admin on __FROM__ side ( __PRIMARY__ ) and run:
```javascript
rs.stepDown()
```

After switching roles ( __rs.stepDown__ ), ensure that:
1. The new __PRIMARY__ node is active:
```javascript
rs.status()
```

On all nodes check counts:
```javascript
use yt_comments
db.video_comments_root.find().count()
```
Counts must be same.


### Switch app to new instance
Now switch YurTube app to another service instance. In YurTube's `.env` modify `YTCOMMENTS_ADDR` - set IP of __TO__ side. Restart app and (optionally) stop local `ytcomments` service:
```
systemctl restart yurtube
systemctl stop ytcomments
```
Test some page with comments and make sure that comments are received via new service instance.
