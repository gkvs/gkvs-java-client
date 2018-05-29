# gkvs-java
gKVS Java Client

The purpose of the gKVS Java Client is to provide better API to access data.
* Scala friendly design.
* Based on GPRC and protobuf
* Support sync and async calls
* Low footprint

### Performance

Environment: 
1. gKVS-java running on JDK8 on MacBook Pro 2015
2. gKVS running on the same MacOs MacBook Pro 2015
3. Aerospike running on Ubuntu 16.04.4 LTS on Virtual Box on the same MacBook Pro 2015
 

Random sync gets (JVM->gKVS->VirtualBox->Aerospike->Memory)
```
10000 get requests in 4622 milliseconds
```

### API

Two types of API are supported: sync and async

#### GET
```
byte[] value = GKVS.Client.get("TEST", "key").sync().value().bytes();
```

#### PUT
```
GKVS.Client.put("TEST", "key", "value").sync();
```

#### REMOVE
```
GKVS.Client.remove("TEST", "key").sync();
```

#### COMPARE_AND_PUT
```
Record record = GKVS.Client.get("TEST", "key").sync();
boolean updated = GKVS.Client.put("TEST", "key", "replace_value").compareAndPut(record.version()).sync().updated();
```

#### EXISTS
```
boolean exists = GKVS.Client.exists("TEST", "key").sync().exists();
```

### Maven

gKVS-java is the single jar with all shaded libraries except "com.google.code.findbugs:jsr305".
Jsr305 is using for annotations only.

How to make release version
```
mvn clean install -Prelease
```

How to see all final deps
```
mvn clean install -Pdeps
ls -l target/gkvs-java/lib/
```

How to build regular simple jar
```
mvn clean install
```
