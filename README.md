# gkvs-java
gKVS Java Client

The purpose of the gKVS Java Client is to provide better API to access data.


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

Simple get example:
```
byte[] value = GKVS.Client.get("TEST", UUID.randomUUID().toString()).sync().value();
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
mvn clean install -Plibs
ls /target/gkvs-java/libs
```

How to build regular simple jar
```
mvn clean install
```
