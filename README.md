# gkvs-java
gKVS Java Client

The purpose of the gKVS Java Client is to provide better API to access data.


### Performance

Environment: 
1. gKVS-java running on JVK8 on MacBook Pro 2015
2. gKVS running on MacOs MacBook Pro 2015 the same machine
3. Aerospike running on Ubuntu 16.04.4 LTS on Virtual Box on the same MacBook Pro 2015
 

Random sync gets (JVM->gKVS->VirtualBox->Aerospike->Memory)
```
10000 get requests in 4622 milliseconds
```

