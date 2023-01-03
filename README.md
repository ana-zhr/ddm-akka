# ddm-akka

## Code for compilation

```sh
mvn package -f "pom.xml"
```
```sh
cd data 
```
```sh
cd ..
```

```sh
java -Xmx3g -ea -cp target/ddm-akka-1.0.jar de.ddm.Main master 

```

```sh
# on the master system host
java -Xmx3g -ea -cp target/ddm-akka-1.0.jar de.ddm.Main master -w 0 -h MASTER_IP

# on the worker system host
java -Xmx3g -ea -cp target/ddm-akka-1.0.jar de.ddm.Main worker -w 0 -mh MASTER_IP -h WORKER_IP
```
