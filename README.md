<<<<<<< HEAD
## Start Hadoop

# hdfs format

```
hadoop namenode -format

```
```
source $HOME/.bashrc

```

# Start hadoop cluster

```
start-all.sh

```

```

jps 

```

ensure jps listing DataNode, NameNode, SecondaryNameNode, ResourceManager running..




## hive

# Initialize the metadata database

```
cd /usr/local/Cellar/hive/3.1.2_3/libexec/bin
schematool -initSchema -dbType mysql

```

# Start hive 

```
hive

```


=======

# start spark cluster 

Open command prompt, run cluster master aka cluster manager

```
spark-class org.apache.spark.deploy.master.Master
```
# run worker

Now check, http://localhost:8080/ from ubuntu browser

open command prompt Run Worker 1 copy the master url and paste in below command

```
spark-class org.apache.spark.deploy.worker.Worker spark://10.0.0.164:7077
```
open command prompt Run Worker 2 copy the master url and paste in below command

```
spark-class org.apache.spark.deploy.worker.Worker spark://192.168.174.129:7077
```
refer to 
```
https://github.com/nodesense/cts-data-engineering-feb-2022/blob/main/notes/043-spark-cluster-ubuntu.md
```
