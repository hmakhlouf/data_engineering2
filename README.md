## Start Hadoop

hdfs format

```
hadoop namenode -format

```


 Start hadoop cluster

```
start-all.sh

jps 
```

ensure jps listing DataNode, NameNode, SecondaryNameNode, ResourceManager running..




## hive

Initialize the metadata database

```
cd /usr/local/Cellar/hive/3.1.2_3/libexec/bin
schematool -initSchema -dbType mysql

```

Start hive 

```
hive

```
