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
>>>>>>> 85ed77e05a5307263166fff2c92bfadfb96fbe89
