#!/bin/bash

###########Hadoop 3.x installation
# (https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html)

#Java Installation (Open JDK)
sudo apt-get update
sudo apt-get install openjdk-11-jdk

#Create reference link to Java installation [optional]
sudo ln -s /usr/lib/jvm/java-11-openjdk-amd64 /opt/java

#Locate java installation directory
whereis java

#SSH installation
sudo apt-get install ssh
sudo apt-get install pdsh

#Download Hadoop
# cd ~/Downloads
#Hadoop version may need to be changed
curl https://downloads.apache.org/hadoop/common/stable/hadoop-3.3.4.tar.gz -o hadoop-3.3.4.tar.gz
#Extract Hadoop
tar xzvf hadoop-3.3.4.tar.gz
#Move Hadoop installation
sudo mv hadoop-3.3.4 /opt/hadoop

 
#JAVA_HOME configuration
cd /opt/hadoop
sudo nano etc/hadoop/hadoop-env.sh
#set to the root of your Java installation
export JAVA_HOME=/opt/java

#HADOOP_HOME configuration
sudo nano ~/.bashrc
#############################
export HADOOP_HOME=/opt/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME 
export HADOOP_COMMON_HOME=$HADOOP_HOME 

export HADOOP_HDFS_HOME=$HADOOP_HOME 
export YARN_HOME=$HADOOP_HOME 
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native 
export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$HADOOP_HOME/lib/native"
export HADOOP_INSTALL=$HADOOP_HOME
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin 
##############################################
# => reloads the changes
source ~/.bashrc

#Standalone mode [Execute below commands w/o making any config changes - OPTIONAL TO TEST STANDALONE MODE]
cd $HADOOP_HOME
sudo mkdir input
sudo cp etc/hadoop/*.xml input
sudo bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.4.jar grep input output 'dfs[a-z.]+'
cat output/*

#####################################################################
#Pseudo Distribution mode
#####################################################################

#Configuration
sudo nano etc/hadoop/core-site.xml
##################################
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
###################################

sudo nano etc/hadoop/hdfs-site.xml
####################################
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
####################################

#Configure password less SSH
# => (will prompt password)
ssh localhost

ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

#=> (login w/o password)
ssh localhost

#Format Namenode
hdfs namenode -format

#Modify pdsh's default rcmd to ssh
sudo nano ~/.bashrc
export PDSH_RCMD_TYPE=ssh
source ~/.bashrc

#Start Namenode
#sbin/start-dfs.sh
#=> (Web URL: http://localhost:9870/)
start-dfs.sh

#pdsh - rcmd socket permission denied error fix
# (https://stackoverflow.com/questions/42756555/permission-denied-error-while-running-start-dfs-sh)

#Create HDFS directories
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/training

hdfs dfs -mkdir /tmp

#Permission change to /user directory
hdfs dfs -chmod -R 777 /user

#Permission change to /tmp directory
hdfs dfs -chmod -R 1777 /tmp

#Permission change to local /tmp directory
sudo chmod -R 777 /tmp

#List HDFS directories
hdfs dfs -ls /user/training

#MapReduce Example
cd $HADOOP_HOME
hdfs dfs -mkdir /user/training/input
hdfs dfs -put etc/hadoop/*.xml /user/training/input

hdfs dfs -rm -r /user/training/output

hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.4.jar grep /user/training/input /user/training/output 'dfs[a-z.]+'
hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.4.jar wordcount /user/training/input /user/training/output

hdfs dfs -get /user/training/output output
cat output/*

#Stop Namenode
stop-dfs.sh

#########################################################################
#YARN on a Single Node
##########################################################################
sudo nano etc/hadoop/mapred-site.xml

##########################
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
##########################

sudo nano etc/hadoop/yarn-site.xml

###########################
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
	
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
    </property>
	
	<property>
	    <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
        <description>Whether virtual memory limits will be enforced for containers</description>
    </property>	
############################

#Start Namenode
#=> (Web URL: http://localhost:9870/)
start-dfs.sh

#Start ResourceManager and NodeManager
#=> (Web URL: http://localhost:8088/)
start-yarn.sh

#Run MR Job
hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.4.jar grep /user/training/input /user/training/output 'dfs[a-z.]+'
hdfs dfs -cat /user/training/output/part*

#Run MR Job (WordCount)
hdfs dfs -put input/*.txt /user/training/input
hdfs dfs -rm -r /user/training/output
hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.4.jar wordcount /user/training/input /user/training/output
hdfs dfs -cat /user/training/output/part*

#Stop YARN (ResourceManager and NodeManager)
sbin/stop-yarn.sh

#Stop Namenode
sbin/stop-dfs.sh