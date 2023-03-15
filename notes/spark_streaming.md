# Spark Streaming

Spark_Architecture
    - engine - spark driver/core
    - storage - hdfs
    - computation - in memory
    - spark driver takes jobs - init
    - 
    -      spark_driver
                |
            Cluster master
            Mesos, Yarn, or Standlone
    cluster worker  -  cluster worker - cluster worker
        Executor          Executor         executor
    
    - executor - jvm
    - default depnds on file size 
    - for one partition one executor
    - input data structures 
        - rdd - batch processing , streaming
        - dataframe - interactive analysis, streaming
        - dataset 
        - 
        - rdds 
            - functional programming
            - type-safe
            - performance limitation
            - to_df(), createDataframe()
            - rdd_csv - split - schema - df
        - dataframes
            - relational
            - catalyst query opt
            - tungsten direct/packed ram
            - jit code generation
            - sorting/shuffling without deserializing
            - faster - opt, easy mem managment
        - dataset 
            - extention of dataframe
            - object oriented prgrmng api
            - datasets are slower than dataframes
            - 
    - options to analyse dataframes
        - dsl - dataframe structured lan
        - spark sql
        - hive query lang
    