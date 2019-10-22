Team member: leon20121005 (https://github.com/leon20121005)
             asweetapple (https://github.com/asweetapple)


Operating System: Windows10
Programming Language: Python3


Spark environment setup:
    1. Make sure that Java 8 is installed, JAVA_HOME and PATH is also set
    2. Download spark-2.3.0-bin-hadoop2.7.tgz from https://spark.apache.org/downloads.html
    3. Extract the file into D:\spark
    4. Set environmental variables: SPARK_HOME,  D:\spark\spark-2.3.0-bin-hadoop2.7
                                    HADOOP_HOME, D:\spark\spark-2.3.0-bin-hadoop2.7
                                    PATH,        D:\spark\spark-2.3.0-bin-hadoop2.7\bin
    5. Download utilities for Windows from https://github.com/steveloughran/winutils/tree/master/hadoop-2.7.1/bin
    6. Extract the files into D:\spark\spark-2.3.0-bin-hadoop2.7\bin


Cluster environment setup:
    1. Two PCs, one for master and worker instance, the other for worker instance
    2. Master specification:
            Hardware: 4-core CPU, 8GB RAM
            OS: Windows10 64bit
    3. Worker specification:
            Hardware: 4-core CPU, 12GB RAM
            OS: Windows10 64bit
    4. Network setup:
            Place 2 PCs in the same LAN (local area network) and disable the firewall


Launching Spark Standalone cluster:
    1. Run the following command in cmd.exe to start a master instance on the machine:
            spark-class.cmd org.apache.spark.deploy.master.Master

    2. Run the following command in cmd.exe to start a worker instance on the machine:
            spark-class2.cmd org.apache.spark.deploy.worker.Worker spark://MASTER_URL:7077

    3. Run the following command in cmd.exe to run an application on the Spark cluster:
            spark-submit --master spark://MASTER_URL:7077 FILE_PATH 100


Source codes: problem01.py for task 1
              problem02.py for task 2
              problem03.py for task 3
              problem04.py for task 4


Compile:
    1. Require all input data in D:\data in both PCs
    2. Open cmd.exe and then change directory to where the source codes exist
    3. Run spark-submit --master spark://MASTER_URL:7077 FILE_PATH 100 in master to run the tasks


Output:
    Task 1: 12 output files in google drive folder task1_output
    Task 2: 6 output files in google drive folder task2_output
    Task 3: printscreen as problem03.jpg
    Task 4: 1 output file in google drive folder task4_output


Note: Because the size of output files in task 1, 2 and 4 is too large, it is placed in Google Drive
Google Drive link: https://drive.google.com/drive/folders/1bHzV8h4DQTUlmZ9-Fo8JD6vSF23j1XWs?usp=sharing


Efficiency:
    Task 1: 331.2470 seconds
    Task 2: 161.0277 seconds
    Task 3:  49.8455 seconds
    Task 4: 912.9925 seconds
