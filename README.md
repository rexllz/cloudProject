# User Behaviors Analysis Using Spark-SQL

###This is the term project of course COMP7305, Cluster and Cloud Computing, of Dept. of Computer Science, The University of Hong Kong. Supervised by [Prof. Cho-Li Wong](https://i.cs.hku.hk/~clwang/).

In this project, we analyse the user behaviours based on an online education website focused on programming techniques.

Main programming language used in our project is **Scala**. We also wrote **Crawler** in Python to collect Course Labels and Catalogs from website, and then join the tabel with cleaned website log.

We focused on **Parallel Programming** features in Spark using Scala, and parameters effect when submitting, as well as discovered **VCpus** and **Memory** configuration in Xen. 

After a series of improvements, we accelerate the running time form more than 3 minutes to less than 1.5 minutes.

## How to start the program

We wrote the command into a shell script, all you need to do is to run this script. Pre-requirements are hadoop filesystem and yarn started.

1. Start HDFS & Yarn (Password: "wangwang")

   ```shell
   hduser@gpu2:/opt/hadoop-2.7.5/sbin$ ./start-dfs.sh
   hduser@gpu2:/opt/hadoop-2.7.5/sbin$ ./start-yarn.sh
   ```

2. Start Spark-history-server

   ```shell
   hduser@gpu2:/opt/spark-2.4.0-bin-hadoop2.7/sbin$ ./start-history-server.sh
   ```

3. Start our program

   ```shell
   hduser@gpu2:/opt/spark-2.4.0-bin-hadoop2.7$ ./submit_demo.sh
   ```

## Source Code

(GitHub 链接 )

- scala

	- Main.scala (295 lines) is written by ourselves.
	- StatDAO.scala (151 lines) is written by ourselves.
	- MinuteCityElement.scala (9 lines) is written by ourselves.
	- LabelMinuteElement.scala (9 lines) is written by ourselves.
	- LabelCityElement.scala (9 lines) is written by ourselves.
	- TotalTable.scala (12 lines) is written by ourselves.
	- StatDAO.scala (136 lines) is written by ourselves.

- python
	- imooc_name.py(125 lines) is written by ourselves.

## Data files

(Google drive 链接)







