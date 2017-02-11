# MapReduce - Who To Follow

The Map Reduce program written in Java is an algorithm that will recommend to user X
a list of people to follow Fi based on the number of followers that X and Fi have in common. The
input and output of the algorithm will be as follows:

* Input: File containing n lines with space-separated integers:
X F1 F2 .. where Fi are followed by user X. For instance:

>1 3 4 5   
>2 1 3 5   
>3 1 2 4 5   
>4 1 2 3 5   
>5 3   

* Output: A File containing n lines in the following format:
X R1(n1) R2(n2) R3(n3) ...
where Ri are the ids of the people recommended to user X and ni is the number of followed
people in common between X and Ri. Recommended people Ri **must not be** followed by X
and **must** be ordered by decreasing values of ni. On the previous example:

>1 2(2)   
>2 4(3)   
>3    
>4    
>5 2(1) 1(1) 4(1) 


## How to run the program

Map Reduce code is built on compiler version : **Java 1.8** and using **Maven** plugin.

1. Clone the repo [Who To Follow Repo](https://github.com/lalet/MapReduce.git "Who To Follow")
2. Import the cloned repo as a Maven Project [Tutorial](http://javapapers.com/java/import-maven-project-into-eclipse/)
3. Open the project,    
        1. Run Configuration    
	![alt text](https://cloud.githubusercontent.com/assets/4597920/22804409/f9d90d98-eee6-11e6-903d-6ea7ef2af13d.png)
     
	2.Add Arguments and run.    
	![alt text](https://cloud.githubusercontent.com/assets/4597920/22804388/dff76a32-eee6-11e6-98cc-a33bdd43d84b.png)
----

### Running in hadoop
1. Copy the input file to the hdfs file system    

2. Export the project as a **Runnable jar**.    
   * Don't forget to **include the correct run configuration**   
   * Make sure option - **Package required libraries into generated jar** is selected.    
    ![alt text](https://cloud.githubusercontent.com/assets/4597920/22808208/c322facc-eef8-11e6-92a4-9b7b75602e0d.png)

3. Run the commmand : hadoop jar name of the jar file input file path output folder path    
Sample:             
> hadoop jar whotofollow.jar input.txt output_who_to_follow     

4.To view the results do:     
> hdfs dfs -cat [output directory name]/*

### Compiling the code without Eclipse.

**Make sure that maven plugin is installted in the system**

1. Go the the project directory containing the pom.xml file    
2. Run the command
     
>> mvn package  
3. Verify if a jar file named MapReduce-0.0.1-SNAPSHOT.jar is created inside the folder named **target**        
4. RUN using the command
  
>> hadoop jar (path to the MapReduce-0.0.1-SNAPSHOT.jar file) (input file path) (output directory path)    

##### After each run in hadoop hdfs, please remove the folder named as "intermediate" since it gets created everytime you run the whotofollow jar. Also make sure that the folder name given to the output directory does not exist in the hdfs.
