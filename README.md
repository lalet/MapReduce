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
people in common between X and Ri. Recommended people Ri *must not be* followed by X
and *must* be ordered by decreasing values of ni. On the previous example:

>1 2(2)   
>2 4(3)   
>3    
>4    
>5 2(1) 1(1) 4(1) 


## How to run the program

This code is built on compiler version : *Java 8*

1. Clone the repo [Who To Follow Repo](https://github.com/lalet/MapReduce.git "Who To Follow")
2. Import the project as a Maven Project [Tutorial](http://javapapers.com/java/import-maven-project-into-eclipse/)
3.    
