# CMU15-445
You need a java environment first

Install ant which is a java project builder, on mac/linux run: brew install ant

We used Zookeeper, log4j, junit for this project, you should install them ahead

To build: 
run ant in the project root, ant will run build.xml file to build the whole project. 

To run: 
you need to start Zookeeper with script the zookeeper-3.4.11/bin/zkServer.sh, use zkServer.sh start

you need to set up ecs server first, use command: java -jar m2-ecs.jar

note that this server takes 8000 by default, you should avoid using it in the future

you can set up as many servers as you want, use commnad: java -jar m2-server.jar -p {port number}

you can set up many clients, use command: java -jar m2-client.jar {port number}
  if you are using the same device, please avoid using duplicate ports

  Servers will run and load existing database files if there are any, no more jobs needed here
 
On client side, you need to connect to a server first, use command: connect {ip address} {port number}
  

Then you can input commands:
  
  put {key} {value}: to put a kv pair in to the system
  
  get {key}: to get a key from the system
  
  tput: to start a transaction (then you can type many <key> <value> to put pairs in a transaction)
  
  tput confirm: to end a transaction
  
 