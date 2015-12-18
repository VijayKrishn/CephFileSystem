# CephFileSystem
distributed file system

#The following funtions are handled by the file system.

Sample package has all the implementation used to run and test the project.

The file system can be generated and these files are placed inside the client --> create files can be done by FSPropagate inside sample.request package.

Sequential read and sequential write --> Requests can generated using ReqGenerate inside sample.request package.

Load balancing, fault tolerance like add a node and failed node are handled and the request can be issued by admin to monitor.
File transfer is handled whenever there is a failure or overloaded.

The monitor and client ops are the heart of the file system which monitors the file system and handles the client request respectively.

The logger can be run with LogPrintServer inside sample.

The file system is given by a tree structure from a file inside the files. You can check the tree structure in test.txt inside files.

#Configuration files are present inside E2_Box folder
