0.1.0 Init build 2019
0.2.0 Upgrade dependencies
   * Apache Spark 1.x =>  2.4.6
   * Scalapb 0.7.0-rc4 => 0.9.8 
   * scalapb-json4s => 0.10.0 
   * spark-tensorflow-connector 1.6.0 => 1.15.0
   * tensorflow 1.8.0 => 1.15.0 
   * protobuf version 3.0.0 => 3.8.0
    
   * Build changes:
   using mvn build on Mac Pro, I encounter and error
   ```
   Caused by: java.io.FileNotFoundException: Unsupported platform: protoc-3.0.0-osx-x86_64.exe
       at com.github.os72.protocjar.Protoc.extractProtoc (Protoc.java:223)
       at com.github.os72.protocjar.Protoc.extractProtoc (Protoc.java:184)
       at com.github.os72.protocjar.Protoc.runProtoc (Protoc.java:68)
       at com.github.os72.protocjar.Protoc.runProtoc (Protoc.java:55)
       at scalapb.ScalaPBC$$anonfun$2.apply (ScalaPBC.scala:61)
       at scalapb.ScalaPBC$$anonfun$2.apply (ScalaPBC.scala:61)
       at protocbridge.ProtocBridge$.runWithGenerators (ProtocBridge.scala:117)
       at scalapb.ScalaPBC$.main (ScalaPBC.scala:53)
       at scalapb.ScalaPBC.main (ScalaPBC.scala) 
   ```
   to get around this error, I create a Dockerfile of ubuntu:18.04 image and build the project 
   inside the docker instance.
   
   The docler-based build is slow, as the docker doesn't remember the maven artifacts already downloaded 
   so each build it try to download all the maven dependencies, it at least it works. 
   
   * in Docker run, need to increase memory used to 2g, 
     as default docker memory is not enough to run Spark Job. 
     
  