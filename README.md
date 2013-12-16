采用Storm框架流式计算每种事件在每天发生的次数，用作采样的桶数的参考。

部署情况：

141： zookeeper(/usr/local/zookeeper-3.4.5) nimbus

142,143,144,145: Supervisor

相关命令：

启动nimbus: nohup /usr/local/storm-0.8.1/bin/storm nimbus >/dev/null 2>&1 &
  
启动web ui: nohup /usr/local/storm-0.8.1/bin/storm ui >/dev/null 2>&1 &
  
启动supervisor: nohup /usr/local/storm-0.8.1/bin/storm supervisor >/dev/null 2>&1 &
  
停止服务: sh /usr/local/storm-0.8.1/bin/stop.sh 

启动event counter: /usr/local/storm-0.8.1/bin/storm jar
  /home/hadoop/git_project_home/stream_processor/dist/stream_processor-jar-with-dependencies.jar
  com.xingcloud.stream.storm.topology.StreamLogEventCountTopology event_counter