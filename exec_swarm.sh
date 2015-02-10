nohup swarm manage --strategy "random"  -H tcp://192.168.2.21:2385 etcd://192.168.2.21:4001/swarm  > ~/swarm.log 2>&1 &
