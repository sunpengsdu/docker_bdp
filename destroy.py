import redis
from docker import Client
import sys
import os
import shutil

if len(sys.argv) < 2:
        print 'no enough parameter'
        exit()

app_name       = sys.argv[1]




print 'destroying application: ', app_name
sys.stdout.flush()

redis_ip =  (os.environ.get('REDIS_IP'))
redis_ip = '192.168.2.21'
try:
        app_redis = redis.StrictRedis(host=redis_ip, port=6379, db=0)
        app_redis.keys('*')
except:
        print 'cannot connect to redis, please check REDIS_IP'
        exit()

print '\nchecking envirament\n'
sys.stdout.flush()

swarm_url =  (os.environ.get('SWARM_URL'))
swarm_url = '192.168.2.21:2385'
if str(swarm_url) == 'None':
        print 'please check the enviroment configuration of SWARM_URL'
        exit()
try:
        docker_client = Client(base_url=swarm_url)
        docker_client.containers()
except:
        print 'cannot work with swarm, please check swarm configuration'
        exit()


print 'deleting DNS record\n'
sys.stdout.flush()

if os.path.isfile('/opt/bdp/dns/'+app_name):
        os.remove('/opt/bdp/dns/'+app_name)

print 'deleting configuration data\n'
sys.stdout.flush()
if os.path.exists('/opt/bdp/app/'+app_name):
        shutil.rmtree('/opt/bdp/app/'+app_name)


if os.path.exists('/export/hdfs/opt/hadoop/'+app_name):
        shutil.rmtree('/export/hdfs/opt/hadoop/'+app_name)


if os.path.exists('/export/hdfs/cluster_data/'+app_name):
        shutil.rmtree('/export/hdfs/cluster_data/'+app_name)

print 'destroying containers'
sys.stdout.flush()
container_set = app_redis.smembers(app_name)

for container in container_set:
	print container
	sys.stdout.flush()
	id = app_redis.hget(container, 'id')
	try:
		docker_client.kill(id)
		docker_client.remove_container(id, True, True, True)
	except:
		pass
	app_redis.delete(container)

app_redis.hdel('bdp_apps', app_name)
app_redis.delete(app_name)
print "destroy the cluster sucessfully"
sys.stdout.flush()
