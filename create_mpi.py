import os
import string
import time
import sys
import redis
from docker import Client
import paramiko
import threading
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import socket
import shutil

mutex = threading.Lock()
syncd_num = 0


def view_bar(num=1, sum=10, bar_word=":"):
        rate = float(num) / float(sum)
        rate_num = int(rate * 100)
#	print '%d%% ' %(rate_num),
#
        sys.stdout.flush()
#        for i in range(0, num):
#                os.write(1, bar_word)
#                sys.stdout.flush()



def swarm_create_container(slave_tmp_name):

        try:
                container_tmp_return = docker_client.create_container(image=image, name=container_pt_slave[slave_tmp_name]['name'], hostname=container_pt_slave[slave_tmp_name]['name'], mem_limit='2g')
                docker_client.start(container_tmp_return.get('Id'))

        except:
                print 'cannot create container'
                for keys in app_redis.keys(app_name+'*'):
                        app_redis.delete(keys)
                exit()
	mutex.acquire()
        app_redis.sadd(app_name, container_pt_slave[slave_tmp_name]['name'])
        app_redis.hset(container_pt_slave[slave_tmp_name]['name'], 'name', container_pt_slave[slave_tmp_name]['name'])
        app_redis.hset(container_pt_slave[slave_tmp_name]['name'], 'id', container_tmp_return['Id'])
        container_pt_slave[slave_tmp_name]['id']=container_tmp_return['Id']
        container_ids[container_tmp_return['Id']] = slave_tmp_name
	
	global syncd_num
	syncd_num = syncd_num + 1
	view_bar(int(syncd_num/float(container_num)*10), 10, "#")
	mutex.release()





def ssh_hosts_dhcp(slave_tmp_name):
        ssh_tmp_dst = container_pt_slave[slave_tmp_name]['host']
	address = (ssh_tmp_dst, 2384)
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect(address)
	command = 'pipework eth1 '+slave_tmp_name+' ' +'dhcp'
	s.send(command)
	data = s.recv(512)
	s.close()

	mutex.acquire()
	global syncd_num
	syncd_num = syncd_num + 1
	view_bar(int(syncd_num/float(container_num)*10), 10, "#")	
	mutex.release()


def ssh_docker_ip(slave_tmp_name):
        ssh_tmp_dst = container_pt_slave[slave_tmp_name]['host']
        address = (ssh_tmp_dst, 2384)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(address)
        command = 'getip '+slave_tmp_name+' ' +'eth1'
        s.send(command)
        tmp_ip = s.recv(512)
        s.close()

	mutex.acquire()
	app_redis.hset(container_pt_slave[slave_tmp_name]['name'], 'ip', tmp_ip)
        container_pt_slave[slave_tmp_name]['ip']=tmp_ip
	global syncd_num
	syncd_num = syncd_num + 1
	view_bar(int(syncd_num/float(container_num)*10), 10, "#")	
	mutex.release()



def sync_file(ip, master_flag=0):
	t = paramiko.Transport((ip, 22))
	t.connect(username = "root", password = "111111")
	sftp = paramiko.SFTPClient.from_transport(t)

	filename_list = []
	filename_list.append('slaves')

	for filename in filename_list:
		remotepath = "/opt/" + filename
		localpath  = workdir + filename
		sftp.put(localpath,remotepath)


	remotepath = "/etc/" + "hosts"
	localpath  = workdir + "hosts"
	sftp.put(localpath,remotepath)
	
	if master_flag == 0:
		global syncd_num
		mutex.acquire()
		syncd_num = syncd_num + 1
		view_bar(int(syncd_num/float(container_num)*10), 10, "#")
		mutex.release()
	t.close()


if len(sys.argv) < 4:
	print 'usage: [application_name] [MPI_version] [worker number]'
	print 'no enough parameter'
	exit()

app_name       = sys.argv[1]
version        = sys.argv[2]
container_num  = sys.argv[3]



docker_repo =  (os.environ.get('DOCKER_REPO'))
docker_repo = '192.168.2.21:5000'


if str(docker_repo) == 'None':
        print 'please check the enviroment configuration of DOCKER_REPO'
        exit()

if version == 'mpich-3.1.3':
        image = docker_repo + '/' + version
else:
        print 'BDP cannot current version of GraphLab'
        exit()




print 'the application name is:                ', app_name
print 'the cluster is created from image:      ', image
print 'the number of workers in the cluster is:', container_num



redis_ip =  (os.environ.get('REDIS_IP'))
redis_ip = '192.168.2.21'
if str(redis_ip) == 'None':
	print 'please check the enviroment configuration of REDIS_IP'
	exit()

hdfs_address =  (os.environ.get('HDFS_ADDRESS'))
hdfs_address = '192.168.0.21:49000'

if str(hdfs_address) == 'None':
        print 'please check the enviroment configuration of HDFS_ADDRESS'
        exit()



swarm_url =  (os.environ.get('SWARM_URL'))
swarm_url = "192.168.2.21:2385"


if str(swarm_url) == 'None':
	print 'please check the enviroment configuration of SWARM_URL'
	exit()

try:
	app_redis = redis.StrictRedis(host=redis_ip, port=6379, db=0)
	app_redis.keys('*')
except:
	print 'cannot connect to redis, please check REDIS_IP'
	exit()

if app_redis.exists(app_name):
	print 'the app', app_name, 'exists'
	print 'you cannot create a cluster with same application name'
	exit()

container_pt_master = {}
container_pt_slave = {}
container_ids = {}

container_pt_master[app_name + '-master']={'name':app_name+'-master'}

for i in range(1, int(container_num)+1):
	container_pt_slave[app_name + '-slave'+str(i)]={'name':app_name+'-slave'+str(i)}

try:
	docker_client = Client(base_url=swarm_url)
	docker_client.containers()
except:
	print 'cannot work with swarm, please check swarm configuration'
	exit()

print 'creating master node'
try:
	container_return = docker_client.create_container(image=image, name=app_name + '-master', hostname=app_name + '-master', mem_limit='2g')
	print 'starting master node'
	docker_client.start(container_return.get('Id'))
except:
	print 'cannot create container'
	exit()


app_redis.sadd(app_name, app_name + '-master')
app_redis.hset(app_name+'-master', 'name', app_name+'-master')
app_redis.hset(app_name+'-master', 'id', container_return['Id'])
container_pt_master[app_name + '-master']['id']=container_return['Id']
container_ids[container_return['Id']] = app_name + '-master'



print 'creating slave nodes'

syncd_num = 0
slave_name_list = container_pt_slave.keys()
slave_name_list.sort()

pool = ThreadPool(5)
pool.map(swarm_create_container, slave_name_list)
pool.close() 
pool.join() 

#for slave_name in slave_name_list:
#	swarm_create_container(slave_name)
#	print ''
#	print '#############creating slave node:', container_pt_slave[slave_name]['name']
#
#	try:
#        	container_return = docker_client.create_container(image=image, name=container_pt_slave[slave_name]['name'], hostname=container_pt_slave[slave_name]['name'], mem_limit='2g')
#		print 'starting slave node', container_pt_slave[slave_name]['name']
#		docker_client.start(container_return.get('Id'))
#
#	except:
#        	print 'cannot create container'
#		for keys in app_redis.keys(app_name+'*'):
#			app_redis.delete(keys)
#        	exit()
#	print 'adding slave node info:', container_pt_slave[slave_name]['name'], 'to Redis'
#	app_redis.sadd(app_name, container_pt_slave[slave_name]['name'])
#	app_redis.hset(container_pt_slave[slave_name]['name'], 'name', container_pt_slave[slave_name]['name'])
#	app_redis.hset(container_pt_slave[slave_name]['name'], 'id', container_return['Id'])
#	container_pt_slave[slave_name]['id']=container_return['Id']
#	container_ids[container_return['Id']] = slave_name

print '\nfetching the container location'

container_ps_a = docker_client.containers();
for container_info in container_ps_a:
	if container_ids.has_key(container_info['Id']):
		tmp_name = container_info['Names'][0].split('/')
		if tmp_name[2] == app_name + '-master':
			container_pt_master[app_name + '-master']['host'] = tmp_name[1]
			app_redis.hset(app_name + '-master', 'host', tmp_name[1])

		else:
			container_pt_slave[tmp_name[2]]['host'] = tmp_name[1]
			app_redis.hset(tmp_name[2], 'host', tmp_name[1])





print 'allocating IP for the master node'
ssh_dst = container_pt_master[app_name + '-master']['host']
address = (ssh_dst, 2384)
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(address)
command = 'pipework eth1 '+container_pt_master[app_name + '-master']['name']+' ' +'dhcp'
s.send(command)
data = s.recv(512)
s.close()


print 'allocating IP for the slave nodes'

syncd_num = 0
slave_name_list = container_pt_slave.keys()
slave_name_list.sort()

pool = ThreadPool(10)
pool.map(ssh_hosts_dhcp, slave_name_list)
pool.close() 
pool.join() 



print 'refreshing the containers ip address'
time.sleep(1)



ssh_dst = container_pt_master[app_name + '-master']['host']
address = (ssh_dst, 2384)
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(address)
command = 'getip '+container_pt_master[app_name + '-master']['name']+' ' +'eth1'
s.send(command)
tmp_ip = s.recv(512)
s.close()


syncd_num = 0
app_redis.hset(container_pt_master[app_name + '-master']['name'], 'ip', tmp_ip)
container_pt_master[app_name + '-master']['ip']=tmp_ip

pool = ThreadPool(10)
pool.map(ssh_docker_ip, slave_name_list)
pool.close()
pool.join()


#print 'the slave node is: '

#slave_name_list = container_pt_slave.keys()
#slave_name_list.sort()
#for slave_name in slave_name_list:
#	print container_pt_slave[slave_name]['name'], 'Location:', container_pt_slave[slave_name]['host'], 'IP:', container_pt_slave[slave_name]['ip']






print '\ngenerating the configuration file'

if os.path.exists('/opt/bdp/app/'+app_name):
	shutil.rmtree('/opt/bdp/app/'+app_name)
os.makedirs('/opt/bdp/app/'+app_name) 

if os.path.isfile('/opt/bdp/dns/'+app_name):
        os.remove('/opt/bdp/dns/'+app_name)





workdir = '/opt/bdp/app/'+app_name+'/'


f1 = open(workdir+'hosts','w')
f_dns = open('/opt/bdp/dns/'+app_name, 'w')

f1.write("127.0.0.1	localhost\n")
f1.write(container_pt_master[app_name + '-master']['ip'] + '\t' + container_pt_master[app_name + '-master']['name'] + '\n')

f_dns.write(container_pt_master[app_name + '-master']['ip'] + '\t' + container_pt_master[app_name + '-master']['name'] + '\n')


for slave_name in slave_name_list:
	f1.write(container_pt_slave[slave_name]['ip'] + '\t' +  container_pt_slave[slave_name]['name'] + '\n')
	f_dns.write(container_pt_slave[slave_name]['ip'] + '\t' +  container_pt_slave[slave_name]['name'] + '\n')


f1.flush()
f_dns.flush()
f1.close()
f_dns.close()

f1 = open(workdir + 'slaves', 'w')

f1.write(container_pt_master[app_name + '-master']['ip'] + '\n')
for slave_name in slave_name_list:
        f1.write(container_pt_slave[slave_name]['ip'] + '\n')
f1.flush()
f1.close()


print 'syncing configuration file for master node'
sync_file(container_pt_master[app_name + '-master']['ip'], 1)




print 'syncing configuration file for slave nodes'
syncd_num = 0
slave_ip_list=[]
for slave_name in slave_name_list:
	slave_ip_list.append(container_pt_slave[slave_name]['ip'])

pool = ThreadPool(10)
pool.map(sync_file, slave_ip_list)
pool.close()
pool.join()

app_redis.hset('bdp_apps', app_name, 'MPI')



print 'sucessfully create the cluster\n'

print 'the master node is: '
print container_pt_master[app_name + '-master']['name'], 'Location:', container_pt_master[app_name + '-master']['host'], 'IP:', container_pt_master[app_name + '-master']['ip']

print 'login to the master node to start the framework, submit GraphLab jobs\n'
