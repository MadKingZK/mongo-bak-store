dbs_info = [
{ 'host': 'dev-test', 'ip': '192.168.1.2', 'port': 27077 },

]

key_file = '/root/.ssh/id_rsa'
local_store = '/mnt/dbbaktmp/'
pid_file = '/tmp/mongo-store.pid'
log = '/tmp/daemon.log'
error_log = '/tmp/daemon.log'
access_key_id = ''
access_key_secret = ''
region_id = 'shenzhen'
ssh_user = 'root'

oss_endpoint = 'oss-cn-shenzhen-internal.aliyuncs.com'
oss_bucket = 'test-mongo-backup-low'

#记录CURSOR
cur_file = '/tmp/curfile'


#dump oplog interval 秒
dumpop_interval = 1800

#upload oss interval 秒
upload_interval = 900

#schedule interval 秒
sche_sleep = 1
