dbs_info = [
{ 'host': 'test', 'ip': '192.168.1.1', 'port': 27077 },

]

key_file = '/root/.ssh/id_rsa'
local_store = '/mnt/dbbaktmp/'
pid_file = '/tmp/mongo-store.pid'
log = '/tmp/daemon.log'
error_log = '/tmp/daemon.log'
access_key_id = ''
access_key_secret = ''
region_id = 'cn-hangzhou'
ssh_user = 'root'

oss_endpoint = 'oss-cn-hangzhou-internal.aliyuncs.com'
oss_bucket = 'test-mongo-backup-low'

#记录CURSOR
cur_file = '/tmp/curfile'


#dump oplog interval 秒
dumpop_interval = 1800

#upload oss interval 秒
upload_interval = 900

#full backup db at
full_bakdb_at = '00:10'

#schedule interval 秒
sche_sleep = 1
