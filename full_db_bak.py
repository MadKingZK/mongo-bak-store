import time, threading, settings
from tools import sshTools, aliEcsSnapshot
from os import listdir
from datetime import datetime

def full_db_backup():
    #一次只做一个host的，相当于抛出一个线程去维护一个host的所有db的全量备份，也就是备份这个host的所有磁盘，
    wholebak_infos = get_wholebak_infos()
    for host, info in wholebak_infos.items():
        job_thread = threading.Thread(target=make_full_backup, args=(host, info))
        job_thread.start()

def make_full_backup(host, info):
    ali_ecs_snap = aliEcsSnapshot(settings.access_key_id, settings.access_key_secret, settings.region_id)
    #锁表
    ip = info.get('ip')
    port_lst = info.get('port')
    ssh = sshTools(ip)
    tags = []
    des_info = ''
    for port in port_lst:
        lock_time = int(time.time())
        ssh.execute_cmd("echo 'db.runCommand({{fsync:1,lock:1}});' | mongo --port {port} admin -u {user} -p {password} ".format(port=port, user=settings.db_user, password=settings.db_password))
        # 检查是否锁，没有则抛错，继续锁，锁三次, 再失败则放弃，报错

        # 取timestamp
        status, start_timestamp, err = ssh.execute_cmd(
            '''
            echo \'\'\'rs.slaveOk()
                use local
                db.replset.minvalid.find({},{_id:0,begin:1})
                \'\'\' | mongo --port %d admin -u %s -p %s --quiet | grep begin | awk -F '[(,)]' '{print $2}'
            ''' % (port, settings.db_user, settings.db_password))
        start_timestamp = start_timestamp[0].strip()
        status, start_timestamp_cur, err = ssh.execute_cmd(
            '''
            echo \'\'\'rs.slaveOk()
                use local
                db.replset.minvalid.find({},{_id:0,begin:1})
                \'\'\' | mongo --port %d admin -u %s -p %s --quiet | grep begin |awk -F '[(,)]' '{print $3}'
            ''' % (port, settings.db_user, settings.db_password))
        start_timestamp_cur = start_timestamp_cur[0].strip()
        if not start_timestamp:
            start_timestamp = lock_time
        tags.append({'Key':'%s_%s'%(host, port),'Value':'{"%s":{"%s":["%s","%s"]}}'%(host, port, start_timestamp, start_timestamp_cur)})
        des_info += str(port) + 'at' + str(start_timestamp) + ' '
    # 打快照
    instance_id = ali_ecs_snap.get_instanceid([ip])
    disk_ids = ali_ecs_snap.get_disk_ids(instance_id)
    for disk_id in disk_ids:
        snap_name = '{host}-{start_timestamp}'.format(host=host, start_timestamp=datetime.now().strftime('%Y-%m-%d'))
        description = 'mongo full backup' + des_info
        snap_response = ali_ecs_snap.create_snapshot(disk_id, snap_name, tags, description)
        print(snap_response)

    #解锁实例 # 整备与增量备份连接
    for port in port_lst:
        ssh.execute_cmd("echo 'db.fsyncUnlock();' | mongo --port {port} admin -u {user} -p {password}".format(port=port, user=settings.db_user, password=settings.db_password))


def get_wholebak_infos():
    wholebak_infos = {}
    for db_info in settings.dbs_info:
        if db_info.get('host') not in wholebak_infos.keys():
            wholebak_infos[db_info.get('host')] = { 'ip': db_info.get('ip'), 'port': [db_info.get('port')] }
        else:
            wholebak_infos[db_info.get('host')]['port'].append(db_info.get('port'))
    return wholebak_infos

if __name__ == '__main__':
    full_db_backup()