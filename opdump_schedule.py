import schedule, time, threading, settings, re, os, sys
from tools import ossTools
from tools import sshTools
from os import listdir
from datetime import datetime

UPLOADING = 0
TIMESTAMP = int(time.time())
CURSOR = TIMESTAMP

def get_cursor():
    with open(settings.cur_file, 'r') as f:
        cursor = f.read()
        if not cursor.isdigit():
            pass
            #logger
            exit(404)
        else:
            return int(cursor)

def put_cursor(cursor):
    with open(settings.cur_file, 'r+') as f:
       f.write(str(cursor))

def oplog_dump():
    cursor = get_cursor()
    start_time = cursor
    end_time = cursor + settings.dumpop_interval
    for db_info in settings.dbs_info:
        host = db_info.get('host')
        ip = db_info.get('ip')
        port = db_info.get('port')
        local_store = settings.local_store
        temp_file_name = '{local_store}{db_name}_{port}_{start_time}_{end_time}_temp'.format(local_store=local_store,db_name=host, port=port, start_time=start_time, end_time=end_time)
        file_name = '{local_store}{db_name}_{port}_{start_time}_{end_time}'.format(local_store=local_store,db_name=host, port=port, start_time=start_time, end_time=end_time)
        cmd = '''mongodump --port {port} -d local -c oplog.rs -q '{{"ts": {{$gt:Timestamp({start_time}, 1),$lt:Timestamp({end_time}, 1)}}}}' -o {file_name} '''.format(host=host, ip=ip, port=port, start_time=start_time, end_time=end_time, file_name=temp_file_name)
        print(cmd)
        job_thread = threading.Thread(target=op_dump_exec, args=(ip, cmd, file_name, temp_file_name))
        job_thread.start()

    put_cursor(end_time)


def op_dump_exec(ip, cmd, file_name, temp_file_name):
    ssh = sshTools(ip)
    status = ssh.execute_cmd(cmd)
    if status == 0:
        status = ssh.execute_cmd('mv {temp_file_name} {file_name}'.format(temp_file_name=temp_file_name, file_name=file_name))
        if status != 0:
            pass
            #logger
    else:
        pass
        #logger
    ssh.ssh.close()


def oas_upload():
    global UPLOADING
    if UPLOADING == 1:
        pass
    else:
        UPLOADING = 1
        file_list = listdir(settings.local_store)
        for file in file_list:
            print(file)
            res = re.match('^.*_\d{10}_\d{10}$', file)
            if res:
                match_obj = re.match('([a-zA-Z-0-9]+)_\d+_\d{10}_\d{10}', file)
                if not match_obj:
                    print('not match')
                    continue
                    #logger
                else:
                    host = match_obj.group(1)
                oss = ossTools(settings.access_key_id, settings.access_key_secret)
                remote_file = datetime.now().strftime('%Y/%m/%d/') + host +'/'+ file
                local_file = settings.local_store + file + '/local/oplog.rs.bson'
                res = oss.multi_upload_obj(remote_file, local_file)
                print(res,host)
                if not res:
                    for restore_file in listdir(settings.local_store + file + '/local/'):
                        os.remove(settings.local_store + file + '/local/' + restore_file)
                    os.rmdir(settings.local_store + file + '/local')
                    os.rmdir(settings.local_store + file)

        UPLOADING = 0


def logger():
    pass

def main():
    if os.path.exists(settings.cur_file):
        print('exits')
        with open(settings.cur_file, 'r') as f:
            cursor = f.read()
        if not cursor.isdigit() or not re.match('^\d{10}$',cursor):
            #logger
            print('not digit')
            with open(settings.cur_file, 'w') as f:
                cursor = int(time.time())
                f.write(str(cursor))

    else:
        print('new')
        with open(settings.cur_file, 'w') as f:
            cursor = int(time.time())
            f.write(str(cursor))

    schedule.every(settings.dumpop_interval).seconds.do(oplog_dump)
    schedule.every(settings.upload_interval).seconds.do(oas_upload)
    # schedule.every(1200).minutes.do(oas_upload)
    while True:
        sys.stdout.write("======check schedule======")
        schedule.run_pending()
        time.sleep(settings.sche_sleep)

if __name__ == '__main__':

    schedule.every(settings.dumpop_interval).seconds.do(oplog_dump)
    schedule.every(settings.upload_interval).seconds.do(oas_upload)

    schedule.run_all()
    while True:
        print("======check schedule======")
        sys.stdout.write("======check schedule======")
        schedule.run_pending()
        time.sleep(settings.sche_sleep)
