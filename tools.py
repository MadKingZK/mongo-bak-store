# -*- coding: utf-8 -*-
import oss2
import paramiko
import settings
from itertools import islice


class ossTools():
    def __init__(self, access_key_id, access_key_secret):
        # 阿里云主账号AccessKey拥有所有API的访问权限，风险很高。强烈建议您创建并使用RAM账号进行API访问或日常运维，请登录 https://ram.console.aliyun.com 创建RAM账号。
        __auth = oss2.Auth(access_key_id, access_key_secret)
        # Endpoint以杭州为例，其它Region请按实际情况填写。
        self._bucket = oss2.Bucket(__auth, settings.oss_endpoint, settings.oss_bucket)


    #遍历文件
    def list_obj(self):
        for b in islice(oss2.ObjectIterator(self._bucket), 10):
            print(b.key)

    def search_obj(self, filter_str):
        for obj in oss2.ObjectIterator(self._bucket, prefix=filter_str):
            print(obj.key)

    #简单上传文件
    def upload_obj(self, remote_file, local_file):
        self._bucket.put_object_from_file(remote_file, local_file)


    #分片并发上传
    def multi_upload_obj(self, remote_file, local_file):
        oss2.resumable_upload(self._bucket, remote_file, local_file,
                              store=oss2.ResumableStore(root='/tmp'), #指定保存断点信息的目录
                              multipart_threshold=100 * 1024, #文件长度大于该值时，则用分片上传
                              part_size=100 * 1024, #分片大小
                              num_threads=4) #并发上传的线程数

    #下载文件
    def download_obj(self, remote_file, local_file):
        self._bucket.get_object_to_file(remote_file, local_file)


    def restore_obj(self, remote_file):
        self._bucket.restore_object(remote_file)


class sshTools(object):
    def __init__(self, host):
        __private_key = paramiko.RSAKey.from_private_key_file(settings.key_file)

        # 创建SSH对象
        self.ssh = paramiko.SSHClient()
        # 允许连接不在know_hosts文件中的主机
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # 连接服务器
        self.ssh.connect(hostname=host, port=22, username=settings.ssh_user, pkey=__private_key)

    def execute_cmd(self, cmd):
        # 执行命令
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        # 获取命令结果
        channel = stdout.channel
        status = channel.recv_exit_status()
        return status

class ansibleTools(object):
    pass

if __name__ == '__main__':
    oss = ossTools(settings.access_key_id, settings.access_key_secret)
    res = oss.multi_upload_obj('test/ossbook.pdf', '/Users/harvey/Downloads/ossbook.pdf')
    #res = oss.search_obj()
    # oss.restore_obj()
    # res = oss.download_obj()
    print(res)
