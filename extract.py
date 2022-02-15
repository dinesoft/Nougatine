import json

import requests
import csv
import paramiko
from datetime import date, timedelta


def daily_request():
    yesterday = date.today() - timedelta(days=1)

    # Request
    headers = {
        'accept': '*/*',
    }

    params = (
        # ('limit', "1"),
        ('where', 't_1h >=  \'{}\''.format(yesterday)),
        ('offset', '0'),
        ('timezone', 'UTC'),
    )


    # response = requests.get(
    #     'https://opendata.paris.fr/api/v2/catalog/datasets/comptages-routiers-permanents/exports/csv', headers=headers,
    #     params=params)

    # data = response.content.decode('utf8')

    # # Save file
    # with open("data/raw/raw_data.csv", "w") as fo:
    #     fo.write(data)

    params = (
    ('op', 'CREATE')
    )

    with open("data/raw/raw_data.csv", "r") as file:
        data = csv.reader(file, delimiter=';', quotechar='|')
        # response = requests.put('root@9fdf093d-8597-44b4-b24c-72ad06f4da03.pub.instances.scw.cloud:/tmp/raw_data.csv', 
        #             params=params, 
        #             data=data)

        import subprocess

        ssh_key_filename = "C:\\Users\\khali\\Documents\\ESGI\\M2\\Auto-infra\\iabd"
        source_file =  "C:\\Users\\khali\\Documents\\ESGI\\M2\\Auto-infra\\Nougatine\\data\\raw\\raw_data.csv" 
        hdfs_point = "root@9fdf093d-8597-44b4-b24c-72ad06f4da03.pub.instances.scw.cloud:/tmp/raw_data.csv"

        process = subprocess.Popen(['scp', '-i', ssh_key_filename, source_file, hdfs_point],
                            stdout=subprocess.PIPE, 
                            stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        print(stdout)

        
        hostname = '9fdf093d-8597-44b4-b24c-72ad06f4da03.pub.instances.scw.cloud' 
        myuser   = 'root'
        mySSHK   = 'C:\\Users\\khali\\Documents\\ESGI\\M2\\Auto-infra\\iabd'
        sshcon   = paramiko.SSHClient()  # will create the object
        sshcon.set_missing_host_key_policy(paramiko.AutoAddPolicy()) # no known_hosts error
        sshcon.connect(hostname, username=myuser, key_filename=mySSHK)

        print("copy to hdfs")

        command = "hdfs dfs -put /tmp/raw_data.csv /data/g3/data"
        stdin, stdout, stderr = sshcon.exec_command(command)
        lines = stdout.readlines()
        sshcon.close()





if __name__ == '__main__':
    daily_request()