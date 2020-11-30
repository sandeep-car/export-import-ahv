# DISCLAIMER: This script is not supported by Nutanix. Please contact
# Sandeep Cariapa (lastname@gmail.com) if you have any questions.
import os
import re
import csv
import sys
import json
import socket
import requests
import paramiko
import subprocess
from pprint import pprint
from urllib.parse import quote

# Variables used by the export script which is run on the source cluster.

# This is where we store VM config files. Note that this is used by the export and import script.
DIR="/root/source/export-import/output"

# Maximum number of jobs you can run on a CVM. This is used by the export script and the
# import SFTP script to regulate qemu-img convert jobs on the CVM.
# No real reason to change this unless your CVMs are too busy.
MAX_CVM_JOBS=6

# Destination container on the source. Qcow2 files will be placed here by AHV.
EXPORTCONTAINER = "exportcontainer"

# Set to "-c" if you want to compress the qcow2 files. Leave as an empty string otherwise.
# Enabling this option increases CPU on your CVM, however results in smaller files to download.
# Use with care in a production cluster.
COMPRESS=""
#COMPRESS="-c"

# Enabled by default. This increases buffer sizes for faster file transfers.
# Disable if your network is very busy and large packet size may result in excessive re-transmits.
# Otherwise there is really no need to change this.
large_file_opt=True
#large_file_opt=False

# Source AHV cluster details. We need these in order to log into the REST API.
src_cluster_ip = "10.254.254.254"
src_cluster_admin = "restapiuser"
src_cluster_pwd = "blahblah"

# CVM password on source AHV cluster. 
src_cvm_pwd = "blahblah"

# Variables used by the import script which is run on the destination cluster.

# Destination containers.
# This is the container where you upload your qcow2 files via SFTP.
SFTPCONTAINER = "sftpcontainer"

# Destination network name. This is the network to which VMs will be assigned.
# You can go to Prism-> Network -> Virtual Networks and get this information.
MYNETWORK = "server lan" 

# Drives with these parameters on the source will be considered boot devices on the destination.
# The script takes these parameters and changes these to scsi:0, because POST /vms seems to require
# the boot drive being that way.
BOOT_DEVICE_BUS="scsi"
BOOT_DEVICE_INDEX="0"

# Destination AHV cluster details. We need these in order to log into the REST API.
dst_cluster_ip = "10.254.254.254"
dst_cluster_admin = "restapiuser"
dst_cluster_pwd = "blahblah"

# CVM password on destination AHV cluster. 
dst_cvm_pwd = "blahblah"

# Suffix for VMS. Only used while testing.
# In production, this string should be empty. i.e.:
# VM_SUFFIX=""
VM_SUFFIX="_TEST-1103_1200"

# ========== DO NOT CHANGE ANYTHING UNDER THIS LINE =====
class my_api():
    def __init__(self,ip,username,password):

        # Cluster IP, username, password.
        self.ip_addr = ip
        self.username = username
        self.password = password
        # Base URL at which v0.8 REST services are hosted in Prism Gateway.
        base_urlv08 = 'https://%s:9440/PrismGateway/services/rest/v0.8/'
        self.base_urlv08 = base_urlv08 % self.ip_addr
        self.sessionv08 = self.get_server_session(self.username, self.password)
        # Base URL at which v1 REST services are hosted in Prism Gateway.
        base_urlv1 = 'https://%s:9440/PrismGateway/services/rest/v1/'
        self.base_urlv1 = base_urlv1 % self.ip_addr
        self.sessionv1 = self.get_server_session(self.username, self.password)
        # Base URL at which v2 REST services are hosted in Prism Gateway.
        base_urlv2 = 'https://%s:9440/PrismGateway/services/rest/v2.0/'
        self.base_urlv2 = base_urlv2 % self.ip_addr
        self.sessionv2 = self.get_server_session(self.username, self.password)
        
    def get_server_session(self, username, password):
          
        # Creating REST client session for server connection, after globally
        # setting authorization, content type, and character set.
        session = requests.Session()
        session.auth = (username, password)
        session.verify = False
        session.headers.update({'Content-Type': 'application/json; charset=utf-8'})
        return session
       
    # Get cluster information.
    def get_cluster_information(self):
        
        cluster_url = self.base_urlv2 + "cluster/"
        print("Getting cluster information for cluster", self.ip_addr)
        try:
            server_response = self.sessionv2.get(cluster_url)
            return server_response.status_code ,json.loads(server_response.text)
        except Exception as ex:
            print(ex)
            return -1,cluster_url

    # Test that we can connect to the port on the given IP address.    
    def test_port(self,ip,port):
        
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect((ip, int(port)))
            return True
        except Exception as e:
            print(e)
            return False
    
    # Ssh into the CVM.
    def ssh_cmd(self,cvm_ip,pwd,filename,nfs_path):
        
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()        
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(cvm_ip, username="nutanix", password=pwd)
        except Exception as ex:
            print("Could not connect to:",cvm_ip)
            print(ex)
            sys.exit(1)
        
        # Run this on the source cluster.
        if (nfs_path != None):
            cmd = "/usr/local/nutanix/bin/qemu-img convert " + COMPRESS + " -f raw nfs://127.0.0.1" + nfs_path + " -O qcow2 nfs://127.0.0.1/" + EXPORTCONTAINER + "/" + filename + " &"
        # Run this on the destination cluster.
        else:
            dst_filename = re.sub(".qcow2", ".raw", filename)
            cmd = "/usr/local/nutanix/bin/qemu-img convert -f qcow2 nfs://127.0.0.1/" + SFTPCONTAINER + "/" + filename + " -O raw nfs://127.0.0.1/" + SFTPCONTAINER + "/" + dst_filename + " &"
        
        print("IN SSH CMD:",cmd)
        # These return values are useless because we CMD runs in the background.
        # If we need to debug, we can remove the "&" at the end of CMD.
        stdin, stdout, stderr = ssh.exec_command(cmd)
        return(stdin,stdout,stderr)

    # Take the CSV filename. Return VM Names in it.
    def get_important_vms(self,csvfile):
        
        important_vms=[]
        with open(csvfile) as csvfp:
            csv_reader = csv.reader(csvfp, delimiter=',')
            for row in csv_reader:
                # Skip empty lines.
                if (len(row) == 0):
                    continue
                important_vms.append(row[0])
        return important_vms

    # Get storage container information.
    def get_storage_container_info(self):
        
        cluster_url = self.base_urlv2 + "/storage_containers/"
        print("Getting storage container info")
        server_response = self.sessionv2.get(cluster_url)
        print("Response code: ",server_response.status_code)
        return server_response.status_code ,json.loads(server_response.text)
        
    # We have to use the v1 API because thats the only way we can figure out if
    # its a controller VM.
    # Return a list of CVM IPs.
    def get_cvms(self):
        cluster_url = self.base_urlv1 + "vms/"
        server_response = self.sessionv1.get(cluster_url)
        
        all_vms = json.loads(server_response.text)
        all_vms_list = all_vms["entities"]
        cvm_list=[]
        for v in all_vms_list:
            if (v["controllerVm"] == True):
                cvm_list.append(v["ipAddresses"][0])
        # print("Response code: ",server_response.status_code)
        return cvm_list

    # SSH into a CVM and return the number of qemu-img convert jobs that are running.
    def check_jobs(self,cvm_ip,pwd):
        
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            ssh.connect(cvm_ip, username="nutanix", password=pwd)
        except Exception as ex:
            print("Could not connect to:",cvm_ip)
            print(ex)
            sys.exit(1)

        cmd = "ps -elf | grep qemu-img | grep -v grep"

        # print("IN CHECK JOBS CMD:",cmd)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        mystr = stdout.read()
        nlines = mystr.count(bytes('\n','utf-8'))
        
        return nlines

    # Get network info so we get new network UUID.
    def get_network_info(self):
    
        cluster_url = self.base_urlv2 + "/networks/"
        print("Getting network info")
        server_response = self.sessionv2.get(cluster_url)
        print("Response code: %s" % server_response.status_code)
        return server_response.status_code ,json.loads(server_response.text)
    
    # Power on VM with this UUID.
    def power_on_vm(self, vmid):
        
        print("Powering on VM: %s." % vmid)
        cluster_url = self.base_urlv2 + "vms/" + str(quote(vmid)) + "/set_power_state/"
        vm_power_post = {"transition":"ON"}
        server_response = self.sessionv2.post(cluster_url, data=json.dumps(vm_power_post))
        # print("Response code: %s" % server_response.status_code)
        return server_response.status_code ,json.loads(server_response.text)
    
    
    # Take the list of files in DIR and return names and UUIDs from config files.
    def get_vmnameanduuid(self,files):
        
        uuid_regex = "[a-z0-9-]+"
        vmname_byuuid = {}
    
        for f in files:
            # If its a config file.
            cfg_regex = "(" + uuid_regex + ").cfg"
            matchObj = re.match(cfg_regex,f)
            if matchObj:
                # print ("matchobj group(0) %s" % matchObj.group(0))
                vm_uuid = matchObj.group(1)
                
                # If the config file looks like a UUID, read it.
                vmcfg_fp = open(DIR + "/" + vm_uuid + ".cfg", "r")
                vm_json = vmcfg_fp.read()
                vmcfg_fp.close()
                
                name_regex = '"name": "([^"]+)"'
                matchObj1 = re.search(name_regex,vm_json)
                if matchObj1:
                    vm_name = matchObj1.group(1)
                else:
                    print ("Could not find %s in %s. This should not happen." %(vm_uuid,f))
                    sys.exit(1)
                vmname_byuuid[vm_uuid] = vm_name
        return vmname_byuuid

    # Sftp into container ls file. Return values:
    # -1: The file doesn't exist.
    # -2: Permission denied. Happens sometimes, possible SFTP server bug.
    # -100: Some other goofy error that we don't know yet.
    # Anything else: size of file.
    def sftp_ls(self,user,pwd,filepath):

        #user = src_cluster_admin + "@" + src_cluster_ip
        #pwd = "-p" + src_cluster_pwd
        #print("IN SFTP_LS: user: ", user)

        cmd_lst = ['sshpass', pwd, 'sftp', '-P', '2222', '-o', 'StrictHostKeyChecking=no', user]
        ls_str = "ls -l " + filepath + "\n"
        spls = subprocess.Popen(cmd_lst, text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        spls.stdin.write(ls_str)
        try:
            size_str,err = spls.communicate()
            size_lst = size_str.split()
            #pprint(size_lst)
            #print("Length of size_lst: ", len(size_lst))
            #print("err: %s. %s" %(err, type(err)))
            
            cantls_regex = "Can't ls:"
            searchObj1 = re.search(cantls_regex,err)
            # If we found "Can't ls:" in stderr, then the file doesn't exist.
            if searchObj1:
                return -1

            perms_regex = "Permission denied"
            searchObj2 = re.search(perms_regex,err)
            if searchObj2:
                return -2

            # If we're here and the list doesn't have > 8 members, we have a problem.
            if len(size_lst) > 8:
                size = int(size_lst[8])
            else:
                return -100
        # If we fail here, its because of some other goofy error with sftp.
        except Exception as ex:
            print(ex)
            return -100

        #print("LEAVING SFTP_LS", size, filepath)
        return size

