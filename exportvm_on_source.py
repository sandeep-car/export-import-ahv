#!/usr/local/bin/python3.7
#
# DISCLAIMER: This script is not supported by Nutanix. Please contact
# Sandeep Cariapa (lastname@gmail.com) if you have any questions.
# NOTE:
# 1. You need a Python library called "requests" which is available from
# the url: http://docs.python-requests.org/en/latest/user/install/#install
# For reference look at:
# https://github.com/nutanix/Connection-viaREST/blob/master/nutanix-rest-api-v2-script.py
# https://github.com/nelsonad77/acropolis-api-examples

import os
import re
import sys
import json
import time
import argparse
import requests
import threading
import subprocess
import clusterconfig as C
from pprint import pprint
from requests.packages.urllib3.exceptions import InsecureRequestWarning


# Call subprocess to fire up sftp. Would have been nice to use paramiko for file transfer
# it wasn't for https://github.com/paramiko/paramiko/issues/822 which causes rekeys and 
# file transfer terminations. It times out over SFTP also.
# 1. Get file size of srcfilepath.
# 2. Construct list to pass to subprocess.Popen().
# 3. Start sftp in a thread. Its in a thread so we can display download information. (X % in Y seconds etc)
# 4. Sleep until complete, printing out progress every 5 seconds.
def sftp_download(filename, vm_name):

    user = C.src_cluster_admin + "@" + C.src_cluster_ip 
    pwd = "-p" + C.src_cluster_pwd

    def run_sftp(srcfilepath,dstfilepath):

        try:
            retr_user = user + ":" + srcfilepath
            # If we don't turn off StrictHostKeyChecking, sftp fails with Host key verification error.
            # We need the while True loop because every now and then we get Permission denied errors 
            # from the sftp server.
            error_count=0
            while True:
                if C.large_file_opt == True:
                    sp = subprocess.Popen(['sshpass', pwd, 'sftp', '-B', '131072', '-P', '2222', \
                                             '-o', 'StrictHostKeyChecking=no', retr_user, dstfilepath], \
                                            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                else:
                    sp = subprocess.Popen(['sshpass', pwd, 'sftp', '-P', '2222', '-o', \
                                             'StrictHostKeyChecking=no', retr_user, dstfilepath], \
                                            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

                out,err = sp.communicate()
                print("Popen out from sftp: ", out)
                print("Popen err: ", err)

                perms_regex = "Permission denied"
                searchObj = re.search(perms_regex,err)
                if searchObj:
                    if error_count == 3:
                        print(">>> Giving up after %d attempts. <<<" % error_count)
                        sys.exit(1)
                    error_count = error_count + 1
                    print("Sftp pipe: Permission denied..sleeping and trying again. %d." % error_count)
                    time.sleep(5)
                else:
                    break
        except Exception as ex:
            print("Subprocess failed while downloading %s." % srcfilepath)
            pprint(ex)
            sys.exit(1)
        return
    
    srcfilepath = "/" + C.EXPORTCONTAINER + "/" + filename
    dstfilepath = C.DIR + "/" + filename

    error_count=0
    while True:
        srcfilesize = mycluster.sftp_ls(user,pwd,srcfilepath)
        # If sftp_ls returned zero or greater break, otherwise deal.
        if srcfilesize >= 0:
            break
        elif srcfilesize == -1:
            print("Sftp_ls could not stat %s on cluster." % srcfilepath)
            print(">>> Did you run this script with --qemu to create it first? <<<")
            sys.exit(1)
        elif srcfilesize == -2:
            error_count = error_count + 1
            print("Sftp_ls: Permission denied..sleeping and trying again. %d." % error_count)
            time.sleep(5)
        # Some other weird error from sftp. Increase error count so we can sleep and try again..
        else:
            error_count = error_count + 1
            print("Sftp_ls: Unknown error..sleeping and trying again. %d." % error_count)
            time.sleep(5)
        if error_count == 3:
            print(">>> Giving up after %d attempts. <<<" % error_count)
            sys.exit(1)
    
    print ("Starting download..hang on..")
    start_time = round(time.time())
    t=threading.Thread(target=run_sftp,args=(srcfilepath,dstfilepath))
    t.start()
    time.sleep(1)

    while t.is_alive():
        try:
            dstfilesize = os.stat(dstfilepath).st_size
        # An exception here is most likely if os.stat failed because the download didn't begin yet.
        except:
            dstfilesize = 0
            time.sleep(5)
        cur_time = round(time.time())
        runtime = cur_time - start_time
        print(srcfilepath, "for", vm_name, "downloaded: %0.2f%%. Run time: %d seconds." \
              %(((dstfilesize / srcfilesize) * 100), runtime))
        time.sleep(5)

    # How long did it take for 100% of the file to transfer over?
    try:
        if (dstfilesize / srcfilesize) != 1:
            dstfilesize = os.stat(dstfilepath).st_size
            cur_time = round(time.time())
            runtime = cur_time - start_time
            print(srcfilepath, "for", vm_name, "downloaded: %0.2f%%. Run time: %d seconds." \
                  %(((dstfilesize / srcfilesize) * 100), runtime))
    # If dstfilesize isn't defined then the thread never got started.
    except NameError:
        print(">>> Could not start sftp. Does it work from the command-line? <<<")
        print("sftp -P 2222 -o StrictHostKeyChecking=no ", user)
        sys.exit(1)
    
    return

# Get list of all VMs.
def get_all_vm_info(mycluster):
    
    cluster_url = mycluster.base_urlv2 + \
                  "vms/?include_vm_disk_config=true&include_vm_nic_config=true"
    server_response = mycluster.sessionv2.get(cluster_url)
    # print("Response code: ",server_response.status_code)
    return server_response.status_code, json.loads(server_response.text)
    
# Get virtual disk information.
def get_vdisk_info(mycluster, vmdisk_uuid):
    
    cluster_url = mycluster.base_urlv2 + "/virtual_disks/" + vmdisk_uuid
    server_response = mycluster.sessionv2.get(cluster_url)
    # print("Response code: ",server_response.status_code)
    return server_response.status_code, json.loads(server_response.text)

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--qemu", action='store_true', help="Run qemu-img convert on vdisks. (default is no)")
        parser.add_argument("csvfile", type=str, help="CSV File with VM names")
        args = parser.parse_args()

        csvfile = args.csvfile

        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        mycluster = C.my_api(C.src_cluster_ip, C.src_cluster_admin, C.src_cluster_pwd)
        status, cluster = mycluster.get_cluster_information()
        if status != 200:
            print("Cannot connect to: %s" % cluster)
            print(">>> Did you remember to update the config file? <<<")
            sys.exit(1)
        
        # Displays cluster authentication response and information.
        # print("Status code: %s" % status)
        # print("Text: ")
        # print(json.dumps(cluster,indent=2))
        # print("=")
        
        # Get specific cluster elements.
        print("Name: %s." % cluster["name"])
        print("ID: %s." % cluster["id"])
        print("Cluster Ext IP Address: %s." % cluster["cluster_external_ipaddress"])
        print("Number of Nodes: %s." % cluster["num_nodes"])
        print("Version: %s." % cluster["version"])
        
        # If we can't connect to this port then we can't sftp.
        # Might as well error out now.
        if mycluster.test_port(C.src_cluster_ip, 2222) == False:
            print("Cannot connect to port 2222 on %s. We won't be able to sftp files out." \
                  % C.src_cluster_ip)
            print("Did you remember to run the following command on any one of the CVMs?")
            print("'allssh modify_firewall -f -o open -i eth0 -p 2222 -a'")
            sys.exit(1)
        
        # Check if the export container has been created. Should we create it here?
        status, resp = mycluster.get_storage_container_info()
        all_containers = resp["entities"]
        
        for container in all_containers:
            if container["name"] == C.EXPORTCONTAINER:
                storage_container_uuid = container["storage_container_uuid"]
        try:
            print("Container: %s. UUID: %s" % (C.EXPORTCONTAINER, storage_container_uuid))
        except NameError:
            print(">>> Cannot proceed. Have you created '%s' on your source cluster? <<<" \
                   % C.EXPORTCONTAINER)
            sys.exit(1)
        
        # Get VM list of VMs that actually matter.
        important_vms = mycluster.get_important_vms(csvfile)
        # pprint(important_vms)
        
        # Get information about all VMS.
        status, all_vms = get_all_vm_info(mycluster)
        all_vms_list = all_vms["entities"]
        # pprint(all_vms_list)
        
        # nfsfile_list[] is a list of dictionaries. 
        # Each dict has key = vmuuid, value is nfs file path.
        # By the end of this loop, this dictionary will have all the information neccessary to
        # convert and download the files.
        nfsfile_list = []
        # Get VM info for each VM.
        for vm_dict in all_vms_list:

            # If the VM is not powered off, no reason to move forward.
            if vm_dict["power_state"] != "off":
                continue
            
            vm_name = vm_dict["name"]
            vm_uuid = vm_dict["uuid"]
            # If the VM is not an important VM, then continue.
            if vm_name not in important_vms:
                continue
            
            print("*** NAME: %s." % vm_dict["name"])
            print("*** UUID: %s." % vm_dict["uuid"])
            print("*** VCPUS: %s." % vm_dict["num_vcpus"])
        
            # Write into its own config file. We use UUIDs instead of VM name, because
            # VM names can contain spaces, () and possibly unicode characters which shell 
            # may not handle properly.
            vm_json = json.dumps(vm_dict)
            # pprint(vm_json)

            try:
                f = open(C.DIR + "/" + vm_uuid + ".cfg", "w")
            except:
                print("Cannot write to", C.DIR)
                print(">>> Did you remember to update the config file? <<<")
                sys.exit(1)
            
            f.write(vm_json)
            f.close()
            
            # Get vdisk information.            
            for vm_disk_dict in vm_dict["vm_disk_info"]:
                # print ("Entering vm_disk_dict loop: for %s at %s" \
                # % (vm_name, time.strftime("%H:%M:%S")))
                # pprint(vm_disk_dict)
                if vm_disk_dict["is_cdrom"]:
                    continue
                
                disk_label = vm_disk_dict["disk_address"]["disk_label"]
                vmdisk_uuid = vm_disk_dict["disk_address"]["vmdisk_uuid"]
                # print "FFFF FOUND VM_DISK_UUID", vmdisk_uuid, " ", disk_label
                
                status, vdisk_info = get_vdisk_info(mycluster, vmdisk_uuid)
                # print "VVVVVVVVVVVVV VDISK INFO"
                # pprint(vdisk_info)
                l = []
                l = [vm_uuid, vdisk_info["nutanix_nfsfile_path"], disk_label, vm_name]
                nfsfile_list.append(l)
                print("*** VMDISK_UUID: %s NFS PATH : %s" \
                      % (vmdisk_uuid, vdisk_info["nutanix_nfsfile_path"]))

        # At this point, all the vdisks we want to process and download are in nfsfile_list.
        # Get a list of our CVMs and distribute tasks amongst them.
        if args.qemu:
            cvm_ip_list = mycluster.get_cvms()
            
            i = 0
            j = 0
            while i < len(nfsfile_list):
                l = nfsfile_list[i]
                vm_uuid = l[0]
                nfs_path = l[1]
                disk_label = l[2]
                spawned = False
                print("Entering loop: i %s. j %s VM_UUID: %s. NFS PATH: %s. disk_label: %s" \
                      % (i, j, vm_uuid, nfs_path, disk_label))
                cvm_ip = cvm_ip_list[j]
            
                # Spawn off first file on CVM1, second on CVM2, etc
                # If the number of jobs on CVM <= C.MAX_CVM_JOBS, then spawn off a new job.
                numjobs = mycluster.check_jobs(cvm_ip,C.src_cvm_pwd)
                if numjobs <= C.MAX_CVM_JOBS:
                    print("*********")
                    print("Submitting: %s on %s for conversion. Index: %d" % (nfs_path, cvm_ip, i))
                    filename = vm_uuid + "_" + disk_label + ".qcow2"
                    mycluster.ssh_cmd(cvm_ip,C.src_cvm_pwd,filename,nfs_path)
                
                    # Sleep for a few seconds to give ssh a chance to fire up before we check.
                    time.sleep(5)
                    spawned = True
                    
                # If we are here, then we either spawned off a job, or skipped
                # because we reached C.MAX_CVM_JOBS. Either way, move to the next CVM.
                j += 1
                if j == len(cvm_ip_list):
                    j = 0
                # If we were able to spawn off a job, then move on to the next nfs file.
                if spawned == True:
                    i += 1
            
            # End while loop.
            # Qemu-img jobs are now running on all CVMs. Loop here and keep checking that they
            # are complete.
            runtime = 0
            while True:
                total_jobs = 0
                for cvm_ip in cvm_ip_list:
                    # print "CVM_IP: ", cvm_ip
                    total_jobs += mycluster.check_jobs(cvm_ip,C.src_cvm_pwd)
                if total_jobs > 0:
                    print("%s conversion jobs are still running. Sleeping...(%s seconds)" \
                          % (total_jobs, runtime))
                    time.sleep(5)
                    runtime += 5
                else:
                    break
            # End while loop.
        # End if args.qemu
        # Download all files at once. We could spawn off multiple download jobs for efficiency.
        # If we did that however we'd need to make sure the removeable drive wasn't swamped etc.
        # Without knowing whether the removeable is Flash or HDD, this becomes tricky.
        i = 0
        while i < len(nfsfile_list):
            l = nfsfile_list[i]
            vm_uuid = l[0]
            nfs_path = l[1]
            disk_label = l[2]
            vm_name = l[3]
            
            filename = vm_uuid + "_" + disk_label + ".qcow2"
            
            print("STARTING SFTP DOWNLOAD")
            sftp_download(filename, vm_name)
            i += 1
        
        print("=")
        print("*COMPLETE*")

    except Exception as ex:
        print(ex)
        sys.exit(1)
