#!/usr/local/bin/python3.7
#
# DISCLAIMER: This script is not supported by Nutanix. Please contact
# Sandeep Cariapa (firstname.lastname@nutanix.com) if you have any questions.
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
import uuid
import argparse
import requests
import threading
import subprocess
import collections
import clusterconfig as C
from pprint import pprint
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Call subprocess to fire up sftp. Would have been nice to use paramiko for file transfer
# if it wasn't for https://github.com/paramiko/paramiko/issues/822 which causes rekeys and
# file transfer terminations. It times out over SFTP also.
# 1. Get file size of srcfile prior to transfer.
# 2. Construct list to pass to subprocess.Popen().
# 3. Start sftp in a thread. Its a thread so we can display upload information. (X % in Y seconds etc)
# 4. Sleep until complete, printing out progress every 5 seconds.
def sftp_upload(filename, vm_name):

    user = C.dst_cluster_admin + "@" + C.dst_cluster_ip
    pwd = "-p" + C.dst_cluster_pwd

    def run_sftp(srcfilepath,dstfilepath):

        put_str = "put " + srcfilepath + " " + dstfilepath + "\nchmod 644 " + dstfilepath + "\n\n"
        # print("User: %s Put_str: %s FileSize: %s" % (user,put_str,srcfilesize))
        try:
            # If we don't turn off StrictHostKeyChecking, sftp fails with Host key verification error.
            # We need the while True loop because every now and then we get Permission denied errors
            # from the sftp server.
            error_count=0
            while True:
                if C.large_file_opt == True:
                    cmd_lst = ['sshpass', pwd, 'sftp', '-B', '131072', '-P', '2222', '-o', 'StrictHostKeyChecking=no', user]
                else:
                    cmd_lst = ['sshpass', pwd, 'sftp', '-P', '2222', '-o', 'StrictHostKeyChecking=no', user]
                sp = subprocess.Popen(cmd_lst, text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, \
                                      stderr=subprocess.PIPE)
                out,err = sp.communicate(input=put_str)
                
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
            print("Subprocess failed while uploading %s." % srcfilepath)
            print(ex)
            sys.exit(1)
        return
    
    srcfilepath = C.DIR + "/" + filename
    dstfilepath = "/" + C.SFTPCONTAINER + "/" + filename
    srcfilesize = os.stat(srcfilepath).st_size

    print ("Starting upload..hang on..")
    start_time = round(time.time())
    t=threading.Thread(target=run_sftp,args=(srcfilepath,dstfilepath))
    t.start()
    time.sleep(1)
    
    while t.is_alive():
        error_count=0
        while True:
            dstfilesize = mycluster.sftp_ls(user,pwd,dstfilepath)
            # If sftp_ls returned zero or greater break, otherwise deal.
            if dstfilesize >= 0:
                break
            # Maybe the upload didn't start yet?
            elif dstfilesize == -1:
                error_count = error_count + 1
                print("Sftp_ls Could not stat %s..sleeping and trying again. %d." %(dstfilepath,error_count))
                time.sleep(5)
            # The sftp server denies permission if its overwhelmed. Sleep and try again.
            elif dstfilesize == -2:
                error_count = error_count + 1
                print("Sftp_ls: Permission denied..sleeping and trying again. %d." % error_count)
                time.sleep(5)
            # Some other weird error from sftp. Increase error count so we can sleep and try again..
            else:
                error_count = error_count + 1
                print("Sftp_ls: Unknown error from sftp_ls..sleeping and trying again. %d." % error_count)
                time.sleep(5)
            if error_count == 5:
                print("Check from the command line if upload is progressing..")
                print(">>> Sftp_ls failure after %d attempts. <<<" % error_count)
                time.sleep(5)
        
        cur_time = round(time.time())
        runtime = cur_time - start_time
        print(srcfilepath, "for", vm_name, "uploaded: %0.2f%%. Run time: %d seconds." \
              %(((dstfilesize / srcfilesize) * 100), runtime))
        time.sleep(5)

    return
    
# Return a dictionary with all vdisks in our storage container.
def get_vdisks(mycluster,storage_container_uuid):
    
    cluster_url = mycluster.base_urlv2 + "/storage_containers/" + storage_container_uuid + "/vdisks"
    print("Getting vdisk info")
    server_response = mycluster.sessionv2.get(cluster_url)
    print("Response code: %s" % server_response.status_code)
    return server_response.status_code ,json.loads(server_response.text)
    
# Take the VM JSON info we have and create a VM with it.
# Things that change in the new VM:
# 1. New storage container UUID. This was replaced in __main__
# 2. New network UUID. This was replaced in __main__
# 3. Get vm_disk_id corresponding to the VM's disks (which have been uploaded to SFTPCONTAINER)
#    from all_vdisks
# 4. Blank out stuff like MAC and VM UUID, let the system pick this.
# 5. Adding a suffix during testing. Suffix should be an empty string during production.
def create_vm(mycluster,vm_json, all_vdisks, storage_container_uuid):
    
    # Deserialize vm_json so it looks like a dictionary again. 
    vm_dict = json.loads(vm_json)
    
    vm_name = vm_dict["name"]
    vm_uuid = vm_dict["uuid"]
    print("VVVVVVVVVVVVVV VM NAME: ",vm_name, "VM UUID: ", vm_uuid)
    
    # Now look for vdisk names that match the vm name in all_vdisks[]
    # vm_unsorted_vdisks_info is a dict of vdisks associated with this VM.
    vm_unsorted_vdisks_info={}
    regex = vm_uuid + "_(\S+)\.(\d+).raw"
    for vdisk in all_vdisks:
        nfs_file_name = vdisk["nfs_file_name"]
        # print ("NFS File name: %s. Regex: %s" % (nfs_file_name, regex))
        matchObj = re.match(regex,nfs_file_name)
        # print "MatchObj after: ", matchObj
        if matchObj:
            print ("NFS File name: %s. G1: %s. G2: %s" % (nfs_file_name,matchObj.group(1),matchObj.group(2)))
            device_bus = matchObj.group(1)
            device_index = matchObj.group(2)
            vm_unsorted_vdisks_info[nfs_file_name] = [device_bus, device_index]
            print("***** UNSORTED")
            pprint(vm_unsorted_vdisks_info)
    
    print("**** SORTED")
    vm_vdisks_info = collections.OrderedDict(sorted(vm_unsorted_vdisks_info.items()))
    pprint(vm_vdisks_info)
    # Making changes to the JSON because VM create spec is different.
    del vm_dict["allow_live_migrate"]
    del vm_dict["gpus_assigned"]
    del vm_dict["power_state"]
    del vm_dict["uuid"]
    del vm_dict["vm_disk_info"]
    del vm_dict["vm_logical_timestamp"]
    # Looping through this means we'll work for VMs with multiple NICS.
    for nic in vm_dict["vm_nics"]:
        del nic["mac_address"]
        del nic["model"]
        try:
            print("Deleting IP Address")
            del nic["ip_address"]
        except KeyError:
            print("IP Address not set: IPAM was probably not configured.")
        try:
            print("Deleting Requested IP Address")
            del nic["requested_ip_address"]
        except KeyError:
            print("Requested IP Address not set: IPAM was probably not configured.")
    # If we create our own UUID here, we can use it afterwards to power up the VM.
    vm_dict["uuid"] = str(uuid.uuid4())
    # Create boot device.
    vm_dict["boot"] = {}
    vm_dict["boot"]["disk_address"] = {}
    vm_disks = []
    # print("before loop")
    # pprint(vm_dict)
    # Each one of these raw files will look like:
    # <vm_uuid>_<device_bus>_<device_index>.raw
    for nfs_file_name,vdisk_info in vm_vdisks_info.items():
        device_bus = vdisk_info[0]
        device_index = vdisk_info[1]
        ndfs_filepath = "/" + C.SFTPCONTAINER + "/" + nfs_file_name
        # print ("NFS File Name: %s. Bus: %s. Index: %s NDFS: %s" %(nfs_file_name,device_bus,device_index,ndfs_filepath))
        # Create an entry only for the boot device.
        # If we found a drive with C.BOOT_DEVICE_BUS and C.BOOT_DEVICE_INDEX, then change to scsi:0.
        # POST /vms seems to want all boot devices to be scsi:0
        if (device_bus == C.BOOT_DEVICE_BUS and device_index == C.BOOT_DEVICE_INDEX):
            print ("creating boot drive. NFS File Name: %s" % nfs_file_name)
            vm_dict["boot"]["boot_device_type"] = "disk"
            vm_dict["boot"]["disk_address"]["device_bus"] = "scsi"
            vm_dict["boot"]["disk_address"]["device_index"] = "0"
            vm_dict["boot"]["disk_address"]["ndfs_filepath"] = ndfs_filepath
            vm_disks.append(
                { 
                    "is_cdrom": False,
                    "vm_disk_clone": {
                        "disk_address": {
                            "device_bus": device_bus,
                            "device_index": device_index,
                            "ndfs_filepath": ndfs_filepath
                        },
                        "storage_container_uuid": storage_container_uuid
                    }
                })
    # If we don't have a boot device then we don't have a VM. Exit.
    # print("after loop")
    # pprint(vm_dict)
    if (len(vm_dict["boot"]["disk_address"]) == 0):
        print(">>> Could not find boot device at: %s:%s" % (C.BOOT_DEVICE_BUS,C.BOOT_DEVICE_INDEX))
        sys.exit(1)
    # pprint(vm_disks)
    vm_dict["vm_disks"] = vm_disks
                
    # VM_SUFFIX is used for testing. It should be an empty string in production.
    vm_dict["name"] = vm_name + C.VM_SUFFIX
        
    # print "XXXXX Final look at vm_dict before creating VM."
    # pprint(vm_dict)
        
    vm_json = json.dumps(vm_dict)
    print("VM_JSON RIGHT BEFORE CREATE: %s" % vm_json)
    cluster_url = mycluster.base_urlv2 + "/vms/"
    print("Creating VM: %s" % vm_dict["name"])
    server_response = mycluster.sessionv2.post(cluster_url, data=json.dumps(vm_dict))
    print("Server Response")
    pprint(server_response)
    
    # If the VM has > 1 drive, attach them here. We need to do this separately because 
    # POST /vms doesn't handle that situation properly.
    if (len(vm_vdisks_info) > 1):
        for nfs_file_name,vdisk_info in vm_vdisks_info.items():
            vm_disk_spec = {}
            vm_disk_spec["vm_disks"] = []
            vm_disks = []
            device_bus = vdisk_info[0]
            device_index = vdisk_info[1]
            ndfs_filepath = "/" + C.SFTPCONTAINER + "/" + nfs_file_name
            # print ("NFS File Name: %s. Bus: %s. Index: %s NDFS: %s" %(nfs_file_name,device_bus,device_index,ndfs_filepath))
            # Skip past the boot drive.
            if (device_bus == "scsi" and device_index == "0"):
                    continue
            vm_disks.append(
                { 
                    "is_cdrom": False,
                    "vm_disk_clone": {
                        "disk_address": {
                            "device_bus": device_bus,
                            "device_index": device_index,
                            "ndfs_filepath": ndfs_filepath
                        },
                        "storage_container_uuid": storage_container_uuid
                    }
                })
            vm_disk_spec["uuid"] = vm_dict["uuid"]
            vm_disk_spec["vm_disks"] = vm_disks
            cluster_url = mycluster.base_urlv2 + "/vms/" + vm_dict["uuid"] + "/disks/attach"
            print("Attaching disk %s %s to %s" % (device_bus, device_index, vm_dict["name"]))
            server_response = mycluster.sessionv2.post(cluster_url, data=json.dumps(vm_disk_spec))
            print("Server Response")
            pprint(server_response)
            # End if it wasn't a boot device.
        # End for loop.
    # End if len(vm_vdisks_info) > 1.
    
    return server_response.status_code,json.loads(server_response.text),vm_dict["uuid"]

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--upload", action='store_true', help="Upload vdisks. (default is no, we assume they are already there)")
        parser.add_argument("csvfile", type=str, help="CSV File with VM names")
        args = parser.parse_args()

        csvfile = args.csvfile
        
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        mycluster = C.my_api(C.dst_cluster_ip,C.dst_cluster_admin,C.dst_cluster_pwd)
        print ("hello world!")
        status, cluster = mycluster.get_cluster_information()
        if (status != 200):
            print("Cannot connect to %s" % cluster)
            print("Did you remember to update the config file?")
            sys.exit(1)
        
        # Displays cluster authentication response and information.
        # print("Status code: %s" % status)
        # print("Text: ")
        # print(json.dumps(cluster,indent=2))
        # print("=")
        
        # Get specific cluster elements.
        print ("Name: %s." % cluster["name"])
        print ("ID: %s." % cluster["id"])
        print ("Cluster Ext IP Address: %s." % cluster["cluster_external_ipaddress"])
        print ("Number of Nodes: %s." % cluster["num_nodes"])
        print ("Version: %s." % cluster["version"])

        # If we can't connect to this port then we can't sftp.
        # Might as well error out now.
        if mycluster.test_port(C.dst_cluster_ip, 2222) == False:
            print("Cannot connect to port 2222 on %s. We won't be able to sftp files in." \
                  % C.dst_cluster_ip)
            print("Did you remember to run the following command on any one of the CVMs?")
            print("'allssh modify_firewall -f -o open -i eth0 -p 2222 -a'")
            sys.exit(1)

        # Get the new storage_container_uuid.
        status,resp = mycluster.get_storage_container_info()
        all_containers = resp["entities"]
        # print "ALL STORAGE CONTAINERS"
        # pprint(all_containers)
        
        for container in all_containers:
            if (container["name"] == C.SFTPCONTAINER):
                storage_container_uuid = container["storage_container_uuid"]
        try:
            print ("Container: %s. UUID: %s" % (C.SFTPCONTAINER, storage_container_uuid))
        except NameError:
            print (">>> Cannot proceed. Have you created '%s' on your destination cluster? <<<" % C.SFTPCONTAINER)
            sys.exit(1)
        
        # Get the new network_uuid.
        status,resp = mycluster.get_network_info()
        all_networks = resp["entities"]
        # print "ALL NETWORKS"
        # pprint(all_networks)
        
        for network in all_networks:
            if (network["name"] == C.MYNETWORK):
                network_uuid = network["uuid"]
        try:
            print ("Network: %s. UUID: %s" % (C.MYNETWORK, network_uuid))
        except NameError:
            print (">>> Cannot proceed. Have you created '%s' on your destination cluster? <<<" % C.MYNETWORK)
            sys.exit(1)
        
        # Get list of VMs that actually matter.
        important_vms = mycluster.get_important_vms(csvfile)
        
        # Read config files from C.DIR so we can match VM names and UUIDs.
        # Keying by UUID means we can accomodate VMs with the same name (by diff UUIDs obviously)
        vmname_byuuid = {}
        files = os.listdir(C.DIR)
        vmname_byuuid = mycluster.get_vmnameanduuid(files)
        print("VMNAME_BYUUID")
        pprint(vmname_byuuid)
        
        uuid_regex = "[a-z0-9-]+"
        # If we choose to, process files, and upload the right qcow2 files.
        if args.upload:
            disk_image_regex = "^(" + uuid_regex + ")_(\S+)\.(\d+).qcow2"
            for f in files:
                matchObj = re.match(disk_image_regex,f)
                if matchObj:
                    vm_uuid = matchObj.group(1)
                    vm_name = vmname_byuuid[vm_uuid]
                    if (vm_name not in important_vms):
                        continue
                    sftp_upload(f,vm_name)
                # End if.
            # End for.
        # End if upload.

        # Now get a list of the disk images/qcow2 files that we uploaded
        # to SFTPCONTAINER earlier.
        # These should look like vm_uuid_disklabel.qcow2
        status,resp = get_vdisks(mycluster,storage_container_uuid)
        all_vdisks = resp["entities"]
        disk_image_list=[]
        for vdisk in all_vdisks:
            # In some AOS versions, nfs_file_name has a .filepart suffix on it if it had been sftp'd
            # to the container. If that is the case, re.search for .filepart, on success, use split()
            # to update nfs_file_name. Three extra lines of code.
            nfs_file_name = vdisk["nfs_file_name"]
            # Is it a disk image file?
            disk_image_regex = "^(" + uuid_regex + ")_(\S+)\.(\d+).qcow2"
            matchObj = re.match(disk_image_regex,nfs_file_name)
            if matchObj:
                # print ("matchobj group(0) %s" % matchObj.group(0))
                
                vm_uuid = matchObj.group(1)
                vm_name = vmname_byuuid[vm_uuid]
                # We don't want to process VMs that are not in the CSV file.
                if (vm_name not in important_vms):
                    continue
                disk_image_list.append(nfs_file_name)
        # End for loop.
        if (len(disk_image_list) == 0):
            print (">>> Cannot proceed. Have you transferred qcow2 files to '%s' on your destination cluster? <<<" % C.SFTPCONTAINER)
            print (">>> You can do this by running this program with the --upload option.")
            sys.exit(1)
        
        # Now read VM config files from C.DIR.
        # These should look like vm_uuid.cfg.
        # If we find file that look different, save the name to let the user know later.
        
        vm_config_list=[]
        unrecognized_list=[]
        for f in files:
            # print "Reading files in ", C.DIR, " ", f
            # Is it a config file?
            cfg_regex = "(" + uuid_regex + ").cfg"
            matchObj = re.match(cfg_regex,f)
            if matchObj:
                vm_uuid = matchObj.group(1)
                vm_name = vmname_byuuid[vm_uuid]
                # We don't want to process VMs that are not in the CSV file.
                if (vm_name not in important_vms):
                    continue
                vm_config_list.append(f)
                # print ("VM Config file MATCH: f: %s. VM: %s" % (f, vm_name))
                continue
            
            # If we are here then we couldn't recognize the file as a vm config file.
            # print ("Could not recognize %s as either a disk image or a config file." % f)
            unrecognized_list.append(f)
        
        # End loop where we read files in C.DIR.
        print("DISK IMAGES in ", C.SFTPCONTAINER)
        pprint(disk_image_list)
        print("VM CONFIG LIST in ", C.DIR)
        pprint(vm_config_list)
        print("NON-VM CONFIG FILES in", C.DIR)
        pprint(unrecognized_list)

        cvm_ip_list = mycluster.get_cvms()
        
        # Process disk images.
        # We assume the files are already transferred, so just ssh into the CVMs
        # and convert them.
        i=0
        j=0
        while i < len(disk_image_list):
            spawned = False
            cvm_ip = cvm_ip_list[j]
            # Spawn off first file on CVM1, second on CVM2, etc
            # If the number of jobs on CVM <= C.MAX_CVM_JOBS, then spawn off a new job.
            numjobs = mycluster.check_jobs(cvm_ip,C.dst_cvm_pwd)
            if (numjobs <= C.MAX_CVM_JOBS):
                print("*********")
                print("Submitting: %s on %s for conversion. Index: %d" % (disk_image_list[i],cvm_ip,i))
                mycluster.ssh_cmd(cvm_ip,C.dst_cvm_pwd,disk_image_list[i],nfs_path=None)

                # Sleep for a few seconds to give ssh a chance to fire up before we check.
                time.sleep(5)
                spawned = True
            
            # If we are here, then we either spawned off a job, or skipped
            # because we reached C.MAX_CVM_JOBS. Either way, move to the next CVM.
            j += 1
            if (j == len(cvm_ip_list)):
                j=0
            # If we were able to spawn off a job, then move on to the next disk image.
            if (spawned == True):
                i += 1

        # End while loop.
        # Qemu-img jobs are now running on all CVMs. Loop here and keep checking that they 
        # are complete.
        runtime=0
        while True:
            total_jobs = 0
            for cvm_ip in cvm_ip_list:
                # print("CVM_IP: ", cvm_ip)
                total_jobs += mycluster.check_jobs(cvm_ip,C.dst_cvm_pwd)
            if total_jobs > 0:
                print("%s conversion jobs are still running. Sleeping...(%s seconds)" % (total_jobs,runtime))
                time.sleep(5)
                runtime += 5
            else:
                break
        # End while loop.
        
        # At this point we have converted all files in SFTPCONTAINER.
        # Start processing each VM config file. 
        for vm_config_file in vm_config_list:
            vmcfg_fp = open(C.DIR + "/" + vm_config_file, "r")
            vm_json = vmcfg_fp.read()
            vmcfg_fp.close()
            
            # Replace storage_container_uuid in vm_json.
            # We're looking for a string that looks like:
            # "storage_container_uuid": "1ed37398-5fb3-49bb-835b-cc9449e0c057"
            regex_src  = "\"storage_container_uuid\": \"([0-9a-z-]*)\""
            regex_repl = "\"storage_container_uuid\": \"" + storage_container_uuid + "\""
            vm_json = re.sub(regex_src, regex_repl, vm_json)
            
            # Replace network_uuid in vm_json.
            # We're looking for a string that looks like:
            # "network_uuid": "4ea3b863-8a9d-43c4-9801-796425569202"
            regex_src  = "\"network_uuid\": \"([0-9a-z-]*)\""
            regex_repl = "\"network_uuid\": \"" + network_uuid + "\""
            vm_json = re.sub(regex_src, regex_repl, vm_json)

            # Get list of vdisks on SFTPCONTAINER. These should have the qcow2 files
            # AND the ones in raw format because we donverted them earlier.
            status,resp = get_vdisks(mycluster,storage_container_uuid)
            all_vdisks = resp["entities"]
            # pprint(all_vdisks)
            
            # Create the VM.
            status,resp,vm_uuid = create_vm(mycluster,vm_json,all_vdisks,storage_container_uuid)
            print ("XXXXXX CREATE VM STATUS: %s." % status)
            # Check if we returned properly, otherwise continue.
            if (status != 201):
                print ("Could not create VM in %s" % vm_json)
                pprint(resp)
                continue
            
            # Power on the VM.
            status,resp = mycluster.power_on_vm(vm_uuid)
            print("Status code for power on: %s" % status)
            pprint(resp)
        # End for loop where we process vm_config files.

        print("================================")
        print("*COMPLETE*")

    except Exception as ex:
        print(ex)
        sys.exit(1)
