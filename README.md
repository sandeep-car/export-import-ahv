# export-import-vms-AHV

SYNOPSIS:
This README file accompanies the two scripts that are required to export and import AHV VMs. There is also a config file which contains variables which must be modified to your environment. In addition there is a HOWTO file which describes the procedure to properly set up Python3, OpenSSL and sshpass on your Linux system.

First, follow the instructions in the HOWTO to configure your Linux system. Then read this README file and clusterconfig.py in their entirety.

Three main steps:
1. Run a script to export VMs from the source AHV cluster, save them on a removeable drive.
2. Ship the drive to the remote site.
3. Attach the removeable drive to a system in the remote site. Run a script to import VMs into the destination AHV cluster.

DETAILS:
1. Pick a Linux vm or system. If you have a Windows laptop/desktop, you can install Virtualbox and create a Centos image. This was tested under Centos 6.9 and Centos 6.10, and Centos 7+. Follow the instructions in the HOWTO file to build the Python environment on Linux.
* Attach your removeable drive to this system. 
* You will  need to open up the firewall on your Linux system, so "iptables -F". Check that the ports are open by "iptables -L -n".
* Your Linux VM will need to ssh into the source and destination AHV cluster as user "nutanix". There are two ways to enable this:

a. Update src_cvm_pwd and dst_cvm_pwd variables in clusterconfig.py to the password of the nutanix user.

b. Set those variables to an empty string in clusterconfig.py, and set up password-less ssh between your Linux VM and every one of the CVMs on the source and destination cluster. :-)

2. Open up port 2222 for sftp on the CVMs.

nutanix@CVM: allssh modify_firewall -f -o open -i eth0 -p 2222 -a

3. Transfer exportvm_on_source.py to the Linux system. You will need python 3.7, and some Python modules (requests and paramiko) which are described in the HOWTO. Create a user administrator called restapiuser so the admin password isn't made public. Please be sure to update the global variables in clusterconfig.py.
* exportvm_on_source.py takes 2 arguments : CSV file with VM names, and  optionally, --qemu . With the optional --qemu argument it will create qcow2 files in EXPORTCONTAINER which is exportcontainer by default.  Without this argument, it assumes that the qcow2 files are in EXPORTCONTAINER already. EXPORTCONTAINER must be manually created on the source AHV cluster.
* The script will now create json files describing each VM specified  in the CSV file (subject to the caveats below) in DIR, which is /root/source/export-import/output by default.  This should be the mount point of your removeable drive. You can also turn on the COMPRESS flag in clusterconfig.py to compress the qcow2 files if it makes sense.
* The qcow2 files in EXPORTCONTAINER will then be automatically downloaded to DIR. 

4. After making sure that the global variables in clusterconfig.py reflect your environment, run exportvm_on_source.py. The script takes a CSV file as a required argument. We assume that the first column of the CSV file contains the names of the VMs that must be exported. So:

vm1

vm2

vm3

Note that a VM will be considered for export only if:
* It exists in the source AHV cluster.
* It is in the CSV file.
* It is powered off.

After confirming that the config files and qcow2 files have been transferred successfully to DIR, you can ship the removeable drive to your remote site!

Here are the steps to be run on the remote site.
1. Pick a Linux vm or system as in step 1 earlier, same requirements for Python etc. 

2. Transfer importvm_on_dest_sftp.py to the Linux system. Create restapiuser as earlier. Please be sure to update the global variables in clusterconfig.py, including manually creating SFTPCONTAINER. DIR should be the location of your mounted removeable drive.

3. You will also need to flush the firewall on Linux by "iptables -F". Check that the ports are open by "iptables -L -n".

4. Attach the removeable drive to the system and mount it on the system. It should be visible from the Linux VM where the code is installed, and mounted as DIR.

5. Two choices:

a. Run importvm_on_dest_sftp.py with 2 arguments (csv file with VM names, and --upload). --upload means we will upload the qcow2  files to SFTPCONTAINER from the script itself.

b. Upload qcow2 files manually to SFTPCONTAINER. Run importvm_on_dest_sftp.py.  If you uploaded the qcow2 files manually (via say winscp) then you must not use the --upload option. We'll still need the removeable drive mounted on DIR because the script needs to read the VM config files.

After the qcow2 files have been uploaded to SFTPCONTAINER (either manually or programatically), the script converts them into raw format, and creates VMs using drives cloned from these files. 

In some cases option(b) may be preferred. Please see the next section.

CAVEATS:
* We ignore CD-ROMS. IE, they are not created on the remote cluster. 
* Snapshots are also ignored. So are volume groups.
* qemu-img convert is run on CVMs in the source AHV cluster to generate qcow2 files. These can be gigantic.
* Extremely large qcow2 files (>30G) sometimes error out during download or upload. Every effort has been made to fix this, however you can always transfer the qcow2 files manually from/to EXPORTCONTAINER/SFTPCONTAINER. In the case of import, you would need to run importvm_on_dest_sftp.py *without* the --upload option. That's step 5(b) above.

* Every effort has been taken to make use of parallelism. Conversions of file formats happen in parallel. Downloads and uploads however happen a single file at a time. This is because not everybody has a fast SSD removeable drive, and we didn't want to overwhelm your removeable drive. If you have a fast SSD removeable drive, great, downloads and uploads are tuned so they happen quickly. 
* The device bus and device index of the boot drive of your VM can be configured in clusterconfig.py, as BOOT_DEVICE_BUS and BOOT_DEVICE_INDEX respectively. The import scripts need to know this so VMs can boot properly on the destination AHV cluster. The import script changes this to scsi:0 because it seems thats hard-wired in POST /vms.

If your VMs are configured in such a way where they each have different boot drives, you will need to import them separately.
* VMs with > 1 drive are imported properly. However in the case of Windows VMs these are not mapped correctly to their drive letters on the destination cluster. Please be sure to log into your Windows VMs and check.
* We make certain assumptions about VMs on the source and destination AHV cluster which may be incorrect. Please see create_vm() in importvm_on_dest_sftp.py.

TODO:
* Implement Ryan Rose's excellent suggestion on using multipart POST for uploading files.
* Should exportcontainer and importcontainer be created by the script instead of manually? Other commands such as allssh and iptables?
