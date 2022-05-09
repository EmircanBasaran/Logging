import paramiko
import datetime

def backup():
    #SFTP
    paramiko.util.log_to_file("C:\Backup\paramiko.log")
    # Open a transport
    host,port = "targetip",22
    transport = paramiko.Transport((host,port))

    # Auth
    username,password = "targetuser","targetpwd"
    transport.connect(None,username,password)

    # Go!
    sftp = paramiko.SFTPClient.from_transport(transport)
    print('File transfer started')
    # Upload
    today=datetime.datetime.today()
    filepath = "targetpath/backup_{}.tar.gz".format(today.strftime('%Y-%m-%d'))
    localpath = "localpath\\backup_{}.tar.gz".format(today.strftime('%Y-%m-%d'))
    sftp.put(localpath,filepath)
    print('File transfer ended')
    # Close
    if sftp: sftp.close()
    if transport: transport.close()

if __name__=='__main__':
    backup()