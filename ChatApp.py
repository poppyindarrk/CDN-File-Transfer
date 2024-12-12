import sys
import json
import os
import sys
import threading
import time
from socket import *
from google.cloud import storage
from datetime import timedelta
import datetime

import requests

def generate_upload_signed_url(bucket_name, blob_name, expiration_minutes=15):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=expiration_minutes),
        method="PUT",
        content_type="application/octet-stream"
    )
    return url

registered_table = dict()  # name, ip, port, status
server_socket = socket(AF_INET, SOCK_DGRAM)
offline_messages = dict()

def server(port_num):
    global registered_table,offline_messages
    print(">>> [Server is online...]")
    server_socket.bind(('localhost', port_num))

    while True:
        try:
            buffer, client_address = server_socket.recvfrom(4096)
            buffer = buffer.decode()
        except KeyboardInterrupt:
            server_socket.close()
            os._exit(1)
        lines = buffer.splitlines()
        header = lines[1]
        client_port = int(lines[3])
        client_name = lines[5]
        value = lines[7]
        # when receive new user connection request
        if header == "file_upload":
            filename = lines[7]
            bucket_name = "com6776"
            signed_url = generate_upload_signed_url(bucket_name, filename)
            
            # Send the signed URL to the client
            ack = f"header:\nfile_upload_ack\nsigned_url:\n{signed_url}\nmessage:\nUpload your file"
            server_socket.sendto(ack.encode(), (client_address[0], client_port))
            print(f">>> [Generated upload URL for {filename}]")
        
        # Broadcast file URL after upload
        elif header == "file_broadcast":
            file_url = lines[7]
            ack = f"header:\nfile_received\nsender:\n{client_name}\nfile_url:\n{file_url}"
            for client in registered_table:
                if registered_table[client][2] == "Online":
                    server_socket.sendto(ack.encode(), (registered_table[client][0], registered_table[client][1]))
            print(f">>> [Broadcasted file URL: {file_url}]")
        elif header == "connect":
            state = True
            #check existence of client
            if client_name in registered_table.keys():
                if registered_table[client_name][2] == "Offline":
                    registered_table[client_name][2] = "Online"
                else:
                    state = False
            #if new client, register
            if state:
                temp = [client_address[0], client_port, "Online"]
                registered_table[client_name] = temp
                print(">>> [Client table updated.]")
                register(state, client_address[0], client_port)
            else:
                print(">>> [Duplicate login detected.]")
                register(state, client_address[0], client_port)
        # when a client reports a disconnection of another client
        elif header == "notify":
            registered_table[value][2] = "Offline"
            print(">>> [Client table updated.]")
            broadcast()
        # when a client have offline messages
        elif header == "Offline":
            res_client=lines[2]
            send_offline_message(client_name,res_client,value)
            ack = f"header:\nOffline\nMessage:"+str(offline_messages)
            server_socket.sendto(ack.encode(), (client_address[0], client_port))
        elif header == "dereg":
            registered_table[client_name][2] = "Offline"
            print(">>> [Client table updated.]")
            broadcast()
            ack = f"header:\nack-dereg\nMessage:\nThis is an ACK from server"
            server_socket.sendto(ack.encode(), (client_address[0], client_port))
        elif header == "reg":
            registered_table[client_name][2] = "Online"
            print(">>> [Client table updated.]")
            #check and get offline messages
            broadcast_o(client_name)
            #feedback to the sender
            ack = f"header:\nack-reg\nMessage:\n"+client_name
            server_socket.sendto(ack.encode(), (client_address[0], client_port))
        #group chat
        elif header == "send_all":
            msg = lines[9]
            ack = f"header:\nack-group\nsender:\n{client_name}\nmessage:\n" + msg
            for client in registered_table:
                if registered_table[client][2] == "Online":
                    server_socket.sendto(ack.encode(), (registered_table[client][0], registered_table[client][1]))
            print(f">>> [Client {client_name} sent group message: {msg}]")
        else:
            pass

def register(state, ip, port):
    time.sleep(0.2)
    server_socket.sendto(f"header:\nregister\nstate\n{state}".encode(), (ip, port))
    time.sleep(0.5)
    if state:
        broadcast()

def broadcast_o(client_name):
    # Check if the client_name is in offline_messages
    if client_name in offline_messages:
        # Retrieve all timestamps and senders for the specific client_name
        timestamps = []
        senders = []
        for message in offline_messages[client_name]:
            # Splitting the message to extract the sender and timestamp
            sender, timestamp, _ = message.split(" ", 2)
            timestamps.append(timestamp)
            senders.append(sender)
        # Check if the client_name status is "Online"
        if registered_table[client_name][2] == "Online":
            # Only get the message for the specific client_name
            message_for_client = {client_name: offline_messages[client_name]}
            info = message_for_client.get(client_name, " ")
            server_socket.sendto((f"header:\nupdate_o\nContent:\n" + json.dumps(info)).encode(),
                                 (registered_table[client_name][0], registered_table[client_name][1]))
            #get desired timestamps and senders
            for i in range (len(timestamps)):
                sender_rp=senders[i].replace(":", "")
                #if it is group chat, delete the prefix
                if(len(sender_rp)>8):
                    sender_rp = sender_rp[10:]
                #send feedback to sender
                ack = f"header:\nreg_rp\nMessage:\n"+client_name+"\n"+timestamps[i]
                server_socket.sendto(ack.encode(), (registered_table[sender_rp][0], registered_table[sender_rp][1]))
            del offline_messages[client_name]

def broadcast():
    for user in registered_table.keys():
        if registered_table[user][2] == "Online":
            server_socket.sendto((f"header:\nupdate\nContent:\n" + json.dumps(registered_table)).encode(),
                                 (registered_table[user][0], registered_table[user][1]))


def checkIp(ip):
    if ip == "localhost":
        return True

    if len(ip.split(".")) != 3:
        return False

    for x in ip.split("."):
        if not x.isnumeric():
            return False

    return True
registered_users = dict()
client_socket = socket(AF_INET, SOCK_DGRAM)
listen_socket = socket(AF_INET, SOCK_DGRAM)
name = ""
is_ack_c = False
wait = ""
is_ack_s = False
mode = "normal"
group = ""
server_ip = ""
server_port = 0
semaphore = threading.Semaphore(1)
#save the offline msg to server in offline_message
def send_offline_message(sender, recipient, message):
    global offline_messages
    current_time = time.strftime("%H:%M:%S", time.localtime())
    formatted_message = f"{sender}: {current_time} {message}"
    # save messages
    if recipient not in offline_messages:
        offline_messages[recipient] = [formatted_message]
    else:
        offline_messages[recipient].append(formatted_message)

def client(user_name, ip, port, client_port):
    # initialization
    global name, is_ack_c, is_ack_s, mode, group, server_port, server_ip, wait, semaphore, offline_time,offline_messages
    name = user_name
    server_ip = ip
    server_port = port
    offline_time=0
    # register client to the server
    init_msg = "header:\n" + "connect" + "\n" + "port:\n" + str(client_port) + "\nname\n" + user_name + "\nmessage\n" + " "
    client_socket.sendto(init_msg.encode(), (server_ip, server_port))
    #join group
    to_send = "header:\n" + "join_group" + "\n" + "port:\n" + str(
                            client_port) + "\nsource:\n" + user_name + "\nname:\n" + " "
    client_socket.sendto(to_send.encode(), (server_ip, server_port))
    # start listening process
    listen_socket.bind(('', client_port))
    listen = threading.Thread(target=client_listen, args=())
    listen.start()
    time.sleep(0.5)
    # keep sending commands
    while True:
        try:
            input_list = []
            try:
                temp = input(">>> ")
                input_list = temp.split()
            except KeyboardInterrupt:
                os._exit(1)
            if len(input_list) == 0:
                break
            header = input_list[0]
            # validify command
            if not validifyOp(header):
                continue
            # send message to other online clients
            if header == "send_file":
                recipient = input_list[1]  # The recipient for the file
                file_path = input_list[2]  # The file to be sent

                # Check if recipient exists and is registered
                if recipient in registered_users:
                    if registered_users[recipient][2] != "Online":
                        # If the recipient is offline, handle offline notification
                        offline_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        displayMsg(f"[Offline File sent at {offline_time} received by the server and saved.]")
                        to_send = f"header:\nfile_upload\nport:\n{client_port}\nsource:\n{name}\nfilename:\n{file_path}"
                        client_socket.sendto(to_send.encode(), (server_ip, server_port))
                        time.sleep(0.5)
                    else:
                        # If the recipient is online, request a signed URL for file upload
                        to_send = f"header:\nfile_upload\nport:\n{client_port}\nsource:\n{name}\nfilename:\n{file_path}"
                        client_socket.sendto(to_send.encode(), (server_ip, server_port))
                        time.sleep(1)
                else:
                    displayMsg("[User could not be found.]")


            elif header == "file_complete":
                # Notify server of file upload completion
                file_url = input_list[1]
                to_send = f"header:\nfile_broadcast\nport:\n{client_port}\nsource:\n{name}\nfile_url:\n{file_url}"
                client_socket.sendto(to_send.encode(), (server_ip, server_port))
                print(f">>> [File URL sent to server: {file_url}]")
            elif header == "send":
                mode="normal"
                recipient = input_list[1]
                if recipient in registered_users:
                    msg = ""
                    for i in range(2, len(input_list)):
                        msg = msg + input_list[i] + " "
                    # check if the target client is online
                    if registered_users[recipient][2] != "Online":
                        #if not, save the send time and msg
                        offline_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        displayMsg(f"[Offline Message sent at {offline_time} received by the server and saved.]")
                        to_send = "header:\n" + "Offline\n" + recipient + "\n" + str(
                        client_port) + "\nsource:\n" + name + "\nmessage\n" + msg
                        client_socket.sendto(to_send.encode(), (server_ip, server_port))
                        time.sleep(0.5)
                        continue
                    #if online send the msg
                    to_send = "header:\n" + header + "\n" + "port:\n" + str(
                        client_port) + "\nsource:\n" + name + "\nmessage\n" + msg
                    # set the target name as a global variable
                    wait = recipient
                    client_socket.sendto(to_send.encode(), (registered_users[recipient][0], registered_users[recipient][1]))
                    displayMsg("[Message sent!]")
                    time.sleep(0.5)
                    #if ack, message recieved, if not try 5 times and exit
                    if is_ack_c:
                        displayMsg(f"[Message received by {recipient}.]")
                        is_ack_c = False
                    else:
                        displayMsg(f"[No ACK from {recipient}, message not delivered.]")
                        notifyServer(recipient)
                else:
                    displayMsg("[User could not be found.]")

            # command to deregister self from server
            elif header == "dereg":
                if input_list[1] == name:
                    to_send = "header:\n" + header + "\n" + "port:\n" + str(
                        client_port) + "\nsource:\n" + name + "\nmessage\n" + " "
                    client_socket.sendto(to_send.encode(), (server_ip, server_port))
                    time.sleep(0.5)
                    #if server feedback, dereg, if server no response, retry 5 times, then exit
                    if is_ack_s:
                        is_ack_s = False
                        displayMsg(f"[You are Offline. Bye.]")
                    else:
                        os._exit(1)
                else:
                    displayMsg("[Incorrect username.]")
            #reg command
            elif header == "reg":
                mode="normal"
                if input_list[1] == name:
                    to_send = "header:\n" + header + "\n" + "port:\n" + str(
                        client_port) + "\nsource:\n" + name + "\nmessage\n" + " "
                    client_socket.sendto(to_send.encode(), (server_ip, server_port))
                    time.sleep(0.5)
                else:
                    displayMsg("[Incorrect username.]")
            # command to send message within a group
            elif header == "send_all": 
                    recipient = input_list[1]
                    mode = "inGroup"
                    msg = ""
                    for i in range(1, len(input_list)):
                        msg = msg + input_list[i] + " "
                    # check if the target client is online
                    #if it's online
                    to_send = "header:\n" + header + "\n" + "port:\n" + str(
                        client_port) + "\nsource:\n" + name + "\ngroup:\n" + group + "\nmessage:\n" + msg
                    client_socket.sendto(to_send.encode(), (server_ip, server_port))
                    time.sleep(0.5)
                    if is_ack_s:
                        displayMsg("[Group Message received by Server.]")
                        is_ack_s = False
                    else:
                        os._exit(1)
                    for recipient, user_data in registered_users.items():
                        if user_data[2] != "Online":
                            #if member not online, save the offline message and time
                            offline_time = time.strftime("%H:%M:%S", time.localtime())
                            displayMsg(f"[Offline Message sent at {offline_time} received by the server and saved.]")
                            group_res="Group_Chat:"+ name
                            to_send = "header:\n" + "Offline\n" + recipient + "\n" + str(
                            client_port) + "\nsource:\n" + group_res + "\nmessage\n" + msg
                            client_socket.sendto(to_send.encode(), (server_ip, server_port))
                            time.sleep(0.5)
                    if is_ack_c:
                        is_ack_c = False
                    else:
                        displayMsg(f"[No ACK from {recipient}, message not delivered.]")
                        notifyServer(recipient)
                        
            else:
                displayMsg("[Invalid input!]")
        except:
            displayMsg("[Invalid inputs!]")

def client_listen():
    global mode, offline_messages
    while True:
        buffer, sender_address = listen_socket.recvfrom(4096)
        buffer = buffer.decode()
        lines = buffer.splitlines()
        header = lines[1]
        # when server updates the registered clients tablex
        if header == "file_upload_ack":
            try:
                # Ask the user for the file path to upload
                file_path = input(">>> Enter the file path to upload: ")

                # Dynamically generate the blob name from the file path
                blob_name = os.path.basename(file_path)  # Extract the file name (e.g., "example.txt")
                bucket_name = "com6776"  # Replace with your actual bucket name

                # Generate a signed URL for the dynamic blob name
                signed_url = generate_upload_signed_url(bucket_name, blob_name)
                print(f"Upload URL for {blob_name}: {signed_url}")

                # Open the file and upload it using the signed URL
                with open(file_path, "rb") as file_data:
                    response = requests.put(
                        signed_url,
                        data=file_data,
                        headers={"Content-Type": "application/octet-stream"}  # Ensure correct content type
                    )
                    if response.status_code == 200:
                        print(">>> [File uploaded successfully!]")
                        file_url = signed_url.split("?")[0]  # Extract the public URL
                        # Notify the server about the uploaded file
                        to_send = f"header:\nfile_broadcast\nport:\n{client_port}\nsource:\n{name}\nfile_url:\n{file_url}"
                        client_socket.sendto(to_send.encode(), (server_ip, server_port))
                    else:
                        print(f">>> [Failed to upload file: {response.status_code}, Response: {response.text}]")

            except FileNotFoundError:
                print(">>> [File not found! Please check the file path.]")
            except Exception as e:
                print(f">>> [An unexpected error occurred: {e}]")


        elif header == "file_received":
            file_url = lines[5]
            sender_name = lines[3]
            print(f">>> [New file from {sender_name}: {file_url}]")
        elif header == "register":
            state = lines[3]
            if state:
                displayMsg("[Welcome, You are registered.]")
            else:
                displayMsg("[You have already login somewhere else.]")
                os._exit(1)
        # when receive successfully registered message from server
        elif header == "update" or header == "update_o":
            msg = lines[3]
            global registered_users, is_ack_s, is_ack_c
            registered_users = json.loads(msg)
            displayMsg("[Client table updated.]")
            #if it's offline message
            if(header == "update_o"):
                print(">>> [You have offline messages:]")
                for user in registered_users:
                    print(">>> " + str(user))
            #if it's register table broadcast
            else:
                print(">>> " + str(registered_users))

        # when receive messages from other clients
        elif header == "send":
            mode = "normal"
            source_port = int(lines[3])
            msg = lines[7]
            source_name = lines[5]
            if mode == "normal":
                displayMsg(f"{source_name}: " +msg)
                client_res(sender_address[0], source_port)
            elif mode == "register":
                displayMsg(f"{source_name}: "+ msg)
                client_res(sender_address[0], source_port)
            else:
                client_res(sender_address[0], source_port)
                # when receive ack from other clients
        elif header == "ack":
            source_name = lines[5]
            if source_name == wait:
                msg = lines[7]
                is_ack_c = True
        elif header == "Offline":
            a=1
        # when receive ack for sending group messages from server
        elif header == "ack-group":
            sender_name = lines[3]
            # is_ack_c = True
            msg = lines[5]
            if sender_name == name:
                is_ack_s = True
                is_ack_c = True
            else:
                client_res_group(server_ip, server_port)
                displayMsg(f"Group Chat {sender_name}: {msg}")
        # # when receive ack for unregistering self
        elif header == "ack-dereg":
            is_ack_s = True
        elif header == "ack-reg":
            is_ack_s = True
        #send the sender feedback
        elif header == "reg_rp":
            timestamp=lines[4]
            receiver=lines[3]
            displayMsg("[Offline Message sent at "+timestamp+" received by "+receiver+"]")
            is_ack_s = True
        else:
            displayMsg("[Unknown header.]")

# ack format
def client_res(client_ip, client_port):
    global start_time
    ack = "header:\n" + "ack" + "\n" + "port:\n" + str(
        server_port) + "\nsource:\n" + name + "\nMessage:\nThis is an ACK from client"
    client_socket.sendto(ack.encode(), (client_ip, client_port))
    start_time = time.time()

# let the server know someone is offline
def notifyServer(recipient):
    msg = "header:\n" + "notify" + "\n" + "port:\n" + str(
                    server_port) + "\nsource:\n" + name + "\ntarget:\n" + recipient
    client_socket.sendto(msg.encode(), (server_ip, server_port))

# check if the operation command fulfills the current mode
def validifyOp(op):
    global mode, group
    if op in ["send", "dereg","reg","send_all","send_file","file_complete"]:
        return True
    displayMsg("[Invalid Command.]")
    return False

def displayMsg(msg):
    print(">>> "+msg)

def client_res_group(client_ip, client_port):
    ack = "header:\n" + "ack-group" + "\n" + "port:\n" + str(
        server_port) + "\nsource:\n" + name + "\nMessage:\nThis is an ACK from client"
    client_socket.sendto(ack.encode(), (client_ip, client_port))

if __name__ == "__main__":
    #check serverwith valid port
    m = sys.argv[1]
    if m == '-s':
        try:
            server_port = int(sys.argv[2])
        except:
            print(">>> Invalid port number!")
        server(server_port)
    #check client with valid port
    elif m == '-c':
        user_name = sys.argv[2]
        server_ip = sys.argv[3]
        if checkIp(server_ip):
            try:
                server_port = int(sys.argv[4])
                client_port = int(sys.argv[5])
            except:
                print(">>> Invalid port number!")
            client(user_name, server_ip, server_port, client_port)
        else:
            print(">>> Invalid IP address")