import socket
import threading
import time
import logging
import argparse
import hashlib
import os
import random
#logging
logging.basicConfig(filename="logs.log", format="%(message)s", filemode="a")
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

#Global Variable
tracker_ip = "localhost"
tracker_port = 5100
lock = threading.Lock()
folder_path = ''
transfer_port = -1
entity_name = ''
missing_chunks = []
total_chunk = -1
local_chunks = {}

def parseCLA():
    parser = argparse.ArgumentParser()
    parser.add_argument('-folder', required=True, help='Full path of the folder')
    parser.add_argument('-transfer_port', type=int, required=True, help='Transfer port number')
    parser.add_argument('-name', required=True, help='Entity name')
    
    args = parser.parse_args()
    return args.folder, args.transfer_port, args.name

#Sending LOCAL_CHUNKS request to tracker
def send_chunks_to_tracker(tracker_socket):
    lock.acquire()
    lines = read_file_by_lines()
    lock.release()
    for line in lines:
        line = line.strip()
        if line.endswith("LASTCHUNK"):
            numChunks, lastChunk = line.split(',')
            return int(numChunks) 
        chunk_index, filename = line.split(',')
        lock.acquire()
        hash = hash_whole_file(filename)
        local_chunks[int(chunk_index)] = filename
        lock.release()
        request = "LOCAL_CHUNKS," + str(chunk_index) + "," + hash + "," + "localhost" + "," + str(transfer_port)
        tracker_socket.sendall(request.encode())
        time.sleep(1)
        lock.acquire()
        logger.info(entity_name + "," + request)
        lock.release()
	
def update_tracker(chunk_index, filename, tracker_socket):
    lock.acquire()
    hash = hash_whole_file(filename)
    local_chunks[int(chunk_index)] = filename
    lock.release()
    request = "LOCAL_CHUNKS," + str(chunk_index) + "," + hash + "," + "localhost" + "," + str(transfer_port)
    tracker_socket.sendall(request.encode())
    time.sleep(1)
    lock.acquire()
    logger.info(entity_name + "," + request)
    lock.release()
    
def request_info_from_tracker(chunk_index, tracker_socket):
    request = "WHERE_CHUNK," + str(chunk_index)
    lock.acquire()
    tracker_socket.sendall(request.encode())
    logger.info(entity_name + "," + request)
    lock.release()
    time.sleep(1)
    response = tracker_socket.recv(1024).decode()
    return response

def request_chunks_from_peer(peer_ip, peer_port, chunk_index, filename):
    peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    peer_socket.connect((peer_ip, peer_port))
    request = "REQUEST_CHUNK," + str(chunk_index)
    peer_socket.sendall(request.encode())
    time.sleep(1)
    lock.acquire()
    logger.info(entity_name + "," + request + "," + "localhost" + "," + str(peer_port))
    with open(os.path.join(folder_path, filename), 'wb') as file:
        while True:
            response = peer_socket.recv(1024)
            if not response:
                file.close()
                break
            file.write(response)
            file.flush()
        file.close()
    lock.release()
    peer_socket.close()
    
def process_peer(peer_socket):
    while True:
        request = peer_socket.recv(1024).decode()
        if not request:
            break
        if request.startswith("REQUEST_CHUNK"):
            typeReq, chunk_index = request.split(',')
            filename = None
            lock.acquire()
            '''
            with open(os.path.join(folder_path, 'local_chunks.txt'),'r') as file:
                for line in file:
                    line = line.strip()
                    if line.startswith(chunk_index) and not line.endswith("LASTCHUNK"):
                        ind, filename = line.split(',')
                        break
            file.close()
            '''
            if int(chunk_index) in local_chunks.keys():
                filename = local_chunks[int(chunk_index)]
            if filename:
                with open(os.path.join(folder_path, filename), 'rb') as file:
                    while True:
                        chunk = file.read(1024)
                        if not chunk:
                            file.close()
                            break
                        peer_socket.sendall(chunk)
                        time.sleep(1)
            lock.release()
            break
    peer_socket.close()
        
#read local chunks file only
def read_file_by_lines():
    with open(os.path.join(folder_path, 'local_chunks.txt'), 'r') as file:
        lines = file.readlines()
    file.close()
    return lines

def hash_whole_file(filename):
    with open(os.path.join(folder_path, filename), 'rb') as file:
        data = file.read()
    file.close()
    return hashlib.sha1(data).hexdigest()

def accepting_peers():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.bind(("localhost",transfer_port))
    client_socket.listen()
    while True:
        peer_socket, addr = client_socket.accept()
        t = threading.Thread(target=process_peer, args=(peer_socket,))
        t.start()
        
def find_missing_chunks():
    tracker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tracker_socket.connect((tracker_ip, tracker_port))
    total_chunk = send_chunks_to_tracker(tracker_socket)
    for i in range(total_chunk):
        if (i + 1) not in local_chunks.keys():
            missing_chunks.append((i+1))

    while len(missing_chunks) > 0:
        print ("Im in here!")
        ind = missing_chunks.pop()
        response = request_info_from_tracker(ind, tracker_socket).split(',')
        print ('ha?')
        print(response)
        if (response[0] != 'CHUNK_LOCATION_UNKNOWN'):
            useful_peers = response[3:]
            print (useful_peers)
            peer_rand =  random.randint(0, (len(useful_peers)/2 - 1))
            peer_rand = 2 * peer_rand
            peer_ip = useful_peers[peer_rand]
            peer_port = int(useful_peers[peer_rand+1])
            print("randin is " + str(peer_rand) + " and the port number is " + response[peer_rand+1])
            new_file_name = 'chunkNo_' + str(ind)
            request_chunks_from_peer(peer_ip, peer_port, ind, new_file_name)
            update_tracker(ind, new_file_name, tracker_socket)
        else:
            missing_chunks.insert(0, ind)
    tracker_socket.close()



if __name__ == "__main__":
    tempF, tempTP, tempN = parseCLA()
    folder_path = tempF
    transfer_port = tempTP
    entity_name = tempN
    t = threading.Thread(target=accepting_peers)
    t.start()
    f =threading.Thread(target=find_missing_chunks)
    f.start()