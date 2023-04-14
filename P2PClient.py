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
tracker_ip = '127.0.0.1'
tracker_port = 5100
lock = threading.Lock()
folder_path = ''
transfer_port = -1
entity_name = ''
missing_chunks = []
total_chunk = -1

def parseCLA():
    parser = argparse.ArgumentParser()
    parser.add_argument('-folder', required=True, help='Full path of the folder')
    parser.add_argument('-transfer_port', type=int, required=True, help='Transfer port number')
    parser.add_argument('-name', required=True, help='Entity name')
    
    args = parser.parse_args()
    return args.folder, args.transfer_port, args.name

#Sending LOCAL_CHUNKS request to tracker
def send_chunks_to_tracker():
    client_socket = socket.socket()
    client_socket.connect((tracker_ip, tracker_port))
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
        lock.release()
        request = "LOCAL_CHUNKS," + str(chunk_index) + "," + hash + "," + tracker_ip + "," + str(transfer_port)
        client_socket.sendall(request.encode())
        time.sleep(0.005)
        lock.acquire()
        logger.info(entity_name + "," + request)
        lock.release()
	
def update_tracker(chunk_index, filename):
    lock.acquire()
    hash = hash_whole_file(filename)
    lock.release()
    client_socket = socket.socket()
    client_socket.connect((tracker_ip, tracker_port))
    request = "LOCAL_CHUNKS," + str(chunk_index) + "," + hash + "," + tracker_ip + "," + str(transfer_port)
    client_socket.sendall(request.encode())
    time.sleep(0.005)
    lock.acquire()
    logger.info(entity_name + "," + request)
    lock.release()

def request_info_from_tracker(chunk_index):
    client_socket = socket.socket()
    client_socket.connect((tracker_ip, tracker_port))
    request = "WHERE_CHUNK," + str(chunk_index)
    client_socket.sendall(request.encode())
    time.sleep(0.005)
    lock.acquire()
    logger.info(entity_name + "," + request)
    lock.release()
    response = client_socket.recv(1024).decode().strip()
    return response

def request_chunks_from_peer(peer_ip, peer_port, chunk_index, filename):
    peer_socket = socket.socket()
    print (peer_ip + " ip and port " + str(peer_port))
    peer_socket.connect((peer_ip, peer_port))
    request = "REQUEST_CHUNK," + str(chunk_index)
    peer_socket.sendall(request.encode())
    time.sleep(0.005)
    lock.acquire()
    logger.info(entity_name + "," + request)
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
    
def process_peer(peer_socket):
    while True:
        request = peer_socket.recv(1024).decode()
        if not request:
            break
        if request.startswith("REQUEST_CHUNK"):
            typeReq, chunk_index = request.split(',')
            filename = None
            lock.acquire()
            with open(os.path.join(folder_path, 'local_chunks.txt'),'r') as file:
                for line in file:
                    line = line.strip()
                    if line.startswith(chunk_index) and not line.endswith("LASTCHUNK"):
                        ind, filename = line.split(',')
                        break
            file.close()
            if filename:
                with open(os.path.join(folder_path, filename), 'rb') as file:
                    while True:
                        chunk = file.read(1024)
                        if not chunk:
                            file.close()
                            break
                        peer_socket.sendall(chunk)
                        time.sleep(0.005)
            lock.release()
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
    client_socket = socket.socket()
    client_socket.bind(('127.0.0.1',transfer_port))
    client_socket.listen()
    while True:
        peer_socket, addr = client_socket.accept()
        t = threading.Thread(target=process_peer, args=(peer_socket,))
        t.start()
        
def find_missing_chunks():
    total_chunk = send_chunks_to_tracker()
    for i in range(total_chunk):
        missing_chunks.append((i+1))
    lines = read_file_by_lines()
    for line in lines:
        chunInd, fileName = line.split(',')
        if (fileName == "LASTCHUNK"):
            break
        missing_chunks.remove(int(chunInd))
    print ("hi there " + str(len(missing_chunks)))
    while len(missing_chunks) > 0:
        print ("Im in here!")
        ind = missing_chunks.pop()
        response = request_info_from_tracker(ind).split(',')
        print ('ha?')
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
            update_tracker(ind, new_file_name)
        else:
            missing_chunks.insert(0, ind)



if __name__ == "__main__":
    tempF, tempTP, tempN = parseCLA()
    folder_path = tempF
    transfer_port = tempTP
    entity_name = tempN
    t = threading.Thread(target=accepting_peers)
    t.start()
    find_missing_chunks()