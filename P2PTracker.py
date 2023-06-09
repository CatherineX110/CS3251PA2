import socket
import threading
import time
import logging

#logging
logging.basicConfig(filename="logs.log", format="%(message)s", filemode="a")
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
#Global Variables
#check_list = {file_hash: {file_hash: [(ip, port)...]}}
check_list = {}
#chunk_list = {chunk_index: (file_hash, [(ip,port)...])}
chunk_list = {}
lock = threading.Lock()
port_num = 5100
host = '127.0.0.1'

def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port_num))
    server_socket.listen()
    while True:
        client_socket, addr = server_socket.accept()
        t = threading.Thread(target=process_client_requests, args=(client_socket,))
        t.start()

#Processing each client request
def process_client_requests(client_socket):
    while True:
        data = client_socket.recv(1024)
        if not data:
            break
        #divide into two cases
        text = data.decode()
        if text.startswith("LOCAL_CHUNKS"):
            process_chunks(text)
        if text.startswith("WHERE_CHUNK"):
            find_chunks(client_socket, text)

    client_socket.close()

#Process local chunks, adding to check_list and chunk_list for corresponding chunks
def process_chunks(text):
    print ("hahahha " + text + " hahahahaha")
    request_type, chunk_index, file_hash, ip_addr, port = text.split(',')
    print ("client send me a " + request_type + " for chunk no. " + chunk_index + "with hash" + file_hash)
    chunk_index = int (chunk_index)

    lock.acquire()
    if chunk_index not in check_list:
        check_list[chunk_index] = {}
    if file_hash not in check_list[chunk_index]:
        check_list[chunk_index][file_hash] = []
    
    check_list[chunk_index][file_hash].append((ip_addr, port))
    if len(check_list[chunk_index][file_hash]) >= 2:
        #if first time agreement or (same chunk and same file_hash)
        if (chunk_index in chunk_list and file_hash == chunk_list[chunk_index][0]) or (chunk_index not in chunk_list):
            chunk_list[chunk_index] = (file_hash, check_list[chunk_index][file_hash])
    lock.release()

#Find if target chunk exists in chunk_list
def find_chunks(client_socket, text):
    request_type, chunk_index = text.split(',')
    chunk_index = int (chunk_index)

    lock.acquire()
    if chunk_index in chunk_list:
        file_hash, peers = chunk_list[chunk_index]
        response = "GET_CHUNK_FROM," + str(chunk_index) + "," + str(file_hash)
        for ip_addr, port in peers:
            response += ","
            response += str(ip_addr)
            response += ","
            response += str(port)

    else:
        response = "CHUNK_LOCATION_UNKNOWN," + str(chunk_index)
    logger.info("P2PTracker," + response)
    lock.release()
    client_socket.sendall(response.encode())
    time.sleep(1)


if __name__ == "__main__":
    start_server()

