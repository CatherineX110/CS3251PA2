import socket
import threading
import time
import logging
import argparse
import hashlib
import os
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
	lines = read_file_by_lines()
	for line in lines:
		line = line.strip()
		if line.endswith("LASTCHUNK"):
			break
		chunk_index, filename = line.split(',')
		hash = hash_whole_file(filename)
		request = "LOCAL_CHUNKS," + str(chunk_index) + "," + hash + "," + str(socket.gethostbyname(socket.gethostname)) + "," + str(transfer_port) + "\n"
		client_socket.sendall(request.encode())
		time.sleep(0.005)
		logger.info(entity_name + "," + request)
	
def update_tracker(chunk_index, filename):
	hash = hash_whole_file(filename)
	client_socket = socket.socket()
	client_socket.connect((tracker_ip, tracker_port))
	request = "LOCAL_CHUNKS," + str(chunk_index) + "," + hash + "," + str(socket.gethostbyname(socket.gethostname)) + "," + str(transfer_port) + "\n"
	client_socket.sendall(request.encode())
	time.sleep(0.005)
	logger.info(entity_name + "," + request)

def request_info_from_tracker(chunk_index):
	client_socket = socket.socket()
	client_socket.connect((tracker_ip, tracker_port))
	request = "WHERE_CHUNK," + str(chunk_index) + "\n"
	client_socket.sendall(request.encode())
	time.sleep(0.005)
	logger.info(entity_name + "," + request)
	response = client_socket.recv(1024).decode().strip()
	return response

def request_chunks_from_peer(peer_ip, peer_port, chunk_index, filename):
	peer_socket = socket.socket()
	peer_socket.connect((peer_ip, peer_port))
	request = "REQUEST_CHUNK," + str(chunk_index) + "\n"
	peer_socket.sendall(request)
	time.sleep(0.005)
	logger.info(entity_name + "," + request)
	with open(os.path.join(folder_path, filename), 'wb') as file:
		while True:
			response = peer_socket.recv(1024)
			if not response:
				break
			file.write(response)
			

def read_file_by_lines():
	with open(os.path.join(folder_path, 'local_chunks.txt'), 'r') as file:
		lines = file.readlines()
	return lines

def hash_whole_file(filename):
	with open(os.path.join(folder_path, filename), 'rb') as file:
		data = file.read()
	return hashlib.sha1(data).hexdigest()


