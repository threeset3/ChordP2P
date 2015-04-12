#main.py

import socket
import sys
import threading
import thread
import time
import datetime
import random
from collections import deque

#my imports
from node import Node
import globals

#communicates with the connected node
def recvThread(conn, unique):
	#continuously receive data from the nodes
	while(globals.keep_alive):
		data = conn.recv(1024)
		
		buf = data.split(' ')

		#if registration message, indicate sender node as active
		if(buf[0] == "registration"):
			if(globals.active_nodes[int(buf[1])] == 0):
				globals.active_nodes[int(buf[1])] = 1
				globals.sock[int(buf[1])] = conn
		
#adds a node to Chord
def join_handler(node_id):
	#check if the node already exists
	if(globals.active_nodes[node_id]):
		print("node already in Chord!\n")
		return

	#create a thread representing the node
	myNode = Node(node_id)

#removes a node from Chord
def leave_handler(node_id):

	#check if the node exists
	if(globals.active_nodes[node_id] == 0):
		print("node doesn't exist!")
		return

	#tell node_id to remove itself from Chord
	msg = "leave"
	globals.sock[node_id].sendall(msg)
	try:
		globals.sock[node_id].sendall(msg)
	except socket.error , msg2:
		print '[[ Send failed : ' + str(msg2[0]) + ' Message ' + msg2[1] + ' ]]'
		sys.exit()

	#indicate that this node is inactive
	globals.active_nodes[node_id] = 0

#tells node 'node_id' to find key with id 'key_id'
def find_handler(node_id, key_id):
	#check if node_id exists
	if(globals.active_nodes[node_id] == 0):
		print("Given node not in the system!")
		return
	#tell node_id to find key_id
	msg = "find key_id"
	globals.sock[node_id].sendall(msg)
	try:
		globals.sock[node_id].sendall(msg)
	except socket.error , msg2:
		print '[[ Send failed : ' + str(msg2[0]) + ' Message ' + msg2[1] + ' ]]'
		sys.exit()

#receives connection from the nodes
def server():
	global s_server, server_port, sock, num_clients
	s_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	try: # setup server socket
		s_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		s_server.bind((globals.coord_ip, int(globals.coord_port)))
	
	# if server setup fail
	except socket.error , msg:
		print '[[ Bind failed. Error Code: ' + str(msg[0]) + ' Message ' + msg[1] + ' ]]'
		sys.exit()

	print 'Socket bind complete.'
	s_server.listen(32)
	print 'Socket listening..'

	while(globals.keep_alive):
		conn, addr = s_server.accept()
		print 'Connected With '  + addr[0] + ':' + str(addr[1])
		thread.start_new_thread(recvThread, (conn, str(addr[1])))

	conn.close()
	s_server.close()

def main():
	#create the global vars
	globals.init()

	#thread for intializing the coordinator to talk with nodes
	server_thread = threading.Thread(target=server, args=())
	server_thread.start()

	# set up node 0
	join_handler(0);

	while(globals.keep_alive):
		userInput = raw_input('>>> ');
		cmd = userInput.split(' ');
		if cmd[0] == "join" and cmd[1] != None:
			join_handler(int(cmd[1]))
		elif cmd[0] == "find" and cmd[1] != None and cmd[2] != None:
			find_handler(cmd[1], cmd[2])
		elif cmd[0] == "leave" and cmd[1] != None:
			leave_handler(cmd[1])
		elif cmd[0] == "show" and cmd[1] != None:
			pass
		elif cmd[0] == "show" and cmd[1] == "all":
			pass
		elif cmd[0] == "quit":
			globals.keep_alive = 0;
		elif cmd[0] == "":
			pass
		else:
			print 'invalid Input'

	server_thread.join()
	sys.exit()
	

#execution starts here
main()
