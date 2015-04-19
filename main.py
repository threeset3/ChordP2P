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
	while(1):
		data = conn.recv(1024)
		buf = data.split(' ')

		#if registration message, indicate sender node as active
		if(buf[0] == "registration"):
			new_node = int(buf[1])
			if(globals.active_nodes[new_node] == 0):
				globals.active_nodes[new_node] = 1
				globals.sock[new_node] = conn
				print '[Coord] Connected with node %d'%new_node
				print '[Coord] Marking node %d as active\n' % new_node
				print '[Coord] Can take new command now :)\n'

		#------Node requested coordinator to forward a message to a different node-----
		elif(buf[0] == "forward_to"):
			dest = int(buf[1])
			msg = buf[2]+' '+buf[3] +' '+buf[4]
			globals.sock[dest].sendall(msg)

#adds a node to Chord
def join_handler(node_id):
	#check if the node already exists
	if(globals.active_nodes[node_id]):
		print("Node already in Chord!\n")
		return

	#create a thread representing the node
	myNode = Node(node_id)
	globals.active_nodes[node_id] = True

#removes a node from Chord
def leave_handler(node_id):

	#check if the node exists
	if(globals.active_nodes[node_id] == 0):
		print("Node doesn't exist!\n")
		return

	#tell node_id to remove itself from Chord
	msg = "leave"
	globals.sock[node_id].sendall(msg)
	try:
		globals.sock[node_id].sendall(msg)
	except socket.error , msg2:
		print '[[ Send failed : ' + str(msg2[0]) + ' Message ' + msg2[1] + ' ]]' + '\n'
		sys.exit()

	#indicate that this node is inactive
	globals.active_nodes[node_id] = 0

#tells node 'node_id' to find key with id 'key_id'
def find_handler(node_id, key_id):
	#check if node_id exists
	if(globals.active_nodes[node_id] == 0):
		print("Given node not in the system!\n")
		return
	#tell node_id to find key_id
	msg = "find key_id"
	globals.sock[node_id].sendall(msg)
	try:
		globals.sock[node_id].sendall(msg)
	except socket.error , msg2:
		print '[[ Send failed : ' + str(msg2[0]) + ' Message ' + msg2[1] + ' ]]' + '\n'
		sys.exit()
def show_handler(node):
	if node is "all":
		print '[Coord] sending show-all command to 0\n'
		globals.sock[0].sendall("show all")
	else:
		if(globals.sock[node] != None):
			print '[Coord] sending show command to %d\n' %node
			globals.sock[node].sendall("show you")

#receives connection from the nodes
def server():
	global s_server, server_port, sock, num_clients
	s_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	try: # setup server socket
		s_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		s_server.bind((globals.coord_ip, int(globals.coord_port)))
	
	# if server setup fail
	except socket.error , msg:
		print '[[ Bind failed. Error Code: ' + str(msg[0]) + ' Message ' + msg[1] + ' ]]' + '\n'
		sys.exit()

	print '[Coord] Socket bind complete.\n'
	s_server.listen(32)
	print '[Coord] Socket listening on ' + str(globals.coord_port)
	globals.coord_initialized = 1;

	while(1):
		conn, addr = s_server.accept()
		recv_t = threading.Thread(target=recvThread, args=(conn, str(addr[1]),))
		recv_t.setDaemon(True)
		recv_t.start()

	conn.close()
	s_server.close()

def main():
	#create the global vars
	globals.init()

	#thread for intializing the coordinator to talk with nodes
	server_thread = threading.Thread(target=server, args=())
	server_thread.setDaemon(True)
	server_thread.start()

	while(not globals.coord_initialized):
		pass

	# set up node 0
	join_handler(0);

	while(1):
		userInput = raw_input('>>> ');
		cmd = userInput.split(' ');
		if cmd[0] == "join" and cmd[1] != None:
			join_handler(int(cmd[1]))
		elif cmd[0] == "find" and cmd[1] != None and cmd[2] != None:
			find_handler(cmd[1], cmd[2])
		elif cmd[0] == "leave" and cmd[1] != None:
			leave_handler(cmd[1])
		elif cmd[0] == "show" and cmd[1] == "all":
			show_handler("all")
		elif cmd[0] == "show" and cmd[1] != None:
			show_handler(int(cmd[1]))
		elif cmd[0] == "quit":
			break;
		elif cmd[0] == "":
			pass
		else:
			print 'invalid Input'
	print '[Coord] I FUCKING QUIT!\n'
	server_thread.join()
	sys.exit()
	

#execution starts here
main()
