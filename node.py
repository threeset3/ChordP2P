import socket
import sys
import threading
import thread
import time
import datetime
import random
from collections import deque

#my imports
import globals

class Node:
	keys = [] * 256
	ft = [] * 8
	num_ft = 0
	sock = [] * 8
	predecessor = None
	def __init__(self, node_id):
		self.node_id = node_id

#setup a connection to all nodes in finger table + coordinator
def clientThread(myNode, node_id):
	print 'Running client..'
	global c_client

	#setup connection to coordinator
	try:
		#create an AF_INET, STREAM socket (TCP)
		c_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	except socket.error, msg:
		print 'Failed to create socket. Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1]
		sys.exit();

	c_client.connect((globals.coord_ip , globals.coord_port))
	print 'Socket Connected to ' + globals.coord_ip

	#setup connection to nodes in fingertable
	for x in range(0, myNode.num_ft):
		try:
			#create an AF_INET, STREAM socket (TCP)
			myNode.sock[x] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		except socket.error, msg:
			print 'Failed to create socket. Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1]
			sys.exit();
		#node port num = 8000 + node_id
		myNode.sock[x].connect(globals.coord_ip, globals.coord_port + myNode.ft[x])
		print 'Socket Connected to node' + myNode.ft[x]

	#register client to the server
	if(c_client.sendall("registration " + node_id)==None):
		print '%s connected to server' % node_id
	else:
		print 'client registration incomplete'


def serverThread(node_id):
	s_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	node_port = globals.coordinator_port + node_id
	try: # setup server socket
		s_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		s_server.bind((globals.coord_ip, node_port))
	
	# if server setup fail
	except socket.error , msg:
		print '[[ Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1] + ' ]]'
		sys.exit()

	print 'Socket bind complete.'
	s_server.listen(32)
	print 'Socket listening..'

	while 1:
		conn, addr = s_server.accept()
		print 'Connected With '  + addr[0] + ':' + str(addr[1])
		thread.start_new_thread(clientThread, (conn, str(addr[1])))

		#keep track of the number of clients connected to server
		num_clients = num_clients + 1

	conn.close()
	s_server.close()

#thread representing a single node
def nodeThread(node_id):
	myNode = Node(node_id)
		#--------myNode will join the network----------
	#node 0 will be initialized with all keys
	if node_id is 0:
		for x in range(0, 256):
			myNode.keys[x] = 1
		for x in range(0, 8):
			myNode.ft[x] = node_id
		myNode.predecessor = node_id
	if node_id != 0:
		#1. initialize the predecessor
		#send message to node 0 to help find myNode find its predecessor
		myNode.sock[0].sendall("find_predecessor "+node_id)
		#2. initialize the finger table

		#3. Update the predecessor of existing nodes
		#4. Update the finger table of existing nodes
		#5. Transfer keys to node_id

		#after setting up is complete, setup connection to appropriate nodes + coordinator

		#client thread
		client_t=threading.Thread(target = clientThread, args = (myNode, node_id,))
		client_t.start()
		
	#server thread
	server_t=threading.Thread(target = serverThread, args = (node_id,))
	server_t.start()
def find_predecessor(node_id):
	pass

#this function creates a new node
def create_node(node_id):
	#thread representing the node
	node_t= threading.Thread(target=nodeThread, args = (node_id,))
	node_t.start()

#this function removes node with the id "node_id" from the system
def remove_node(node_id):
	pass
	#somehow remove the node
#this function finds key_id
def find_key(node_id, key_id):
	pass
	#somehow find the key
	

