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
	ft = [] * 7
	num_ft = 0
	sock = [] * 7
	predecessor = None

	# join()
	def __init__(self, node_id):
		self.node_id = node_id

		#node 0 will be initialized with all keys
		if node_id is 0:
			for x in range(0, 256):
				self.keys[x] = 1
			for x in range(0, 8):
				self.ft[x] = node_id
			self.predecessor = node_id

		else:
			# setup connection
			self.initConn()

			#send message to node 0 to help find myNode find its predecessor
			print "----------------------------------"
			print self.sock
			print "----------------------------------"
			self.sock[0].sendall("find_successor " + str(self.node_id))
			#1. initialize the finger table
			#2. initialize predecessor
			#3. Update the predecessor of existing nodes
			#4. Update the finger table of existing nodes
			#5. Transfer keys to node_id

		#after setting up is complete, setup connection to appropriate nodes + coordinator

		#client thread - sets up connection to other nodes
		client_t=threading.Thread(target = self.clientThread, args = (node_id,))
		client_t.start()
		
		#server thread - receives connection from other nodes
		server_t=threading.Thread(target = self.serverThread, args = (node_id,))
		server_t.start()



	#setup a connection to all nodes in finger table + coordinator
	def initConn(self):
		print 'Setting up node'
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
		for x in range(0, self.num_ft):
			try:
				#create an AF_INET, STREAM socket (TCP)
				self.sock[x] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			except socket.error, msg:
				print 'Failed to create socket. Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1]
				sys.exit();
			#node port num = 8000 + node_id
			self.sock[x].connect(globals.coord_ip, globals.coord_port + self.ft[x])
			print 'Socket Connected to node' + self.ft[x]

		#register client to the server
		if(c_client.sendall("registration " + str(self.node_id))==None):
			print '%s connected to server' % self.node_id
		else:
			print 'client registration incomplete'

	#receives messages from other nodes
	def recvThread(node_id):
		#continuously receive data from the nodes
		while(globals.keep_alive):
			data = conn.recv(1024)
		
			buf = data.split(' ')
			#if node 0 asked to find a node's successor
			if(buf[0] == "find_successor"):
				find_successor(buf[1])

	#receives connection from other nodes
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

		while(globals.keep_alive):
			conn, addr = s_server.accept()
			print 'Connected With '  + addr[0] + ':' + str(addr[1])
			thread.start_new_thread(recvThread, (conn, str(addr[1])))

		conn.close()
		s_server.close()

	#thread representing a single node
	def nodeThread(node_id, cmd):
		if(cmd=="join"):
			join(node_id)

	def find_successor(req_node):
		pass
		#
	def find_predecessor(node_id):
		pass

	#this function removes node with the id "node_id" from the system
	def remove_node(node_id):
		pass
		#somehow remove the node
	#this function finds key_id
	def find_key(node_id, key_id):
		pass
		#somehow find the key
	

