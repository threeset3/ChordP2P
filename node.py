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
	sock = [None] * 8
	predecessor = None
	coord = None

	# join()
	def __init__(self, node_id):
		self.node_id = node_id

		#node 0 will be initialized with all keys
		if node_id is 0:
			self.keys = [1] * 256
			self.ft = [node_id] * 8
			self.predecessor = node_id

		else:
			# setup connection to 0th node
			self.init_base()

			#send message to node 0 to help find myNode find its predecessor
			self.sock[0].send("find_successor " + str(self.node_id))
			#1. initialize the finger table
			#2. initialize predecessor
			#3. Update the predecessor of existing nodes
			#4. Update the finger table of existing nodes
			#5. Transfer keys to node_id
		
		# initialize finger table
		self.init_finger_table()

		#server thread - receives connection from other nodes
		server_t=threading.Thread(target = self.serverThread, args = ())
		server_t.start()


	## initialize conneciton to 0th node
	def init_base(self):
		try:
			self.sock[0] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		except socket.error, msg:
			print 'Failed to create socket. Error code: ' + str(msg[0]) + ' ' + str(msg[1])
			sys.exit();
		
		try:
			self.sock[0].connect((globals.coord_ip , globals.coord_port + 1)) 
		except socket.error, msg:
			print 'Failed to connect socket. Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1]
			sys.exit();

		self.sock[0].send("registration " + str(self.node_id))
		print '[Node %d] Connected to node 0.\n' % self.node_id

	#setup a connection to coordinator
	def init_coord(self):
		#setup connection to coordinator
		try:
			#create an AF_INET, STREAM socket (TCP)
			self.coord = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		except socket.error, msg:
			print 'Failed to create socket. Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1]
			sys.exit();

		self.coord.connect((globals.coord_ip , globals.coord_port))
		print 'Socket Connected to ' + globals.coord_ip

		#register client to the server
		if(self.coord.send("registration " + str(self.node_id))==None):
			print '%s connected to server' % self.node_id
		else:
			print 'client registration incomplete'

	def init_finger_table(self):
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

	#receives messages from other nodes
	def recvThread(self, conn):
		#continuously receive data from the nodes
		while(globals.keep_alive):
			data = conn.recv(1024)
		
			buf = data.split(' ')
			#if node 0 asked to find a node's successor
			if(buf[0] == "find_successor"):
				self.find_successor(buf[1])
			elif(buf[0]  == "registration"):
				print buf
				print '[Node %d] Connection identified as %s\n' % (self.node_id, buf[1])

	#receives connection from other nodes
	def serverThread(self):
		s_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		node_port = globals.coord_port + self.node_id + 1
		try: # setup server socket
			s_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			s_server.bind((globals.coord_ip, node_port))
	
		# if server setup fail
		except socket.error , msg:
			print '[[ Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1] + ' ]]'
			sys.exit()

		print '[Node %d] Socket bind complete.\n' % self.node_id
		s_server.listen(32)
		print '[Node %d] Socket listening..\n' % self.node_id

		while(globals.keep_alive):
			conn, addr = s_server.accept()
			print '[Node %d] Connected\n' % self.node_id
			thread.start_new_thread(self.recvThread, (conn,))

		conn.close()
		s_server.close()

	def find_successor(self, req_node):
		print '[Node %d] find_successor(%s) called.\n' % (self.node_id, req_node)
		#
	def find_predecessor(self, node_id):
		pass

	#this function removes node with the id "node_id" from the system
	def remove_node(self, node_id):
		pass
		#somehow remove the node
	#this function finds key_id
	def find_key(self, node_id, key_id):
		pass
		#somehow find the key
	

