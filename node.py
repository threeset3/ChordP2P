import socket
import sys
import threading
import thread
import time
import datetime
import random
from collections import deque
import math
#my imports
import globals

class Node:
	keys = []
	ft = [None] * 8
	num_ft = 0
	sock = [None] * 256
	predecessor = None
	coord = None
	# join()
	def __init__(self, node_id):
		self.node_id = node_id
		#node 0 will be initialized with all keys
		if node_id is 0:
			self.keys = range(256)
			self.ft = [node_id] * 8
			self.predecessor = node_id

		else:
			# setup connection to 0th node
			self.init_base()
			#1. initialize the finger table
			#2. initialize predecessor
			#3. Update the predecessor of existing nodes
			#4. Update the finger table of existing nodes
			#5. Transfer keys to node_id
		
		# initialize finger table
		self.conn_finger_table()
		self.init_coord()

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

		self.sock[0].sendall("registration " + str(self.node_id))
		print '[Node %d] Connected to node 0.\n' % self.node_id
		msg = self.sock[0].recv(1024)
		print msg

	#setup a connection to coordinator
	def init_coord(self):
		#setup connection to coordinator
		try:
			#create an AF_INET, STREAM socket (TCP)
			self.coord = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		except socket.error, msg:
			print 'Failed to create socket. Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1] + '\n'
			sys.exit();

		try:
			self.coord.connect((globals.coord_ip , globals.coord_port))
		except socket.error, msg:
			print 'Failed to connect socket. Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1] + '\n'
			sys.exit();

		print 'Socket Connected to coordinator\n'

		#register client to the coordinator
		if(self.coord.sendall("registration " + str(self.node_id))==None):
			print '[Node %s] connected to coordinator' % self.node_id
		else:
			print 'client registration incomplete'

		#server thread - receives messages from the coordinator
		server_t=threading.Thread(target = self.coord_recvThread, args = ())
		server_t.start()


	#build the finger table of node req_node
	def init_finger_table(self, req_node):
		#node_0 builds req_node's ft and sends it back in message format
		req_ft = [None]*8
		req_ft[0] = self.find_sucessor(req_node+1)

	def conn_finger_table(self):
		#setup connection to nodes in fingertable
		for x in range(1, self.num_ft):
			try:
				#create an AF_INET, STREAM socket (TCP)
				if(self.sock[x] != None):
					continue
				self.sock[x] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			except socket.error, msg:
				print 'Failed to create socket. Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1] + '\n'
				sys.exit();
			#node port num = 8000 + node_id
			node_port = globals.port + self.ft[x] + 1
			self.sock[x].connect(globals.coord_ip, node_port)
			print 'Socket Connected to node' + self.ft[x]		
	def coord_recvThread(self):
		while(1):
			data = self.coord.recv(1024)
			buf = data.split(' ')
			if(buf[0]=="show-all"):
				print '[Node %d] FINGER TABLE:\n' %self.node_id
				print self.ft
				print '[Node %d] KEYS:\n' %self.node_id
				print self.keys

	#receives messages from other nodes
	def recvThread(self, conn):
		#continuously receive data from the nodes
		while(1):
			data = conn.recv(1024)
			
			buf = data.split(' ')
			#if node 0 asked to find a node's successor
			if(buf[0]  == "registration"):
				print '[Node %d] Connection identified as %s\n' % (self.node_id, buf[1])
				self.sock[int(buf[1])] = conn
				newinfo = self.init_finger_table(int(buf[1]))
				conn.sendall(newinfo)

			elif(buf[0] == "find_predecessor"):
				msg = self.find_predecessor(int(buf[1]))
				conn.sendall(msg)
			elif(buf[0] == "your_predecessor"):
				self.predecessor = int(buf[1])

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

		while(1):
			conn, addr = s_server.accept()
			print '[Node %d] Connected. Identifying..\n' % self.node_id
			thread.start_new_thread(self.recvThread, (conn,))

		conn.close()
		s_server.close()

	#build the finger table of node req_node
	def init_finger_table(self,req_node):
		print '[Node %d] find_init_finger_table(%s) called.\n' % (self.node_id, req_node)

		#node_0 builds req_node's ft and sends it back in message format
		req_ft = [None]*8

		msg = self.find_successor(req_node+1)
		msg = msg.split(' ')
		# msg will contain "PRD SUCC"

		predecssor = int(msg[0])
		successor = int(msg[1])
		req_ft[0] =  successor

		#tell successor that I'm your predecessor
		#if(successor == 0):
			#self.predecessor = req_node
		#else:
			#self.sock[successor].sendall("your_predecessor " + req_node)

		#build the rest of the finger table
		for i in range(0, 7):
			finger_id = req_node+math.pow(2, i+1)
			print 'FINGER ID: %d\n' %finger_id 
			if(finger_id > self.node_id and finger_id <= self.ft[i]):
				print 'No need to call find_successor\n'
				req_ft[i+1] = self.ft[i]
			else:
				msg = self.find_successor(finger_id)
				msg = msg.split(' ')
				req_ft[i+1] = msg[1]
		#send back whole table back to requesting node
		return (" ".join(str(e) for e in req_ft))

	def find_successor(self, req_node):
		print '[Node %d] find_successor(%s) called.\n' % (self.node_id, req_node)
		msg = self.find_predecessor(req_node)
		return msg
	
	def find_predecessor(self, req_node):
		print '[Node %d] find_predecessor(%s) called.\n' % (self.node_id, req_node)

		node = self.node_id

		if(self.node_id == self.ft[0] and self.node_id==self.predecessor):
			return str('0 0')
		elif(req_node > self.node_id and req_node <= self.ft[0]):
			return str(node + ' ' + self.ft[0])
		else:
			node = self.closest_preceding_finger(req_node)
			print 'closest_preceding_finger: %d\n' %node

			self.sock[node].sendall("find_predecessor "+str(req_node))
			#get the response
			data = self.sock[node].recv(1024)
			return data

	#returns the node preceding req_node
	def closest_preceding_finger(self, req_node):
		for x in list(reversed(range(8))):
			if(self.ft[x] > self.node_id and self.ft[x] < req_node):
				return self.ft[x]
		return self.node_id

	#this function removes node with the id "node_id" from the system
	def remove_node(self, node_id):
		pass
		#somehow remove the node
	#this function finds key_id
	def find_key(self, node_id, key_id):
		pass
		#somehow find the key
	

