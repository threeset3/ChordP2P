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
	# join()
	def __init__(self, node_id):
		self.node_id = int(node_id)
		self.key_start = None
		self.key_end = None
		self.ft = [None] * 8
		self.sock = {}
		self.predecessor = None

		#socket to coordinator
		self.coord = None
		#socket to node 0
		self.node0 = None

		#flag that indicates if finger table is done building
		self.build_done = 0
		#flag that indicates if query reply to find_predecessor has arrived
		self.reply_predecessor=0
		#buffer that contains "pred succ"
		self.msg_predecessor = ""
		

		#server thread - receives connection from other nodes
		server_t=threading.Thread(target = self.serverThread, args = ())
		server_t.start()
		self.join()

	def join(self):
		#node 0 will be initialized with all keys
		if self.node_id is 0:

			self.key_start = 0
			self.key_end = 255
			self.ft = [0] * 8
			self.predecessor = self.node_id
			self.build_done = 1

		else:
			# setup connection to 0th node and get finger table from node 0
			self.init_base()
			# wait until finger table is built
			while(not self.build_done):
				pass
			#connect to nodes in the finger table
			self.conn_finger_table()
			#4. Update the finger table of existing nodes
			#self.update_others()
			#5. Transfer keys to node_id

		self.init_coord()

	## initialize conneciton to 0th node
	def init_base(self):
		try:
			self.node0 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		except socket.error, msg:
			print 'Failed to create socket. Error code: ' + str(msg[0]) + ' ' + str(msg[1])
			sys.exit();
		
		try:
			self.node0.connect((globals.coord_ip , globals.coord_port + 1)) 
		except socket.error, msg:
			print 'Failed to connect socket. Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1]
			sys.exit();

		#listens from node 0 - ALWAYS
		thread.start_new_thread(self.recvThread, (self.node0,))

		#ask node 0 to fill self's finger tabl
		self.request_FT()

	#receives finger table from node 0
	def request_FT(self):
		self.node0.sendall("registration " + str(self.node_id))
		print '[Node %d] Connected to node 0.\n' % self.node_id
	#joined node fill in his local table
	def build_table(self, msg):
		while(globals.printing):
			pass
		globals.printing=1
		print '[Node %d]New table entries: '%self.node_id
		print msg
		print '\n'
		print '[Node %d] Building Finger Table...\n'%self.node_id
		req_ft = msg.split(' ')
		for x in range(1, 9):
			self.ft[x-1] = int(req_ft[x])
			#to avoid re-connecting to node 0
			if req_ft[x-1] == 0:
				self.sock[0] = self.node0
		self.predecessor = int(req_ft[9])
		print '[Node %d] New Finger Table by Builder is: '%self.node_id
		print self.ft
		print '\n'
		print '[Node %d] Predecessor is %s' %(self.node_id, self.predecessor)
		globals.printing = 0
		#notify that finger table has been built
		self.build_done = 1

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

		#register client to the coordinator
		if(self.coord.sendall("registration " + str(self.node_id))==None):
			print '[Node %s] connected to coordinator' % self.node_id
		else:
			print 'client registration incomplete'

		#server thread - receives messages from the coordinator
		server_t=threading.Thread(target = self.recvThread, args = (self.coord,))
		server_t.start()

	#!!!!!!NOT WORKING PROPERLY!!!!! - not setting up connection after table is updated
	#setup connection to nodes in the finger table
	def conn_finger_table(self):
		#setup connection to nodes in fingertable
		for x in range(0, 8):
			#don't connect if connection exists or to itself
			if(self.sock.has_key(self.ft[x]) or self.ft[x] == self.node_id):
				#print '[Node %d] Has key: %d with value: '%(self.node_id, self.ft[x])
				#print self.sock[self.ft[x]]
				#print '\n'
				continue
			try:
				#create an AF_INET, STREAM socket (TCP)
				self.sock[self.ft[x]] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				print '[Node %d]Saved socked for node %d\n' %(self.node_id, self.ft[x])
			except socket.error, msg:
				print 'Failed to create socket. Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1] + '\n'
				sys.exit();
			node_port = int(globals.coord_port) + int(self.ft[x]) + 1
			self.sock[self.ft[x]].connect((globals.coord_ip, node_port))
			print '[Node %d] Connected to Node_%d\n' %(self.node_id, self.ft[x])
			
			#receive messages from the newly connected node
			server_t=threading.Thread(target = self.recvThread, args = (self.sock[self.ft[x]],))
			server_t.start()

	#receives messages from other nodes that CONNECTED TO THIS NODE!!!
	def recvThread(self, conn):
		#continuously receive data from the nodes
		while(1):
			data = conn.recv(1024)
			#print '[NODE %d] GOT MESSAGE: '%self.node_id
			#print data
			#print '\n'

			buf = data.split(' ')
			#--------------ONLY NODE 0--------------

			#----------REQUEST - if node 0 asked to find a node's successor---------
			if(buf[0]  == "registration"):
				#print '[Node %d] Connection identified as %s\n' % (self.node_id, buf[1])
				req_node = int(buf[1])
				self.sock[req_node] = conn
				print '[Node %d]Saved socked for node %d: \n' %(self.node_id, req_node)
				newinfo = self.init_finger_table(int(buf[1]))
				newinfo = "ft " + newinfo
				if(conn.sendall(newinfo)==None):
					pass
					#print '[node 0]finger table sent to Node_%d\n'%int(buf[1])
				else:
					print '[ERROR] Node 0 failed to send finger table to Node_%d\n'%int(buf[1])
			#--------------END-------------------
			#REPLY - node 0 send self its node table
			elif(buf[0]=="ft"):
				#print '[Node %d] Received finger table message from node 0:\n'%self.node_id
				#print data
				self.build_table(data)

			#-----------REPLY - to find_predecessor------------
			elif(buf[0] == "found_predecessor"):
				#do something fun
				print '[Node %d] received predecessor reply\n'%self.node_id
				predecessor = buf[1]
				successor = buf[2]
				self.msg_predecessor = predecessor + ' ' + successor
				self.reply_predecessor = 1

			#------------------REQUEST - find_predecessor----------------
			elif(buf[0] == "find_predecessor"):
				#print '[Node %d] received request "find_predecessor" for ID_%d \n' %(self.node_id, int(buf[1]))
				msg = self.find_predecessor(int(buf[1]))
				#add the header
				msg = "found_predecessor " + msg
				print '[Node %d] reply to find_pred: '%self.node_id + msg + '\n'
				conn.sendall(msg)
				print '[Node %d] reply Complete \n'%self.node_id 

			#---------------REQUEST - node is told what its predecessor is---------
			elif(buf[0] == "your_predecessor"):
				print '[Node %d] my old predecessor is: %d\n'%(self.node_id, self.predecessor)
				self.predecessor = int(buf[1])
				print '[Node %d] my new predecessor is: %d\n'%(self.node_id, self.predecessor)

			#---------------REQUEST - node is told what its succesor is---------
			elif(buf[0] == "your_successor"):
				print '[Node %d] my old successor is: %d\n'%(self.node_id, self.ft[0])
				self.ft[0] = int(buf[1])
				print '[Node %d] my new successor is: %d\n'%(self.node_id, self.ft[0])

				#connect to new nodes if required
				self.conn_finger_table()

				while(globals.printing):
					pass
				globals.printing = 1
				print '[Node %d] my new finger table is: \n'%self.node_id
				self.print_ft()
				globals.printing = 0

			#--------------COMMAND - node is told to print keys-----------
			if(buf[0]=="show"):
				print '[Node %d] FINGER TABLE:\n' %self.node_id
				self.print_ft()
				print '[Node %d] KEYS:\n' %self.node_id

				if self.ft[0] != 0:
					self.sock[self.ft[0]].sendall("show all")

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
		#print '[Node %d] find_init_finger_table(%s) called.\n' % (self.node_id, req_node)

		#node_0 builds req_node's ft and sends it back in message format
		req_ft = [None]*8

		msg = self.find_successor(req_node+1)
		
		# msg will contain "PRD SUCC"
		msg = msg.split(' ')
		print '[Node %d] Received pred succ: '%self.node_id
		print msg
		print '\n'
		predecessor = int(msg[0])
		successor = int(msg[1])
		req_ft[0] =  successor
		# WARNING!!!: if I tell successor/predecessor about req_node, then it's conflicting with building the rest of the table

		#build the rest of the finger table
		for i in range(0, 7):
			finger_id = req_node+math.pow(2, i+1)

			if(req_ft[i] >= finger_id):
				#successor(i+1) == successor(i)
				req_ft[i+1] = req_ft[i]
			elif(finger_id <= 255 and req_ft[0] == 0):
				#if finger_id is the current biggest key -then wrap back to node 0
				req_ft[i+1] = req_ft[i]

			else: #finger_id > req_ft[i]
				#successor(i+1) > successor(i)
				if(finger_id > 255): #wrap back
					finger_id = finger_id -255

				msg = self.find_successor(finger_id)
				msg = msg.split(' ')
				req_ft[i+1] = msg[1]

		#tell successor that req_node is its predecessor
		if(successor == 0):
			self.predecessor = req_node
		else:
			self.sock[successor].sendall("your_predecessor " + str(req_node))
		#tell predecessor that req_node is its successor
		if(predecessor==0):
			self.ft[0] = req_node
		else:
			self.sock[predecessor].sendall("your_successor " + str(req_node))
		
		#send back whole table back to requesting node
		retFt = (" ".join(str(e) for e in req_ft))
		return (retFt + ' ' + str(predecessor))

	def find_successor(self, req_node):
		#print '[Node %d] find_successor(%s) called.\n' % (self.node_id, req_node)
		msg = self.find_predecessor(req_node)

		#print '[Node %d]find_successor returning:'%self.node_id + msg
		return msg
	
	def find_predecessor(self, req_node):
		#print '[Node %d] find_predecessor(%s) called.\n' % (self.node_id, req_node)
		while(globals.printing):
			pass
		globals.printing = 1
		print '[Node %d] My Ft\n' %self.node_id
		self.print_ft()
		globals.printing = 0
		node = self.node_id

		retVal = "OOPS!"
		if(self.node_id == self.ft[0] and self.node_id==self.predecessor):
			#if node 0 is the only node in the system - case 1
			retVal = '0 0'
		elif(req_node > self.node_id):
			if(req_node <=self.ft[0]):
				#new node falls between self and self.successor
				print '[Node %d] find_pred_case1: '%self.node_id + str(node) + ' ' + str(self.ft[0]) + '\n'
				retVal = str(node) + ' ' + str(self.ft[0])
			elif(self.ft[0] == 0):
				#new node is the new biggest node
				print'[Node %d] find_pred_case2\n' %self.node_id
				retVal = (str(self.node_id) + " 0")
			else: # no match, pass a request to a node that might be able to handle
				node = self.closest_preceding_finger(req_node)
				print '[Node %d]closest_preceding_finger: %d\n' %(self.node_id, node)
				self.reply_predecessor = 0
				self.sock[node].send("find_predecessor "+str(int(req_node)))
				#print'[Node %d]sent find_predecessor request to node_%d\n' %(self.node_id, node)
				
				#wait for reply
				while(not self.reply_predecessor):
					pass
				#get the response
				retVal = self.msg_predecessor
				self.msg_predecessor = ""
				#print '[Node %d] predecessor reply is '%self.node_id + str(retVal) + '\n'
		return str(retVal)

	#returns the node preceding req_node
	def closest_preceding_finger(self, req_node):
		for x in list(reversed(range(8))):
			if(self.ft[x] > self.node_id and self.ft[x] < req_node):
				return self.ft[x]
		return self.node_id

	#new node will help others to update their finger table
	def update_others(self):
		for x in range(0, 8):
			#do something cool
			print '[Node %d] Updating others\n'%self.node_id


	#this function removes node with the id "node_id" from the system
	def remove_node(self, node_id):
		pass
		#somehow remove the node
	#this function finds key_id
	def find_key(self, node_id, key_id):
		pass
		#somehow find the key
	def print_ft(self):
		print self.ft
		print self.ft[0]
		print '\n'

