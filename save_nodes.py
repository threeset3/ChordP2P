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
		self.join_done = 0
		#buffer that contains "pred succ"
		self.msg_predecessor = "OOHO"
		

		#server thread - receives connection from other nodes
		server_t=threading.Thread(target = self.serverThread, args = ())
		server_t.start()

		self.init_coord()
		self.join()
		while(not self.join_done):
			pass
		self.join_finished()
	def join(self):
		#node 0 will be initialized with all keys
		if self.node_id is 0:
			self.key_start = 0
			self.key_end = 256
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
			self.update_others()
			#5. Transfer keys to node_id
		self.join_done = 1

	def join_finished(self):
		counter = 10
		while(counter>0):
			counter-=1
		self.coord.sendall("join_finished "+ str(self.node_id))
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
		req_ft = msg.split(' ')
		for x in range(1, 9):
			self.ft[x-1] = int(req_ft[x])
			#to avoid re-connecting to node 0
			if req_ft[x-1] == 0:
				self.sock[0] = self.node0
		self.predecessor = int(req_ft[9])
		self.update_keys()
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

	#setup connection to nodes in the finger table
	def conn_finger_table(self):
		#setup connection to nodes in fingertable
		for x in range(0, 8):
			#don't connect if connection exists or to itself
			if(self.sock.has_key(self.ft[x]) or self.ft[x] == self.node_id):
				continue
			try:
				if(self.ft[x]==0):
					self.sock[0] = self.node0
					continue
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

		print '[Node %d]Done with conn_finger_table\n'%self.node_id
	#receives messages from other nodes that CONNECTED TO THIS NODE!!!
	def recvThread(self, conn):
		#continuously receive data from the nodes
		while(1):
			data = conn.recv(1024)

			buf = data.split(' ')
			#--------------ONLY NODE 0--------------

			#REQUEST - if node 0 asked to find a node's successor
			if(buf[0]  == "registration"):
				try:
					req_node = int(buf[1])
				except ValueError:
					print buf
				
				self.registration_handler(conn, req_node)
			#--------------END-------------------
			#REPLY - node 0 send self its node table
			elif(buf[0]=="ft"):
				print '[Node %d] Received FT\n'%self.node_id
				self.build_table(data)

			#REPLY - to find_predecessor
			elif(buf[0] == "found_predecessor"):
				predecessor = buf[1]
				successor = buf[2]
				self.found_predecessor_handler(predecessor, successor)

			#REQUEST - find_predecessor
			elif(buf[0] == "find_predecessor"):
				try:
					id = int(buf[1])
				except ValueError:
					print buf
				self.find_predecessor_handler(conn, id)

			#REQUEST - node is told what its predecessor is
			elif(buf[0] == "your_predecessor"):
				try:
					new_pred = int(buf[1])
				except ValueError:
					print buf
				self.your_predecessor_handler(new_pred)
			#REQUEST - node is told what its succesor is
			elif(buf[0] == "your_successor"):
				try:
					new_success = int(buf[1])
				except ValueError:
					print buf

				self.your_successor_handler(new_success)
			#--------------REQUEST - node is told to update its finger table
			elif(buf[0] == "update_table"):
				try:
					idx = int(buf[1])
					finger = int(buf[2])
				except ValueError:
					print buf
				self.update_table_handler(idx, finger)
			#--------------COMMAND - node is told to print keys-----------
			if(buf[0]=="show"):
				cmd = buf[1]
				self.show_handler(cmd)

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
			thread.start_new_thread(self.recvThread, (conn,))

		conn.close()
		s_server.close()

	#build the finger table of node req_node
	def init_finger_table(self,req_node):

		#node_0 builds req_node's ft and sends it back in message format
		req_ft = [None]*8

		msg = self.find_successor(int(req_node+1))
		
		# msg will contain "PRD SUCC"
		msg = msg.split(' ')
		predecessor = int(msg[0])
		successor = int(msg[1])
		req_ft[0] =  successor

		#build the rest of the finger table
		for i in range(0, 7):
			finger_id = int(req_node+math.pow(2, i+1))

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
				print msg
				msg = msg.split(' ')
				req_ft[i+1] = msg[1]

		#tell successor that req_node is its predecessor
		if(successor == 0):
			self.predecessor = req_node
			self.update_keys()
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
		msg = self.find_predecessor(req_node)

		print '[Node %d]find_successor returning:'%self.node_id + msg
		return msg
	
	def find_predecessor(self, req_node):
		print '[Node %d] find_predecessor(%d)\n'%(self.node_id, req_node)
		while(globals.printing):
			pass
		node = self.node_id

		retVal = "OOPS!"
		if(self.node_id == self.ft[0] and self.node_id==self.predecessor):
			#if node 0 is the only node in the system - case 1
			retVal = '0 0'
		elif(req_node > self.node_id and req_node <=self.ft[0]):
			#new node falls between self and self.successor
			#print '[Node %d] find_pred_case1: '%self.node_id + str(node) + ' ' + str(self.ft[0]) + '\n'
			retVal = str(node) + ' ' + str(self.ft[0])
		elif(req_node > self.node_id and self.ft[0] == 0):
			#new node is the new biggest node
			#print'[Node %d] find_pred_case2\n' %self.node_id
			retVal = (str(self.node_id) + " 0")
		elif(req_node < self.node_id and req_node > self.predecessor):
			retVal = (str(self.predecessor) + ' ' + str(self.node_id))
		elif(req_node == self.node_id):
			retVal = (str(self.predecessor) + ' ' + str(self.ft[0]))
		elif(req_node == self.ft[0]):
			retVal = (str(self.node_id) + ' ' + "Garbage")
		else:
			if(req_node == 8):
				print '*******JKDHAS DUWYGDSDKJSBDJHASVBDKHAJSBDJSD&Y&SYDISAJHDSAJDJB\n'
			# no match, pass a request to a node that might be able to handle
			node = self.closest_preceding_finger(req_node)
			print '[Node %d]For idx:%d closest_preceding_finger:%d\n'%(self.node_id, req_node, int(node))
			if(node == self.node_id):
				node = self.predecessor
			#print '[Node %d]closest_preceding_finger: %d\n' %(self.node_id, node)
			self.reply_predecessor = 0
			if(self.sock.has_key(node)):
				self.sock[node].send("find_predecessor "+str(req_node))
			else:
				request = "forward_predecessor_to"+' ' + str(node) +' ' + "find_predecessor" +' '+ str(req_node)
				self.coord.sendall(request)
			#print'[Node %d]sent find_predecessor request to node_%d\n' %(self.node_id, node)
			
			#wait for reply
			while(not self.reply_predecessor):
				pass
			#get the response
			retVal = self.msg_predecessor
			self.msg_predecessor = ""
			#print '[Node %d] predecessor reply is '%self.node_id + str(retVal) + '\n'

		if(req_node == 8):
			print '*******JKDHAS DUWYGDSDKJSBDJHASVBDKHAJSBDJSD&Y&SYDISAJHDSAJDJB\n'
		print '[Node %d] find_predecessor returning: '%self.node_id + retVal
		print 'For id %d\n'%req_node
		return str(retVal)

	#returns the node preceding req_node
	def closest_preceding_finger(self, req_node):
		counter = 0
		retVal = self.node_id
		for x in list(reversed(range(8))):
			if(self.ft[x] < self.node_id):
				counter +=1
			elif(self.ft[x] > self.node_id and self.ft[x] < req_node):

				retVal = self.ft[x]
			elif(self.ft[0] == 0 and self.predecessor == 0):
				#if the table is filled in with 0s only - i.e. self is 2nd node in the network
				retVal = 0
		if(counter == 8):
			retVal = self.ft[0]
		if(retVal == self.node_id and req_node < self.node_id):
			return self.ft[0]

		return retVal

	#new node will help others to update their finger table
	def update_others(self):
		print '[Node %d] Updating others Yay! or Not\n'%self.node_id
		for i in range(1, 8):
			print 'IM HERE *************\n'
			id = int(self.wrap(self.node_id - math.pow(2, i)))
			print '***id %d***\n'%id
			pred_succ = self.find_predecessor(id)
			print '[Node %d] predecessor for idx %d is\n'%(self.node_id,i) + str(pred_succ)
			pred_succ = pred_succ.split(' ')
			finger_id = int(pred_succ[0])
			if(finger_id == self.node_id):
				#if i'm the predecessor, no action is required - update finished!
				print '[Node %d] Done updating others\n'%self.node_id
				return
			print '[Node %d] update_finger_table(%d, %d, %d)\n'%(self.node_id, self.node_id, finger_id, i)
			self.update_finger_table(self.node_id, finger_id, i)

		print '[Node %d] Done updating Others\n'%self.node_id

	def update_finger_table(self, new_node, dest_node, idx):
		request = ""
		request = "update_table"+ ' ' + str(idx) + ' ' + str(new_node)
		#check if I can reach this node
		if(self.sock.has_key(dest_node)):
			self.sock[dest_node].send(request)
		else: #I don't have connection, then talk to coordinator
			request = "forward_to"+' ' + str(dest_node) +' '+ request
			self.coord.sendall(request)
		print '[Node %d] sending request to update table: '%self.node_id + request + '\n'

	#this function removes node with the id "node_id" from the system
	def remove_node(self, node_id):
		pass
		#somehow remove the node
	#this function finds key_id
	def find_key(self, node_id, key_id):
		pass
		#somehow find the key
	#**********
	#HELPER FUNCTIONS
	#**********

	def print_ft(self):
		print self.ft
		print '\n'

	def print_keys(self):
		s = []
		for x in range(self.key_start, self.key_end):
			s.append(x)
		key_list = (" ".join(str(e) for e in s))
		print key_list

	def update_keys(self):
		self.key_start = self.predecessor+1
		self.key_end = self.node_id+1
		if(self.node_id==0 and self.predecessor is not 0):
			self.key_end = 256

	def wrap(self, finger_id):
		if(finger_id < 0):
			print '***Its less than 0!!!***\n'
			finger_id = 255 + finger_id
		return finger_id 


	#*********
	#Message Handlers
	#*********

	def registration_handler(self,conn, req_node):
		self.sock[req_node] = conn
		print '[Node %d]Saved socked for node %d: \n' %(self.node_id, req_node)
		newinfo = self.init_finger_table(req_node)
		newinfo = "ft " + newinfo
		if(conn.sendall(newinfo)==None):
			pass
		else:
			print '[ERROR] Node 0 failed to send finger table to Node_%d\n'%int(buf[1])
	
	def find_predecessor_handler(self, conn, id):
		msg = self.find_predecessor(id)
		#add the header
		msg = "found_predecessor " + msg
		conn.sendall(msg)
	
	def found_predecessor_handler(self, predecessor, successor):
		self.msg_predecessor = predecessor + ' ' + successor
		self.reply_predecessor = 1

	def your_predecessor_handler(self, predecessor):
		self.predecessor = predecessor
		self.update_keys()

	def your_successor_handler(self, successor):
		self.ft[0] = successor
		#connect to new nodes if required
		self.conn_finger_table()

		while(globals.printing):
			pass
		globals.printing = 1
		print '[Node %d] my new finger table is: \n'%self.node_id
		self.print_ft()
		globals.printing = 0

	def update_table_handler(self, idx, finger):
		print '[Node %d]Received update_table request: node:%d idx: %d \n'%(self.node_id, finger, idx)
		if(finger >= self.node_id):
			accept = 0
			if(finger < self.ft[idx]):
				accept = 1
			if(self.ft[idx] == 0): 
				accept = 1
			if(self.predecessor == self.ft[0]):
				accept = 1
			if(self.ft[idx] == self.node_id):
				accept = 1
			if(accept):
				print '[Node %d] Updating_table_handler: idx: %d, new node %d \n'%(self.node_id, idx, finger)
				self.ft[idx] = finger
				self.conn_finger_table()
				#self.update_finger_table(finger, self.predecessor, idx)

	#--------COMMAND HANDLERS-------
	def show_handler(self, cmd):
		print '[Node %d] FINGER TABLE:\n'%self.node_id
		self.print_ft()
		print self.predecessor
		print '[Node %d] KEYS:\n' %self.node_id
		self.print_keys()

		if cmd == "all" and self.ft[0] != 0:
			self.sock[self.ft[0]].sendall("show all")









