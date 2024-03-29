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
		self.pred_sock = None
		#flag that indicates if finger table is done building
		self.build_done = 0
		#flag that indicates if query reply to find_predecessor has arrived
		self.reply_predecessor=0
		self.cmd_done = 0
		#buffer that contains "pred succ"
		self.msg_predecessor = "OOHO"
		

		#server thread - receives connection from other nodes
		server_t=threading.Thread(target = self.serverThread, args = ())
		server_t.start()

		self.init_coord()
		self.join()
		while(not self.cmd_done):
			pass
		self.cmd_finished()
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
			self.conn_predecessor()
			#4. Update the finger table of existing nodes
			self.update_others("update_table")
			#5. Transfer keys to node_id
		self.cmd_done = 1

	def cmd_finished(self):
		msg = "cmd_finished "+ str(self.node_id)
		self.coord.sendall("Start"+msg+"End")
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
		self.node0.sendall("Start"+"registration " + str(self.node_id)+"End")
		#print '[Node %d] Connected to node 0.\n' % self.node_id

	#joined node fill in his local table
	def build_table(self, msg):
		req_ft = msg.split(' ')
		#print '\n[Node %d]Inside build_table, req_ft: '%self.node_id + str(req_ft) + '\n'
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
		if(self.coord.sendall("Start"+"registration " + str(self.node_id)+"End")==None):
			print '[Node %s] connected to coordinator' % self.node_id
		else:
			print 'client registration incomplete'

		#server thread - receives messages from the coordinator
		server_t=threading.Thread(target = self.recvThread, args = (self.coord,))
		server_t.start()

	#setup connection to nodes in the finger table + predecessor
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
		           
				self.sock[self.ft[x]] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			except socket.error, msg:
				print 'Failed to create socket. Error code: ' + str(msg[0]) + ' , Error message : ' + msg[1] + '\n'
				sys.exit();
			node_port = int(globals.coord_port) + int(self.ft[x]) + 1
			self.sock[self.ft[x]].connect((globals.coord_ip, node_port))
			print '[Node %d] Connected to Node_%d\n' %(self.node_id, self.ft[x])

			#receive messages from the newly connected node
			server_t=threading.Thread(target = self.recvThread, args = (self.sock[self.ft[x]],))
			server_t.start()

	def conn_predecessor(self):
		#connect to predecessor
		if(self.pred_sock != None):
			self.pred_sock.close()

		self.pred_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		node_port = int(globals.coord_port) + int(self.predecessor) + 1
		self.pred_sock.connect((globals.coord_ip, node_port))
		#print '[Node %d] Connected to predecessor\n'%self.node_id
		
		#receive messages from the newly connected node
		server_t=threading.Thread(target = self.recvThread, args = (self.pred_sock,))
		server_t.start()



	#receives messages from other nodes that CONNECTED TO THIS NODE!!!
	def recvThread(self, conn):
		#continuously receive data from the nodes
		while(1):
			total_data=[]
			end_idx = 0
			data = conn.recv(1024)
			length = len(data)
			while end_idx < length:
				if "Start" in data:
					start_idx = data.find("Start")
					end_idx = data.find("End")
					total_data.append(data[start_idx+5:end_idx])
					temp = data[end_idx+3:len(data)]
					data = temp
				else:
					break
				if len(total_data) > 1:
					last_pair = total_data[-2] + total_data[-1]
					if "End" in last_pair:
						total_data[-2]=last_pair[:last_pair.find('End')]
						total_data.pop()
						break
			for j in range(0,len(total_data)):
				single_msg = ''.join(total_data[j])
				buf = single_msg.split(' ')

				#print '\n\n[Node %d]GOT MESSAGE: '%self.node_id + str(buf) + '\n\n'
				#--------------ONLY NODE 0--------------

				#REQUEST - if node 0 asked to find a node's successor
				if(buf[0]  == "registration"):
					try:
						req_node = int(buf[1])
						self.registration_handler(conn, req_node)
					except ValueError:
						print buf
				
				#--------------END-------------------
				#REPLY - node 0 send self its node table
				elif(buf[0]=="ft"):
					#print '\n[Node %d] Received FT\n'%self.node_id
					self.build_table(single_msg)

				#REPLY - to find_predecessor
				elif(buf[0] == "found_predecessor"):
					predecessor = buf[1]
					successor = buf[2]
					self.found_predecessor_handler(predecessor, successor)

				#REQUEST - find_predecessor
				elif(buf[0] == "find_predecessor"):
					try:
						id = int(buf[1])
						self.find_predecessor_handler(conn, id)
					except ValueError:
						print buf
					

				#REQUEST - node is told what its predecessor is
				elif(buf[0] == "your_predecessor"):
					try:
						new_pred = int(buf[1])
						self.your_predecessor_handler(new_pred)
					except ValueError:
						print buf
					
				#REQUEST - node is told what its succesor is
				elif(buf[0] == "your_successor"):
					try:
						new_success = int(buf[1])
						self.your_successor_handler(new_success)
					except ValueError:
						print buf

					
				#--------------REQUEST - node is told to update its finger table
				elif(buf[0] == "update_table"):
					try:
						idx = int(buf[1])
						finger = int(buf[2])
						self.update_table_handler(idx, finger)
					except ValueError:
						print buf
				elif(buf[0] == "imma_leaving"):
					try:
						who_leaving = int(buf[1])
						index = int(buf[2])
						replacement = int(buf[3])
						print '[Node %d] Node %d is Leaving? why ;_;\n'%(self.node_id, who_leaving)
						self.update_leave_handler(index, replacement, who_leaving)
					except ValueError:
						print buf
				#--------------COMMAND - node is told to print keys-----------
				if(buf[0]=="show"):
					cmd = buf[1]
					#print '\n[Node %d]Received cmd show '%self.node_id + str(buf) + '\n'
					self.show_handler(cmd)
				elif(buf[0] == "find"):
					key_to_find = int(buf[1])
					self.find_handler(key_to_find)
				elif(buf[0] == "leave"):
					self.leave_handler()


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
				#print '[Node %d] successor(i+1) == successor(i) with finger_id %d and prev finger %d\n'%(self.node_id, finger_id, req_ft[i])
				req_ft[i+1] = req_ft[i]
			elif(finger_id <= 255 and req_ft[0] == 0):
				#if finger_id is the current biggest key -then wrap back to node 0
				#print '[Node %d] case 2 successor(i+1) == successor(i)\n'%self.node_id
				req_ft[i+1] = req_ft[i]

			else: #finger_id > req_ft[i]
				#successor(i+1) > successor(i)
				#print '[Node %d] successor(i+1) > successor(i)\n'%self.node_id
				if(finger_id > 255): #wrap back
					finger_id = finger_id -255

				msg = self.find_successor(finger_id)
				#print msg
				msg = msg.split(' ')
				req_ft[i+1] = int(msg[1])

		#tell successor that req_node is its predecessor
		if(successor == 0):
			self.predecessor = req_node
			self.update_keys()
		else:
			self.sock[successor].sendall("Start"+"your_predecessor " + str(req_node)+"End")
		#tell predecessor that req_node is its successor
		if(predecessor==0):
			self.ft[0] = req_node
		else:
			self.sock[predecessor].sendall("Start"+"your_successor " + str(req_node)+"End")
		#send back whole table back to requesting node
		retFt = (" ".join(str(e) for e in req_ft))
		return (retFt + ' ' + str(predecessor))

	def find_successor(self, req_node):
		msg = self.find_predecessor(req_node)

		#print '[Node %d]find_successor returning:'%self.node_id + msg
		return msg
	
	def find_predecessor(self, req_node):
		#print '[Node %d] find_predecessor(%d)\n'%(self.node_id, req_node)
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
			# no match, pass a request to a node that might be able to handle
			node = self.closest_preceding_finger(req_node)
			#print '[Node %d]For idx:%d closest_preceding_finger:%d\n'%(self.node_id, req_node, int(node))
			if(node == self.node_id):
				node = self.predecessor
			#print '[Node %d]closest_preceding_finger: %d\n' %(self.node_id, node)
			self.reply_predecessor = 0
			if(self.sock.has_key(node)):
				self.sock[node].sendall("Start"+"find_predecessor "+str(req_node)+"End")
			else:
				request = "forward_predecessor_to"+' ' + str(node) +' ' + "find_predecessor" +' '+ str(req_node)
				self.coord.sendall("Start"+request+"End")
			#print'[Node %d]sent find_predecessor request to node_%d\n' %(self.node_id, node)
			
			#wait for reply
			while(not self.reply_predecessor):
				pass
			#get the response
			retVal = self.msg_predecessor
			self.msg_predecessor = ""
			#print '[Node %d] predecessor reply is '%self.node_id + str(retVal) + '\n'

		#print '[Node %d] find_predecessor returning: '%self.node_id + retVal
		#print 'For id %d\n'%req_node
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
	def update_others(self, request):
		#print '[Node %d] Updating others Yay! or Not\n'%self.node_id
		for i in range(1, 8):
			id = int(self.wrap(self.node_id - math.pow(2, i)+1))
			pred_succ = self.find_predecessor(id)
			#print '[Node %d] predecessor for idx %d is\n'%(self.node_id,i) + str(pred_succ)
			pred_succ = pred_succ.split(' ')
			finger_id = int(pred_succ[0])
			if(finger_id == self.node_id):
				#if i'm the predecessor, no action is required - update finished!
				#print '[Node %d] Done updating others\n'%self.node_id
				return
			#print '[Node %d] update_finger_table(%d, %d, %d)\n'%(self.node_id, self.node_id, finger_id, i)
			if(request == "update_table"):
				self.update_finger_table(request, self.node_id, finger_id, i)
			else:
				self.update_finger_table(request, self.ft[0], finger_id, i)

	#	print '[Node %d] Done updating Others\n'%self.node_id

	def update_finger_table(self, request, new_node, dest_node, idx):
		#request = "update_table"+ ' ' + str(idx) + ' ' + str(new_node)
		request = request + ' ' + str(idx) + ' ' +str(new_node)
		#check if I can reach this node
		if(self.sock.has_key(dest_node)):
			self.sock[dest_node].sendall("Start"+request+"End")
		elif(dest_node == self.predecessor):
			self.pred_sock.sendall("Start"+request+"End")
		else: #I don't have connection, then talk to coordinator
			request = "forward_to"+' ' + str(dest_node) +' '+ request
			self.coord.sendall("Start"+request+"End")
		#print '[Node %d] sending request to dest_node:%d to update table: '%(self.node_id, dest_node) + request + '\n'

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
		globals.file.write(str(self.node_id) + " ")
		if(self.node_id==0):
			globals.file.write("0" + " ")
		globals.file.write(key_list+ " ")
		print key_list

	def update_keys(self):
		self.key_start = self.predecessor+1
		self.key_end = self.node_id+1
		if(self.node_id==0 and self.predecessor is not 0):
			self.key_end = 256

	def wrap(self, finger_id):
		if(finger_id < 0):
			finger_id = 255 + finger_id
		return finger_id 

	#def clear_old_finger(self, idx):
		#self.sock[self.ft[idx]].close()
		#del self.sock[self.ft[idx]]
	#*********
	#Message Handlers
	#*********

	def registration_handler(self,conn, req_node):
		self.sock[req_node] = conn
		#print '\n[Node %d]Saved socked for node %d: \n' %(self.node_id, req_node)
		newinfo = self.init_finger_table(req_node)
		newinfo = "ft " + newinfo
		if(conn.sendall("Start"+newinfo+"End")==None):
			pass
		else:
			print '[ERROR] Node 0 failed to send finger table to Node_%d\n'%int(buf[1])
	
	def find_predecessor_handler(self, conn, id):
		msg = self.find_predecessor(id)
		#add the header
		msg = "found_predecessor " + msg
		conn.sendall("Start"+msg+"End")
	
	def found_predecessor_handler(self, predecessor, successor):
		self.msg_predecessor = predecessor + ' ' + successor
		self.reply_predecessor = 1

	def your_predecessor_handler(self, predecessor):
		self.predecessor = predecessor
		print '[Node %d] My new predecessor is %d'%(self.node_id, predecessor)
		self.update_keys()
		self.conn_predecessor()
	def your_successor_handler(self, successor):
		print '[Node %d] My new successor is %d'%(self.node_id, successor)
		#self.clear_old_finger(idx)
		self.ft[0] = successor
		#connect to new nodes if required
		self.conn_finger_table()

		while(globals.printing):
			pass
		globals.printing = 1
		#print '[Node %d] my new finger table is: \n'%self.node_id
		self.print_ft()
		globals.printing = 0

	def update_table_handler(self, idx, finger):
		#print '[Node %d]Received update_table request: idx: %d node:%d\n'%(self.node_id, idx, finger)
		fing_succ = int(self.node_id+math.pow(2, idx))
		#print '[Node %d] idx: %d fing_succ: %d\n'%(self.node_id, idx, fing_succ)
		if(finger > self.node_id):
			accept = 0
			if(finger < self.ft[idx]):
				accept = 1
			elif(self.ft[idx] == 0 and finger >=fing_succ):
				accept = 1
			#EXISTED HERE
			#if(self.predecessor == self.ft[0]):
				#accept = 1
			elif(self.ft[idx] == self.node_id and finger >=fing_succ):
				accept = 1
			if(accept):
				#print '[Node %d] Updating_table_handler: idx: %d, new node %d \n'%(self.node_id, idx, finger)

				#before changing, clear old
				#self.clear_old_finger(idx)
				self.ft[idx] = finger
				self.conn_finger_table()
				#print '[Node %d] you better get it node %d\n'%(self.node_id, self.predecessor)
				self.update_finger_table("update_table", finger, self.predecessor, idx)
	def update_leave_handler(self, idx, newnode, wholeaving):
		print '[Node %d]Node %d is leaving change idx%d ?\n'%(self.node_id, wholeaving, idx)
		#if I possess the leaving node in ft[idx], its successor shall take its place
		if(self.ft[idx] == wholeaving):
			self.ft[idx] = newnode
			self.conn_finger_table()
			print '[Node %d] Node %d, you better fuking get this\n'%(self.node_id, self.predecessor)
			self.update_finger_table("imma_leaving "+str(wholeaving), newnode, self.predecessor, idx)

	#--------COMMAND HANDLERS-------
	def show_handler(self, cmd):
		if globals.file == None or globals.file.closed==True:
			globals.file = open(globals.filename, "w")
		print '\n[Node %d] FINGER TABLE:\n'%self.node_id
		self.print_ft()
		print 'predecessor: %d\n'%self.predecessor
		print '[Node %d] KEYS:\n' %self.node_id
		self.print_keys()
		#write keys to the given file
		if cmd == "all" and self.ft[0] != 0:
			globals.file.write("\n")
			self.sock[self.ft[0]].sendall("Start"+"show all"+"End")
			return
		globals.file.close()
		self.cmd_finished()

	def find_handler(self, key_to_find):
		#check if I have it
		if(key_to_find >= self.key_start and key_to_find < self.key_end):
			print '[Node %d] FOUND KEY! I have key %d\n'%(self.node_id, key_to_find)
		else:
			#if I don't, find the key's successor
			result = self.find_successor(key_to_find)
			
			# msg will contain "PRD SUCC"
			result= result.split(' ')
			succ = int(result[1])
			print '[Node %d] FOUND KEY! Node %d has the key %d\n'%(self.node_id, succ, key_to_find)
		self.cmd_finished()

	#self.leave
	def leave_handler(self):
		#print 'wasap'
		#tell appropriate nodes Adios
			#others will change their keys accordingly
		self.update_others("imma_leaving "+str(self.node_id))
		#tell your successor that its new predecessor is your predecessor
		print '[Node %d]my successor:%d should change its predecessor to %d\n'%(self.node_id, self.ft[0], self.predecessor)
		self.sock[self.ft[0]].sendall("Start" + "your_predecessor" + ' ' + str(self.predecessor) + "End")
		#tell your predecessor that its new successor is your successor
		print '[Node %d]my predecessor:%d should change its successor to %d\n'%(self.node_id, self.predecessor , self.ft[0])
		self.pred_sock.sendall("Start"+"your_successor" + ' ' + str(self.ft[0])+ "End")
		self.cmd_finished()
		#do cleanup
			#destroy sockets
			#kill threads
	



