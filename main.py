#main.py

import socket
import sys, getopt
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
		total_data=[]
		end_idx = 0
		#receive data
		data = conn.recv(1024)
		length = len(data)
		#print "\n[Coord]data:" + data + '\n'
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
		#print '\n[Coord] all data separated:' + str(total_data) + '\n'
		for j in range(0,len(total_data)):
			single_msg = ''.join(total_data[j])

			#print '\n[Coord] single_msg:'+single_msg + '\n'
			buf = single_msg.split(' ')

			#if registration message, indicate sender node as active
			if(buf[0] == "registration"):
				new_node = int(buf[1])
				if(globals.active_nodes[new_node] == 0):
					globals.active_nodes[new_node] = 1
					print '[Coord] Marking node %d as active\n' % new_node
					globals.sock[new_node] = conn
					print '[Coord] Connected with node %d'%new_node

			elif(buf[0] == "cmd_finished"):
				new_node = int(buf[1])
				print '[Coord] Node %d done with operation\n'%new_node
				globals.cmd_done=1
				print '[Coord] Can take new command now :)\n'
			#------Node requested coordinator to forward a message to a different node-----
			elif(buf[0] == "forward_to"):
				dest = int(buf[1])
				forward = ''
				for k in range(2, len(buf)):
					forward += buf[k] + ' '
				#print '\n[Coord] Sending forward_to to node %d\n'%dest
				#print '\n[Coord] Content: ' + msg + '\n'
				if(globals.sock[dest].sendall("Start"+forward+"End")==None):
					pass
				else:
					print '\n[Coord] I fail as a coordinator :(\n'
			elif(buf[0] == "forward_predecesor_to"):
				#print '\n[Coord] received request to FORWARD find_predecessor: '
				#print buf
				#print '\n'
				dest2 = int(buf[1])
				msg2 = buf[2]+' '+buf[3]
				if(globals.sock[dest2].sendall("Start"+msg2+"End")==None):
					pass
				else:
					print '\n[Coord] I fail as a coordinator :(\n'


#adds a node to Chord
def join_handler(node_id):
	#check if the node already exists
	if(globals.active_nodes[node_id]):
		print("Node already in Chord!\n")
		globals.cmd_done = 1
		return

	#create a thread representing the node
	myNode = Node(node_id)
	globals.nodes[node_id] = myNode
	#store the object globally - needed to delete the object later

#removes a node from Chord
def leave_handler(node_id):

	#check if the node exists
	if(globals.active_nodes[node_id] == 0):
		print("Node doesn't exist!\n")
		globals.cmd_done = 1
		return

	#tell node_id to remove itself from Chord
	msg = "Start"+"leave"+"End"
	try:
		globals.sock[node_id].sendall(msg)
	except socket.error , msg2:
		print '[[ Send failed : ' + str(msg2[0]) + ' Message ' + msg2[1] + ' ]]' + '\n'
		sys.exit()
	#indicate that this node is inactive

	print '[Coord] Marking node%d as inactive\n'%node_id
	globals.active_nodes[node_id] = 0

	#delete the object
	del globals.nodes[node_id]
#tells node 'node_id' to find key with id 'key_id'
def find_handler(node_id, key_id):
	#check if node_id exists
	if(globals.active_nodes[node_id] is 0):
		print("Given node not in the system!\n")
		globals.cmd_done = 1
		return
	#tell node_id to find key_id
	msg = "Start"+ "find "+str(key_id) + "End"
	try:
		globals.sock[node_id].sendall(msg)
	except socket.error , msg2:
		print '[[ Send failed : ' + str(msg2[0]) + ' Message ' + msg2[1] + ' ]]' + '\n'
		sys.exit()
def show_handler(node):
	if node is "all":
		print '[Coord] sending show-all command to 0\n'
		globals.sock[0].sendall("Start"+"show all"+"End")
	else:
		print '[Coord] show_handler for node %d\n'%node
		if(globals.active_nodes[node] is 0):
			print '[Coord] Requested node is inactive\n'
			globals.cmd_done = 1
			return
		if(globals.sock[node] != None):
			print '[Coord] sending show command to %d\n' %node
			globals.sock[node].sendall("Start"+"show you"+"End")

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

def main(argv):
	#create the global vars
	globals.init()

	#get filename
	try:
		opts, args = getopt.getopt(argv,'-g:', ["filename"])
	except getopt.GetoptError:
		print 'main.py -i <inputfile>'
		sys.exit(2)
	globals.filename = argv[0]

	#thread for intializing the coordinator to talk with nodes
	server_thread = threading.Thread(target=server, args=())
	server_thread.setDaemon(True)
	server_thread.start()

	while(not globals.coord_initialized):
		pass

	# set up node 0
	join_handler(0);

	while(1):
		while(globals.cmd_done):
			userInput = raw_input('>>> ');
			cmd = userInput.split(' ');
			if cmd[0] == "join" and cmd[1] != None:
				globals.cmd_done=0
				join_handler(int(cmd[1]))

			elif cmd[0] == "find" and cmd[1] != None and cmd[2] != None:
				globals.cmd_done=0
				find_handler(int(cmd[1]), int(cmd[2]))

			elif cmd[0] == "leave" and cmd[1] != None:
				globals.cmd_done=0
				leave_handler(int(cmd[1]))

			elif cmd[0] == "show" and cmd[1] == "all":
				globals.cmd_done=0
				show_handler("all")

			elif cmd[0] == "show" and cmd[1] != None:
				globals.cmd_done=0
				show_handler(int(cmd[1]))

			elif cmd[0] == "quit":
				break;
			elif cmd[0] == "":
				pass
			else:
				print 'invalid Input'
	server_thread.join()
	sys.exit()
	print'[Coord] I quit\n'

#execution starts here
if __name__ == "__main__":
   main(sys.argv[1:])









