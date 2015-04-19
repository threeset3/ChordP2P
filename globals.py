#globals

def init():
	global nodes, coord_port, coord_ip, active_nodes, num_active, keep_alive, sock, coord_initialized, printing, cmd_done, nodes, filename, file
	
	#stores the connection of nodes
	sock = [None] * 256
	coord_port = 8180
	coord_ip = "localhost"
	active_nodes = [0]*256
	coord_initialized = 0
	cmd_done = 1
	printing = 0
	nodes = [None]*256
	filename = None
	file = None