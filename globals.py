#globals

def init():
	global nodes, coord_port, coord_ip, active_nodes, num_active, keep_alive, sock, coord_initialized

	#stores the connection of nodes
	sock = [None] * 256
	coord_port = 8180
	coord_ip = "localhost"
	active_nodes = [0]*256
	num_active = 0
	coord_initialized = 0
