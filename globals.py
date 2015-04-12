#globals

def init():
	global nodes, coord_port, coord_ip, active_nodes, num_active

	#stores the connection of nodes
	sock = [] * 256
	coord_port = 8000
	coord_ip = "localhost"
	active_nodes = [0]*256
	num_active = 0
