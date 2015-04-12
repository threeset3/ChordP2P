#globals

def init():
	global nodes, coordinator_port, active_nodes

	#stores the connection of nodes
	sock = [] * 256
	coord_port = 8000
	coord_ip = localhost
	active_nodes = []*256
	num_active = 0
	for x in range(0, 256):
		active_nodes[x] = 0