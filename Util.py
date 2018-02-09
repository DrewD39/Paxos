

class Config:

	def __init__ (self, config_file=None, tolerated_faults=None, server_info=None, parameters=None):

		if config_file:
			with open(config_file) as f:
				for i, line in enumerate(f.readlines()):

					line = line.partition('#')[0]

					if i == 0: # number of tolerated faults
						self.tolerated_faults = int(line)
					elif i == 1: # (ip, port) pairs
						self.server_pairs = self.__parse_pairs(line)
					elif i == 2: # testing parameters
						self.parameters = str(line)
		else:
			self.tolerated_faults = tolerated_faults
			self.server_pairs = self.__parse_pairs(server_info)
			self.parameters = parameters

		assert(len(self.server_pairs) == 2 * int(self.tolerated_faults) + 1) # 2f + 1

	def __parse_pairs (self, line):
		parsed_line = [x.strip('( )') for x in line.split(',')]
		pairs = zip(parsed_line[::2], parsed_line[1::2])
		return pairs

	def __str__ (self):
		return "\nConfig is:\n\tTolerated faults: {}\n\tServer pairs: {}\n\tParameters: {}\n\t"\
					.format(self.tolerated_faults, [', '.join(x) for x in self.server_pairs], self.parameters) 
