import time
from storm.drpc import DRPCClient

c = DRPCClient("localhost", 3772)
for i in range(10):
	print c.execute("tweets", "Stick to the plan OPM")
	time.sleep(3)