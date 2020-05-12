import random
import re
x = []

for i in range(1000000):
  x.append(str(i*20))

X = [float(i) for i in x if re.sub('\.', '', i).isdigit()]

