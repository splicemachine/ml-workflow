import random
from fastnumbers import fast_float
x = []

for i in range(1000000):
  x.append(str(i*20))

X = [fast_float(i) for i in x]
print(X[:10])
