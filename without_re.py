import random
x = []

for i in range(1000000):
  x.append(str(i*20))

X = [float(i) for i in x if i.replace('.','').isdigit()]
print(X[:10])