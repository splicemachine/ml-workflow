from os import environ as env_vars
BUILD_NUMBER = env_vars['BUILD_NUMBER']
newlines = []
f = open('docker-compose.yaml').readlines()
for line in f:
    newline = line.rstrip('\n')
    if 'sm_k8_mlflow' in line or 'sm_k8_bobby' in line or 'sm_k8_feature_store' in line:
        if 'DEV' not in line:
            newline = newline + f'_DEV{BUILD_NUMBER}'
        elif line[-1].isdigit:
            newline = newline[:-1] + str(BUILD_NUMBER)
    newlines.append(newline + "\n")
with open('docker-compose.yaml', 'w+') as new_file:
    new_file.writelines(newlines)
