import sys

with open(sys.argv[1], 'rb') as afp, \
        open(sys.argv[2], 'rb') as bfp:
    while True:
        print ('foo')
        a = afp.read(1073741824)
        b = bfp.read(1073741824)
        if a == '' and b == '':
            sys.exit(0)
        if a != b:
            print('a not b')
            sys.exit(1)
