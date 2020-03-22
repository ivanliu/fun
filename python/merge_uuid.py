import os
import re
import sys

def load_uuid(_file):
    '''
    load uuid from a file
    '''
    fin = open(_file)
    fin.readline().strip()  # skip the header
    
    uuid = set()
    for line in fin:
        _id = line.split(',')[0].strip()
        uuid.add(_id)
    fin.close()
    
    return uuid

def dump_uuid(_file, uuids):
    '''
    dump uuid to a file
    '''
    fout = open(_file, 'w')
    for uuid in uuids:
        fout.write("%s\n" % str(uuid))
    fout.close()

def main():
    '''
    '''
    _input1 = ''
    _input2 = ''
    _output = ''

    omnibus_uuid = load_uuid(_input1)  # the file contains omni uuid
    global_uuid = load_uuid(_input2) # the file contains global uuid
   
    # dump uuids to files
    dump_uuid(omnibus_uuid.union(global_uuid), _output)
    

if __name__ == '__main__':
    '''
    '''
    main()

