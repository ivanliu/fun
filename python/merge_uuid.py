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
        _id = line.strip()
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
    _input3 = ''
    _output1 = ''
    _output2 = ''

    all_uuid = load_uuid(_input1)  # the file contains all uuid
    acct_uuid = load_uuid(_input2) # the file contains all users who open account
    cash_uuid = load_uuid(_input3) # the file contains all users who deposit cash
    
    # find acct users and cash users
    interested_acct_uuid = all_uuid.intersection(acct_uuid)
    interested_cash_uuid = all_uuid.intersection(cash_uuid)
    
    # dump uuids to files
    dump_uuid(interested_acct_uuid, _output1)
    dump_uuid(interested_cash_uuid, _output2)
    

if __name__ == '__main__':
    '''
    '''
    main()

