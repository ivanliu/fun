import os
import re
import sys

pattern = re.compile("INSERT INTO `.*` VALUES")

def main():
    '''
    '''
    _input = ''
    _output = ''

    fin = open(_input)
    fout = open(_output, 'w')
    for line in fin:
        line = line.strip()
        m = pattern.match(line)
        if m:
            items = re.findall('\([^)]*\)', line)
            for item in items:
                tt = eval(item)
                ll = list(tt)
                fout.write("%s\n" % '\t'.join(map(str, ll)))
    fin.close()
    fout.close()

if __name__ == '__main__':
    '''
    '''
    main()


