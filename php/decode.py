import base64

def decode(encoded_code):
    '''
    decode the data which is encoded by base64
    '''
    decoded_code = base64.b64decode(encoded_code)
    #decoded_code = base64.standard_b64encode(encoded_code)

    return decoded_code

def get_code(raw_data, prefix, suffix):
    '''
    extract code from raw data based on prefix and suffix
    '''
    start = raw_data.find(prefix)
    end = raw_data.find(suffix)

    print "starting with...", raw_data[start+len(prefix) : start+20]
    print "ending with...", raw_data[end-20 : end]

    return raw_data[start+len(prefix) : end]

if __name__ == '__main__':
    '''
    '''
    inp_file = '/Users/kailiu/codes/codes/php/runtime.php'
    out_file = '/Users/kailiu/codes/codes/php/runtime.php.decoded'

    prefix = 'eval($O00O0O("'
    suffix = '"));'

    fin = open(inp_file)
    fout = open(out_file, 'w')

    raw_data = fin.read()
    encoded_code = get_code(raw_data, prefix, suffix)
    decoded_code = decode(encoded_code)
    fout.write(decoded_code)

    fin.close()
    fout.close()

