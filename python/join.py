import os, sys

inputpath = "input_feeds"
mappers="DAC:com.yahoo.miners.btmp.slingshotgrid.processing.fe.DACMapper;MAIL:com.yahoo.miners.btmp.slingshotgrid.processing.fe.FETextMapper"
formats="DAC:org.apache.hadoop.mapred.SequenceFileInputFormat;MAIL:org.apache.hadoop.mapred.TextInputFormat"
prefixs="DAC:/projects/btmp/prod/HourlyDAC/DataGen;MAIL:/projects/targeting/mrt/prod/yuid2sid/enrich_data"
suffixs="DAC:SID/LATEST/STITCHER/;MAIL:"

def parse_map(_str):
    '''
    parse mapper/format/prefix settings
    '''
    kvs = {}
    parts = _str.split(";")
    for part in parts:
        _kv = part.split(":")
        kvs[_kv[0]] = _kv[1]

    return kvs

def extract_date(_str, prefix, suffix):
    '''
    extract date from path
    '''
    prefix_found = _str.find(prefix)
    suffix_found = _str.find(suffix)
    if (prefix_found == -1) or (suffix_found == -1):
        return ""

    start = _str.find(prefix) + len(prefix)
    if suffix != "":
        return _str[start : _str.find(suffix)].strip("/")
    else:
        return _str[start : ].strip("/")

if __name__ == '__main__':

    mapper_dict = parse_map(mappers)
    format_dict = parse_map(formats)
    prefix_dict = parse_map(prefixs)
    suffix_dict = parse_map(suffixs)

    input_str = ""
    mapper_str = ""
    format_str = ""

    # get dates
    kvs = {}
    fin = open(inputpath)
    for line in fin:
        line = line.strip()
        parts = line.split(":")
        tag, path = parts
        if tag not in kvs:
            kvs[tag] = []

        prefix = prefix_dict.get(tag)
        suffix = suffix_dict.get(tag)
        date = extract_date(path, prefix, suffix)
        kvs[tag].append(date)
    fin.close()

    # aggregation
    for tag in kvs.keys():
        dates = kvs.get(tag)
        aggregated_path = prefix_dict.get(tag) + '/{' + '\,'.join(dates) + '}/' + suffix_dict.get(tag)

        # input
        input_str += aggregated_path + ','

        # mapper
        mapper_str += aggregated_path + ';' + mapper_dict.get(tag) + ','

        # format
        format_str += aggregated_path + ';' + format_dict.get(tag) + ','

    print "= Input = \n" + input_str.strip(',')
    print "\n= Mapper = \n" + mapper_str.strip(',')
    print "\n= Format = \n" + format_str.strip(',')


