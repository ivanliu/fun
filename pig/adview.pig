register /grid/0/gs/pig/current/libexec/released/sds.jar;

-- A = load '/projects/btmp/prod/raw/DataGen/20121109/SID/LATEST/STITCHER/*part*,/projects/btmp/prod/raw/DataGen/2012111*/SID/LATEST/STITCHER/*part*,/projects/btmp/prod/raw/DataGen/2012112*/SID/LATEST/STITCHER/*part*,/projects/btmp/prod/raw/DataGen/20121130/SID/LATEST/STITCHER/*part*,/projects/btmp/prod/raw/DataGen/20121201/SID/LATEST/STITCHER/*part*,/projects/btmp/prod/raw/DataGen/20121202/SID/LATEST/STITCHER/*part*,/projects/btmp/prod/raw/DataGen/20121203/SID/LATEST/STITCHER/*part*,/projects/btmp/prod/raw/DataGen/20121204/SID/LATEST/STITCHER/*part*,/projects/btmp/prod/raw/DataGen/20121205/SID/LATEST/STITCHER/*part*,/projects/btmp/prod/raw/DataGen/20121206/SID/LATEST/STITCHER/*part*,/projects/btmp/prod/raw/DataGen/20121207/SID/LATEST/STITCHER/*part*,/projects/btmp/prod/raw/DataGen/20121208/SID/LATEST/STITCHER/*part*' using com.yahoo.yst.sds.ULT.ULTLoader() as (s,m,l);

A = load '/projects/btmp/prod/raw/DataGen/20121109/SID/LATEST/STITCHER/Sid-part-00000' using com.yahoo.yst.sds.ULT.ULTLoader() as (s,m,l);

B = foreach A generate s#'bcookie' as bid, s#'sid' as sid, flatten(l#'features') as features;
C = filter B by (bid != '') and (sid != '') and ((features#'k' == 'adv') or (features#'k' == 'c2adv') or (features#'k' == 'ICLKadv'));
D = foreach C generate bid, sid, features#'k' as k;

-- Calculate the counts for all impressions and c1 impressions.
D_one = group D all;
all_views = foreach D_one generate 'Total view ', COUNT(D);
dump all_views;

G = filter D by k == 'adv';
G_one = group G all;
c1_views = foreach G_one generate 'C1 view ', COUNT(G);
dump c1_views;

-- For unique # of SIDs and % of SIDs seen with more than one BID
H = group D by sid;
I = foreach H {
    unique_bid = DISTINCT D.bid;
    generate group, COUNT(unique_bid) as bid_cnt;
};
I_one = group I all;
unique_sid = foreach I_one generate 'Unique sid ', COUNT(I);
dump unique_sid;

J = filter I by bid_cnt > 1;
J_one = group J all;
multi_bid = foreach J_one generate 'Multiple bid ', COUNT(J);
dump multi_bid;

-- For unique # of BIDs and % of BIDs seen with more than one SID
K = group D by bid;
M = foreach K {
    unique_sid = DISTINCT D.sid;
    generate group, COUNT(unique_sid) as sid_cnt;
};
M_one = group M all;
unique_bid = foreach M_one generate 'Unique bid ', COUNT(M);
dump unique_bid;

N = filter M by sid_cnt > 1;
N_one = group N all;
multi_sid = foreach N_one generate 'Multiple sid ', COUNT(N);
dump multi_sid;

