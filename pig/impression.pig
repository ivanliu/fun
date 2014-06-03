REGISTER /home/d_pbp/OOZIE/current/share/oozie/workflows/common/lib/admovate.jar;

DEFINE DateKey com.yahoo.ads.pb.platform.etl.pig.udf.AdmovateDateKey();
DEFINE ValidateTld com.yahoo.ads.pb.platform.etl.pig.udf.TldValidator();
DEFINE AgeBucket com.yahoo.ads.pb.platform.etl.pig.udf.AgeToBucketID();

A = load '$IMP_INPUT' using com.yahoo.ads.pb.platform.etl.pig.load.ProtobufJsonPigLoader('com.yahoo.ads.pb.application.report.ImpressionDataProtos.ImpressionData');
B = foreach A generate line_id, 1 as imp_count;
C = filter B by (line_id == 1307) or (line_id == 1309); 
D = group C by line_id;
E = foreach D generate group, SUM(C.imp_count);

store E into '$OUTPUT' using PigStorage('\t');

