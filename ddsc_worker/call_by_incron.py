from ddsc_worker.fugrotasks import data_convert, data_validate, write2_cassandra, data_delete
import sys

pathDir = sys.argv[1] + "//"
fileName = sys.argv[2]

src = pathDir + fileName

(data_convert.s(src) | data_validate.s(4,7) | write2_cassandra.s() | data_delete.s(src))()

print "Import task well Done!"
