
from bson.code import Code
from pymongo import MongoClient as Connection
from datetime import datetime,timedelta

mapper = Code(""" function() {
   day_hour_min = Date.UTC(this.timestamp.getFullYear(), this.timestamp.getMonth(),
                           this.timestamp.getDate(), this.timestamp.getHours(),
                           this.timestamp.getMinutes());
   emit( { timestamp:day_hour_min, project_id:this.project_id, counter_name:this.counter_name }, this.counter_volume );
   }
   """)

reducer = Code(""" function(key, values) {
   var total = 0;
   for(var i in values) {
      total += values[i];
   }
  return total / values.length();
  }
  """)

t1 = datetime.utcnow() - timedelta(seconds=3600)
t2 = datetime.utcnow() - timedelta(seconds=0)

q1 = { 'timestamp': { '$gt': t1, '$lt': t2 }, 'counter_name': 'cpu_util' }
q2 = { 'timestamp': { '$gt': t1, '$lt': t2 }, 'counter_name': 'memory.resident' }

find_filter = { 'timestamp': { '$gte': t1, '$lt': t2} }

db = Connection().ceilometer

#db.average_util.drop()

#TODO : out - merge data to another server
#
#   out: { <action>: <collectionName>
#        [, db: <dbName>]
#        [, sharded: <boolean> ]
#        [, nonAtomic: <boolean> ] }
#

result = db.meter.map_reduce(mapper, reducer, out={'merge': 'average_util'},  query=q1 )
result = db.meter.map_reduce(mapper, reducer, out={'merge': 'average_util'},  query=q2 )

for doc in result.find():
    print doc

