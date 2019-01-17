using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace NeoToMongo
{
    class handleFullLog
    {
        static string collectionType = "fulllog";
        static IMongoCollection<BsonDocument> _Collection;
        static IMongoCollection<BsonDocument> Collection
        {
            get
            {
                if (_Collection == null)
                {
                    _Collection = Mongo.getCollection(collectionType);
                }
                return _Collection;
            }
        }

        public static void handle(string txid)
        {
            var res=Rpc.getfullloginfo(Config.NeoCliJsonRPCUrl, txid).Result;
            if(res!=null)
            {
                MyJson.JsonNode_Object logItem = new MyJson.JsonNode_Object();
                logItem.SetDictValue("txid", txid);
                logItem.SetDictValue("fulllog7z", res.ToString());
                Collection.InsertOne(BsonDocument.Parse(logItem.ToString()));
            }
        }
    }
}
