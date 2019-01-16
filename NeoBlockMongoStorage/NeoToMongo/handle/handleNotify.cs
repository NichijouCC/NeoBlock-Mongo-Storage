using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace NeoToMongo
{
    class handleNotify
    {
        static string collectionType = "notify";
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

        public static void handle(MyJson.JsonNode_Object blockData)
        {
            int blockindex = blockData["index"].AsInt();
            var blockTx = blockData["tx"].AsList();
            var blockTimeTS = blockData["time"].AsInt();
            DateTime blockTime = TimeZoneInfo.ConvertTime(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), TimeZoneInfo.Local).AddSeconds(blockTimeTS);

            foreach (MyJson.JsonNode_Object txItem in blockTx)
            {
                if(txItem["type"].AsString() == "InvocationTransaction")
                {
                    string txid = txItem["txid"].AsString();
                    var resNotify = Rpc.getapplicationlog(Config.NeoCliJsonRPCUrl,txid).Result;
                    if(resNotify!=null)
                    {
                        Collection.InsertOne(BsonDocument.Parse(resNotify.ToString()));
                    }

                    //todo handleNep5
                    handleNep5.handle(blockindex, blockTime,txid, resNotify);

                }
            }
        }
    }
}
