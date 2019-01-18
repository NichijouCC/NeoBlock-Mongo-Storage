using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NeoToMongo
{
    class handleBlock
    {
        public static string collectionType = "block";
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
        async public static Task<MyJson.JsonNode_Object> handle(int height)
        {
            var queryArr=Mongo.Find(Collection, "index", height);
            if(queryArr.Count==0)
            {
                var blockData = await Rpc.getblock(Config.NeoCliJsonRPCUrl, height);
                blockData.Remove("confirmations");
                blockData.Remove("nextblockhash");

                Collection.InsertOne(BsonDocument.Parse(blockData.ToString()));
                //Mongo.SetSystemCounter(collectionType, height);
                return blockData;
            }else
            {
                BsonDocument queryB = queryArr[0].AsBsonDocument;
                var jsonWriterSettings = new JsonWriterSettings { OutputMode = JsonOutputMode.Strict };
                MyJson.JsonNode_Object block = MyJson.Parse(queryB.ToJson(jsonWriterSettings)) as MyJson.JsonNode_Object;
                return block;
            }
            
        }
    }
}
