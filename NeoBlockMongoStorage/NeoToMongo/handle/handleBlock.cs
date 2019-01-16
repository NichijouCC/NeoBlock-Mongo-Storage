using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

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
        public static MyJson.JsonNode_Object handle(int height)
        {
            //if(!Mongo.isDataExist(Collection, "index", height))
            //{
            //    var blockData =Rpc.getblock(Config.NeoCliJsonRPCUrl, height).Result;
            //    blockData.Remove("confirmations");
            //    blockData.Remove("nextblockhash");

            //    Collection.InsertOne(BsonDocument.Parse(blockData.ToString()));

            //    return blockData;
            //}else
            //{
            //   var query= Mongo.Find(Collection,"index",height);
            //    BsonDocument queryB = query[0].AsBsonDocument;
            //    //queryB.Remove("_id");
            //    //queryB.Remove("nonce");
            //    var jsonWriterSettings = new JsonWriterSettings { OutputMode = JsonOutputMode.Strict };
            //    MyJson.JsonNode_Object block = MyJson.Parse(queryB.ToJson(jsonWriterSettings)) as MyJson.JsonNode_Object;
            //    return block;
            //}
            var counter = Mongo.GetSystemCounter(collectionType);
            if(counter.lastBlockindex<height)
            {
                var blockData = Rpc.getblock(Config.NeoCliJsonRPCUrl, height).Result;
                blockData.Remove("confirmations");
                blockData.Remove("nextblockhash");

                Collection.InsertOne(BsonDocument.Parse(blockData.ToString()));
                Mongo.SetSystemCounter(collectionType, height);
                return blockData;
            }
            else
            {
                var query = Mongo.Find(Collection, "index", height);
                BsonDocument queryB = query[0].AsBsonDocument;
                //queryB.Remove("_id");
                //queryB.Remove("nonce");
                var jsonWriterSettings = new JsonWriterSettings { OutputMode = JsonOutputMode.Strict };
                MyJson.JsonNode_Object block = MyJson.Parse(queryB.ToJson(jsonWriterSettings)) as MyJson.JsonNode_Object;
                return block;
            }
            
        }
    }
}
