using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace NeoToMongo
{
    class handleTx
    {
        static string collectionType = "tx";
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

        async public static void handle(MyJson.JsonNode_Object blockData)
        {
            int blockindex = blockData["index"].AsInt();
            var blockTx = blockData["tx"].AsList();

            List<BsonDocument> listbson = new List<BsonDocument>();
            foreach(var item in blockTx)
            {
                item.AsDict().SetDictValue("blockindex",blockindex);
                listbson.Add(BsonDocument.Parse(item.ToString()));
            }

            if(listbson.Count>0)
            {
               await Collection.InsertManyAsync(listbson);
            }
        }
    }
}
