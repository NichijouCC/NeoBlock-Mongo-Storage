using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace NeoToMongo
{
    class handleNEP5Asset
    {
        static string collectionType = "NEP5asset";
        static IMongoCollection<NEP5.Asset> _Collection;
        public static IMongoCollection<NEP5.Asset> Collection
        {
            get
            {
                if (_Collection == null)
                {
                    var client = new MongoClient(Config.mongodbConnStr);
                    var database = client.GetDatabase(Config.mongodbDatabase);
                    _Collection = database.GetCollection<NEP5.Asset>(collectionType);
                }
                return _Collection;
            }
        }

        public static void handle(MyJson.JsonNode_Object notification)
        {
            string assetId = notification["contract"].AsString();

            //var findBsonNEP5AssetBson = BsonDocument.Parse("{assetid:'" + assetId + "'}");
            //var queryNEP5AssetBson = Collection.Find(findBsonNEP5AssetBson).ToList();
            var queryNEP5AssetBson = Mongo.Find(Collection, "assetid", assetId);

            if (queryNEP5AssetBson.Count == 0)
            {
                NEP5.Asset asset = new NEP5.Asset(Config.NeoCliJsonRPCUrl, assetId);
                Collection.InsertOne(asset);
            }
        }
    }
}
