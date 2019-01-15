using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace NeoToMongo
{
    class handleAsset
    {

        static string collectionType = "asset";
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

        public static void handle(string assetID)
        {
            if(!Mongo.IsDataExist(collectionType,"id",assetID))
            {
                var resasset=Rpc.getassetstate(Config.NeoCliJsonRPCUrl, assetID).Result;
                if(resasset.AsString()!=string.Empty)
                {
                    Collection.InsertOne(BsonDocument.Parse(resasset.ToString()));
                }
            }
        }
    }
}
