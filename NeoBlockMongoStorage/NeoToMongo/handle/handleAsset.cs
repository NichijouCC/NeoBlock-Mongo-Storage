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
            //var findBsonNEP5AssetBson = BsonDocument.Parse("{id:'" + assetID + "'}");
            //var queryNEP5AssetBson = Collection.Find(findBsonNEP5AssetBson).ToList();

            var queryNEP5AssetBson = Mongo.Find(Collection, "id", assetID);

            if (queryNEP5AssetBson.Count==0)
            {
                var resasset=Rpc.getassetstate(Config.NeoCliJsonRPCUrl, assetID).Result;
                if(resasset.AsString()!=string.Empty)
                {
                    Collection.InsertOne(BsonDocument.Parse(resasset.ToString()));
                }
            }
        }

        public static void handleTxItem(int blockindex, DateTime blockTime, MyJson.JsonNode_Object txItem)
        {
            var vout_tx = txItem["vout"].AsList();
            if(vout_tx.Count>0)
            {
                foreach (MyJson.JsonNode_Object voutitem in vout_tx)
                {
                    var assetID = voutitem["asset"].AsString();
                    if (!Mongo.isDataExist(Collection, "id", assetID))
                    {
                        var resasset = Rpc.getassetstate(Config.NeoCliJsonRPCUrl, assetID.Replace("0x", "")).Result;
                        if (resasset!=null)
                        {
                            Collection.InsertOne(BsonDocument.Parse(resasset.ToString()));
                        }
                    }
                }
            }
        }
    }
}
