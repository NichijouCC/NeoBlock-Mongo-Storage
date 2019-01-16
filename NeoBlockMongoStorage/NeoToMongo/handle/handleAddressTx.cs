using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace NeoToMongo
{
    class handleAddressTx
    {
        static string collectionType = "address_tx";
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

        public static void handle(Address addr)
        {
            BsonDocument B = new BsonDocument();
            B.Add("addr", addr.addr);
            B.Add("txid", addr.lastuse.txid);
            B.Add("blockindex", addr.lastuse.blockindex);
            B.Add("blocktime", addr.lastuse.blocktime);
            Collection.InsertOne(B);
        }
    }
}
