using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace NeoToMongo
{
    class handleBlockSysFee
    {

        static string collectionType = "block_sysfee";
        static IMongoCollection<BlockSysfee> _Collection;
        static IMongoCollection<BlockSysfee> Collection
        {
            get
            {
                if (_Collection == null)
                {
                    var client = new MongoClient(Config.mongodbConnStr);
                    var database = client.GetDatabase(Config.mongodbDatabase);
                    _Collection = database.GetCollection<BlockSysfee>(collectionType);
                }
                return _Collection;
            }
        }

        public static void handle(MyJson.JsonNode_Object blockData)
        {
            int blockindex = blockData["index"].AsInt();
            var blockTx = blockData["tx"].AsList();

            decimal totalSysFee = 0;
            foreach (MyJson.JsonNode_Object item in blockTx)
            {
                var fee = decimal.Parse(item["sys_fee"].AsString());
                totalSysFee += fee;
            }
            //var blockSysfeeFindBson = BsonDocument.Parse("{index:" + (blockindex - 1) + "}");
            //var blockSysfeeQuery = Collection.Find(blockSysfeeFindBson).ToList();
            var blockSysfeeQuery = Mongo.Find(Collection, "index", (blockindex - 1).ToString());

            if (blockSysfeeQuery.Count > 0)
            {
                totalSysFee += blockSysfeeQuery[0].totalSysfee;
            }
            BlockSysfee bsf = new BlockSysfee(blockindex,totalSysFee);
            Collection.InsertOne(bsf);
        }

    }
}
