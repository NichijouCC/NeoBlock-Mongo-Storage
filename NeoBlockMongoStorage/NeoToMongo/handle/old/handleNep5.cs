using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace NeoToMongo
{
    class handleNep5
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

        public static void handle(int blockindex, DateTime blockTime,string txid, MyJson.JsonNode_Object notifyInfo)
        {
            var executionItem = notifyInfo["executions"].AsList()[0].AsDict();

            var besucced = executionItem["vmstate"].AsString();
            if(besucced!= "FAULT, BREAK")
            {
                var ntfArr = executionItem["notifications"].AsList();
                if (ntfArr.Count>0)
                {
                    for(int i=0;i<ntfArr.Count;i++)
                    {
                        var ntfItem = ntfArr[i] as MyJson.JsonNode_Object;
                        if (NEP5.beTransfer(ntfItem))
                        {
                            handleNEP5Asset.handle(ntfItem);
                            handleNEP5Transfer.handle(blockindex, blockTime,txid, i,ntfItem);
                        }
                    }
                }
            }

        }
    }
}
