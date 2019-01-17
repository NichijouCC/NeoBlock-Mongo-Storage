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

        public static void handleTxItem(int blockindex, DateTime blockTime, MyJson.JsonNode_Object txItem)
        {
            if (txItem["type"].AsString() == "InvocationTransaction")
            {
                string txid = txItem["txid"].AsString();

                var quaryArr = Mongo.Find(Collection, "txid", txid);
                MyJson.JsonNode_Object targetNotify;
                if(quaryArr.Count==0)
                {
                    targetNotify = Rpc.getapplicationlog(Config.NeoCliJsonRPCUrl, txid).Result;
                    if (targetNotify != null)
                    {
                        Collection.InsertOne(BsonDocument.Parse(targetNotify.ToString()));
                    }
                }else
                {
                    targetNotify =MyJson.Parse(quaryArr[0].ToJson()) as MyJson.JsonNode_Object;
                }

                //todo handleNep5
                if(targetNotify != null)
                {
                    var executionItem = targetNotify["executions"].AsList()[0].AsDict();
                    var besucced = executionItem["vmstate"].AsString();
                    if (besucced != "FAULT, BREAK")
                    {
                        var ntfArr = executionItem["notifications"].AsList();
                        if (ntfArr.Count > 0)
                        {
                            for (int i = 0; i < ntfArr.Count; i++)
                            {
                                var ntfItem = ntfArr[i] as MyJson.JsonNode_Object;
                                if (NEP5.beTransfer(ntfItem))
                                {
                                    handleNEP5Asset.handle(ntfItem);
                                    handleNEP5Transfer.handle(blockindex, blockTime, txid, i, ntfItem);
                                }
                            }
                        }
                    }
                }


            }
        }
    }
}
