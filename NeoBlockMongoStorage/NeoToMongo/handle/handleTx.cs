using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace NeoToMongo
{
    class handleTx
    {
        public static string collectionType = "tx";
        static IMongoCollection<BsonDocument> _Collection;
        public static IMongoCollection<BsonDocument> Collection
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

            List<BsonDocument> listbson = new List<BsonDocument>();

            var count = Mongo.GetSystemCounter(collectionType);
            int startTxIndex = count.lastBlockindex < blockindex ? 0:count.lastTxindex;

            for(
                int i= startTxIndex; i< blockTx.Count;i++)
            {
                var item = blockTx[i] as MyJson.JsonNode_Object;
                item.AsDict().SetDictValue("blockindex", blockindex);
                listbson.Add(BsonDocument.Parse(item.ToString()));
                Collection.InsertOne(BsonDocument.Parse(item.ToString()));

                Mongo.SetSystemCounter(collectionType,blockindex,i);
                //HandleUtxo.handle2(blockindex, blockTime,item);
                //Task.Run(() =>
                //{
                //    string txid = item["txid"].AsString();
                //    handleFullLog.handle(txid);
                //});
            }

            //if(listbson.Count>0)
            //{
            //   Collection.InsertMany(listbson);
            //}
        }
    }
}
