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

        async public static Task handle(MyJson.JsonNode_Object blockData)
        {
            int blockindex = blockData["index"].AsInt();
            var blockTx = blockData["tx"].AsList();
            var blockTimeTS = blockData["time"].AsInt();
            DateTime blockTime = TimeZoneInfo.ConvertTime(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), TimeZoneInfo.Local).AddSeconds(blockTimeTS);

            List<BsonDocument> listbson = new List<BsonDocument>();

            for(int i= 0; i< blockTx.Count;i++)
            {
                var item = blockTx[i] as MyJson.JsonNode_Object;

                //var quaryArr=Mongo.Find(Collection, "txid", item["txid"].AsString());
                //if(quaryArr.Count==0)
                //{
                //    item.SetDictValue("txindex", i);
                //    item.AsDict().SetDictValue("blockindex", blockindex);
                //    listbson.Add(BsonDocument.Parse(item.ToString()));
                //    Collection.InsertOne(BsonDocument.Parse(item.ToString()));
                //    //Mongo.SetSystemCounter(collectionType, blockindex, i);
                //}else
                {
                    item.SetDictValue("txindex", i);
                    item.AsDict().SetDictValue("blockindex", blockindex);
                }

                {
                    Task task1 = Task.Factory.StartNew(() =>
                    {
                        handleAsset.handleTxItem(blockindex, blockTime, item);
                    });
                    Task task2 = Task.Factory.StartNew(() =>
                    {
                        HandleUtxo.handleTxItem(blockindex, blockTime, item);
                    });
                    Task task3 = Task.Factory.StartNew(() =>
                    {
                        handleNotify.handleTxItem(blockindex, blockTime, item);
                    });
                    await Task.WhenAll(task1,task2,task3);
                    //handleAddress.handleTxItem(blockindex, blockTime, item);
                }
            }
            //if(listbson.Count>0)
            //{
            //   Collection.InsertMany(listbson);
            //}
        }
    }
}
