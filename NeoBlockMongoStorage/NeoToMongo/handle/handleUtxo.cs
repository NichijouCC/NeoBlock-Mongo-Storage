using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NeoToMongo
{

    [BsonIgnoreExtraElements]
    class UTXO
    {
        public UTXO()
        {
            addr = string.Empty;
            txid = string.Empty;
            n = -1;
            asset = string.Empty;
            value = 0;
            createHeight = -1;
            used = string.Empty;
            useHeight = -1;
            claimed = string.Empty;
        }

        public ObjectId _id { get; set; }
        public string addr { get; set; }
        public string txid { get; set; }
        public int n { get; set; }
        public string asset { get; set; }
        public decimal value { get; set; }
        public int createHeight { get; set; }
        public string used { get; set; }
        public int useHeight { get; set; }
        public string claimed { get; set; }
    }

    class HandleUtxo
    {

        static string collectionType = "utxo";
        static IMongoCollection<UTXO> _Collection;
        static IMongoCollection<UTXO> Collection
        {
            get
            {
                if (_Collection == null)
                {
                    var client = new MongoClient(Config.mongodbConnStr);
                    var database = client.GetDatabase(Config.mongodbDatabase);
                    _Collection = database.GetCollection<UTXO>(collectionType);
                }
                return _Collection;
            }
        }

        public static void handleTxItem(int blockindex, DateTime blockTime, MyJson.JsonNode_Object item)
        {
            //int blockindex = blockData["index"].AsInt();
            //var blockTx = blockData["tx"].AsList();
            //var blockTimeTS = blockData["time"].AsInt();
            //DateTime blockTime = TimeZoneInfo.ConvertTime(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), TimeZoneInfo.Local).AddSeconds(blockTimeTS);

            //foreach (MyJson.JsonNode_Object item in blockTx)
            {
                string txid = item["txid"].AsString();
                var vin_tx = item["vin"].AsList();
                var vout_tx = item["vout"].AsList();

                if (vout_tx.Count > 0)
                {
                    foreach (MyJson.JsonNode_Object voutitem in vout_tx)
                    {
                        string findStr = "{{txid:'{0}',n:{1}}}";
                        findStr = string.Format(findStr, txid, voutitem["n"].AsInt());
                        BsonDocument findB = BsonDocument.Parse(findStr);
                        var quaryArr = Collection.Find(findB).ToList();
                        if(quaryArr.Count==0)
                        {
                            UTXO utxo = new UTXO
                            {
                                addr = voutitem["address"].AsString(),
                                txid = txid,
                                n = voutitem["n"].AsInt(),
                                asset = voutitem["asset"].AsString(),
                                value = decimal.Parse(voutitem["value"].AsString()),
                                createHeight = blockindex
                            };
                            Collection.InsertOne(utxo);
                        }else
                        {
                            UTXO utxo = quaryArr[0];
                            utxo.addr = voutitem["address"].AsString();
                            utxo.asset = voutitem["asset"].AsString();
                            utxo.value = decimal.Parse(voutitem["value"].AsString());
                            utxo.createHeight = blockindex;
                            Collection.ReplaceOne(findB, utxo);
                        }
                    }
                }

                if (vin_tx.Count > 0)
                {
                    for (var i = 0; i < vin_tx.Count; i++)
                    {
                        var vinitem = vin_tx[i] as MyJson.JsonNode_Object;
                        string voutTx = vinitem["txid"].AsString();
                        int voutN = vinitem["vout"].AsInt();

                        //查找UTXO创建记录
                        string findStr = "{{txid:'{0}',n:{1}}}";
                        findStr = string.Format(findStr, voutTx, voutN);
                        BsonDocument findB = BsonDocument.Parse(findStr);
                        var quaryArr = Collection.Find(findB).ToList();

                        if (quaryArr.Count!=0)
                        {
                            UTXO utxo = quaryArr[0];
                            utxo.used = txid;
                            utxo.useHeight = blockindex;
                            Collection.ReplaceOne(findB, utxo);
                        }
                        else
                        {
                            UTXO utxo = new UTXO()
                            {
                                txid = voutTx,
                                n = voutN,
                                used = txid,
                                useHeight = blockindex
                            };
                            Collection.InsertOne(utxo);
                        }
                    }
                }

                //记录GAS领取
                if (item.ContainsKey("claims"))
                {
                    var claims = item["claims"].AsList();
                    if (claims.Count > 0)
                    {
                        foreach (MyJson.JsonNode_Object claimItem in claims)
                        {
                            string voutTx = claimItem["txid"].AsString();
                            int voutN = claimItem["vout"].AsInt();

                            //查找UTXO创建记录
                            string findStr = "{{txid:'{0}',n:{1}}}";
                            findStr = string.Format(findStr, voutTx, voutN);
                            BsonDocument findB = BsonDocument.Parse(findStr);
                            //UTXO utxo = Collection.Find(findB).ToList()[0];
                            var quaryArr = Collection.Find(findB).ToList();
                            if (quaryArr.Count != 0)
                            {
                                UTXO utxo = quaryArr[0];
                                utxo.claimed = txid;
                                Collection.ReplaceOne(findB, utxo);
                            }
                            else
                            {
                                UTXO utxo = new UTXO()
                                {
                                    txid = voutTx,
                                    n = voutN,
                                    claimed = txid,
                                };
                                Collection.InsertOne(utxo);
                            }
                        }
                    }
                }
            }
        }
    }
}
