using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace NeoToMongo
{

    [BsonIgnoreExtraElements]
    public class Address
    {
        public Address()
        {
            addr = string.Empty;
            firstuse = new AddrUse();
            lastuse = new AddrUse();
            txcount = 0;
            //balanceOfUTXO = new List<assetBalance>();
            //balanceOfNEP5 = new List<assetBalance>();
            //balances = new List<AddrBalance>();
        }

        public ObjectId _id { get; set; }
        public string addr { get; set; }
        public AddrUse firstuse { get; set; }
        public AddrUse lastuse { get; set; }
        public int txcount { get; set; }
        //public List<assetBalance> balanceOfUTXO{ get;set;}
        //public List<assetBalance> balanceOfNEP5 { get; set; }
        //public List<AddrBalance> balances { get; set; }
    }

    public class AddrUse
    {
        public AddrUse()
        {
            txid = string.Empty;
            blockindex = -1;
            blocktime = new DateTime();
        }

        public string txid { get; set; }
        public int blockindex { get; set; }
        public DateTime blocktime { get; set; }
    }

    class handleAddress
    {
        static string collectionType = "address";
        static IMongoCollection<Address> _Collection;
        static IMongoCollection<Address> Collection
        {
            get
            {
                if (_Collection == null)
                {
                    var client = new MongoClient(Config.mongodbConnStr);
                    var database = client.GetDatabase(Config.mongodbDatabase);
                    _Collection = database.GetCollection<Address>(collectionType);
                }
                return _Collection;
            }
        }

        public static void handle(int blockindex, string VoutVin_addr, string VoutVin_txid, DateTime blockTime)
        {
            //var findBson = BsonDocument.Parse("{addr:'" + VoutVin_addr + "'}");
            //var queryAddr = Collection.Find(findBson).ToList();
            var queryAddr = Mongo.Find(Collection, "addr", VoutVin_addr);

            Address addressItem;
            if(queryAddr.Count==0)
            {
                Address addr = new Address
                {
                    addr = VoutVin_addr,
                    firstuse = new AddrUse
                    {
                        txid = VoutVin_txid,
                        blockindex = blockindex,
                        blocktime = blockTime
                    },
                    lastuse = new AddrUse
                    {
                        txid = VoutVin_txid,
                        blockindex = blockindex,
                        blocktime = blockTime
                    },
                    txcount = 1
                };
                Collection.InsertOne(addr);
                addressItem = addr;
            }
            else
            {
                Address addr = queryAddr[0];
                addr = queryAddr[0];
                if (addr.lastuse.txid != VoutVin_txid)
                {
                    addr.txcount++;
                    if (addr.lastuse.blockindex < blockindex)
                    {
                        addr.lastuse = new AddrUse
                        {
                            txid = VoutVin_txid,
                            blockindex = blockindex,
                            blocktime = blockTime
                        };
                    }
                    var findBson = BsonDocument.Parse("{addr:'" + VoutVin_addr + "'}");
                    Collection.ReplaceOne(findBson, addr);
                }
                addr.lastuse = new AddrUse
                {
                    txid = VoutVin_txid,
                    blockindex = blockindex,
                    blocktime = blockTime
                };
                addressItem = addr;
            }

            //handle addresstx
            handleAddressTx.handle(addressItem);

        }
    }
}
