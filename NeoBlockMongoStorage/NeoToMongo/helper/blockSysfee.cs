using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;

namespace NeoToMongo
{
    [BsonIgnoreExtraElements]
    class BlockSysfee
    {
        public BlockSysfee(int blockIndex,decimal sysFee) {
            index = blockIndex;
            totalSysfee = sysFee;
        }

        public ObjectId _id { get; set; }
        public int index { get; set; }
        public decimal totalSysfee { get; set; }
    }
}
