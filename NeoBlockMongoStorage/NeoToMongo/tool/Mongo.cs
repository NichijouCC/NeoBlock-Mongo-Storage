using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;

namespace NeoToMongo
{
    class Mongo
    {
        public static IMongoCollection<BsonDocument> getCollection(string type)
        {
            var client = new MongoClient(Config.mongodbConnStr);
            var database = client.GetDatabase(Config.mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>(type);
            client = null;
            return collection;
        }

        public static bool IsDataExist(string coll, string key, object value)
        {
            var client = new MongoClient(Config.mongodbConnStr);
            var database = client.GetDatabase(Config.mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>(coll);

            BsonDocument findBson = new BsonDocument();
            if (value.GetType() == typeof(string))
            {
                findBson = BsonDocument.Parse("{" + key + ":'" + value + "'}");
            }
            else
            {
                findBson = BsonDocument.Parse("{" + key + ":" + value + "}");
            }

            var query = collection.Find(findBson).ToList();

            int n = query.Count;

            client = null;

            if (n == 0) { return false; }
            else { return true; }
        }

    }
}
