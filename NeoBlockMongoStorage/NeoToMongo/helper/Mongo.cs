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

        public static List<T> Find<T>(IMongoCollection<T> collection, string key, object value)
        {
            if (value.GetType() == typeof(string))
            {
                var findBson = BsonDocument.Parse("{" + key + ":'" + value + "'}");
                return collection.Find(findBson).ToList();
            }
            else
            {
                var findBson = BsonDocument.Parse("{" + key + ":" + value + "}");
                return collection.Find(findBson).ToList();
            }
        }
        public static bool isDataExist<T>(IMongoCollection<T> collection, string key, object value)
        {
            var quaryArr = Find(collection, key, value);
            return quaryArr.Count != 0;
        }


        public static Couter GetSystemCounter(string counter)
        {
            var client = new MongoClient(Config.mongodbConnStr);
            var database = client.GetDatabase(Config.mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>("system_counter");

            var queryBson = BsonDocument.Parse("{counter:'" + counter + "'}");
            var query = collection.Find(queryBson).ToList();
            //if (query.Count == 0) { maxIndex = -1; }
            //else
            //{
            //    maxIndex = (int)query[0]["lastBlockindex"];
            //}
            client = null;
            if (query.Count == 0)
            {
                return new Couter();
            }else
            {
                var count = new Couter();
                count.lastBlockindex= (int)query[0]["lastBlockindex"];
                count.lastTxindex=(int)query[0]["lastTxindex"];

                return count;
            }
        }

       public static void SetSystemCounter(string counter, int lastBlockindex,int lastTxIndex=0)
        {
            var client = new MongoClient(Config.mongodbConnStr);
            var database = client.GetDatabase(Config.mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>("system_counter");

            BsonDocument setBson = BsonDocument.Parse("{counter:'" + counter + "',lastBlockindex:" + lastBlockindex + ",lastTxindex:" + lastBlockindex + "}");
            var queryBson = BsonDocument.Parse("{counter:'" + counter + "'}");
            var query = collection.Find(queryBson).ToList();
            if (query.Count == 0)
            {
                collection.InsertOne(setBson);
            }
            else
            {
                collection.ReplaceOne(queryBson, setBson);
            }

            client = null;
        }

        public struct Couter
        {
            public int lastBlockindex;
            public int lastTxindex;
        }

    }
}
