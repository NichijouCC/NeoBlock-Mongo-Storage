using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace NeoToMongo
{
    public class MongoIndexHelper
    {
        public static void initIndex(string mongodbConnStr, string mongodbDatabase)
        {
            //读取index配置文件
            JArray indexConfigJA = JArray.Parse(File.ReadAllText("indexSettings.json"));
            //var a = JsonConvert.SerializeObject(indexConfigJA);
            foreach (var J in indexConfigJA)
            {
                string collName = (string)J["collName"];
                foreach (var indexJ in (JArray)J["indexs"])
                {
                    var indexName = (string)indexJ["indexName"];
                    var indexDefinition = JsonConvert.SerializeObject(indexJ["indexDefinition"]);
                    var isUnique = (bool)indexJ["isUnique"];

                    setIndex(mongodbConnStr, mongodbDatabase, collName, indexDefinition, indexName, isUnique);
                }
            }
        }

        static void setIndex(string mongodbConnStr, string mongodbDatabase, string coll, string indexDefinition, string indexName, bool isUnique = false)
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>(coll);

            //检查是否已有设置index
            bool isSet = false;
            using (var cursor = collection.Indexes.List())
            {
                JArray JAindexs = JArray.Parse(cursor.ToList().ToJson());
                var query = JAindexs.Children().Where(index => (string)index["name"] == indexName);
                if (query.Count() > 0) isSet = true;
                // do something with the list...
            }

            if (!isSet)
            {
                try
                {
                    var options = new CreateIndexOptions { Name = indexName, Unique = isUnique };
                    collection.Indexes.CreateOne(indexDefinition, options);
                }
                catch { }
            }

            client = null;
        }
    }
}
