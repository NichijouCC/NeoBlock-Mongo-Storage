using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Threading.Tasks;

namespace NeoToMongo
{
    class Program
    {
        static string configPath = "appsetting.json";
        static void Main(string[] args)
        {
            loadConfig();

            MongoIndexHelper.initIndex(Config.mongodbConnStr,Config.mongodbDatabase);



            AsyncLoop().Wait();



        }


        async static Task AsyncLoop()
        {
            while(true)
            {
                try
                {
                    int blockHeight = await Rpc.getblockcount(Config.NeoCliJsonRPCUrl) - 1;
                    if (blockHeight >= 0 && StateInfo.currentBlockHeight < blockHeight)
                    {
                        //Console.WriteLine("block count: " + blockcount);
                        await SyncBlockToHeight(StateInfo.currentBlockHeight + 1, blockHeight);
                    }
                    else
                    {
                        await Task.Delay(5000);
                        continue;
                    }
                }
                catch
                {
                    await Task.Delay(5000);
                }
            }
        }

        async static Task SyncBlockToHeight(int fromHeight, int toHeight)
        {
            for (int i = fromHeight; i <= toHeight; i++)
            {
                await Task.Run(async()=>
                {
                    var blockData= await Rpc.getblock(Config.NeoCliJsonRPCUrl, i);
                    blockData.Remove("confirmations");
                    blockData.Remove("nextblockhash");

                    //存储区块数据
                    MongoInsertOne("block", blockData);



                });
            }
        }


        /// <summary>
        /// 加載配置
        /// </summary>
        static void loadConfig()
        {
            try
            {
                Config.loadFromPath(configPath);
            }
            catch (Exception e)
            {
                Console.WriteLine("load config failed" + e.Message);
            }
        }



        private static void MongoInsertOne(string collName, MyJson.JsonNode_Object J, bool isAsyn = false)
        {
            var client = new MongoClient(Config.mongodbConnStr);
            var database = client.GetDatabase(Config.mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>(collName);

            var document = BsonDocument.Parse(J.ToString());

            if (isAsyn)
            {
                collection.InsertOneAsync(document);
            }
            else
            {
                collection.InsertOne(document);
            }

            client = null;
        }

    }
}
