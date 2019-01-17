using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NeoToMongo
{
    class Program
    {
        static string configPath = "setting/appsettings.json";
        static void Main(string[] args)
        {
            loadConfig();

            MongoIndexHelper.initIndex(Config.mongodbConnStr,Config.mongodbDatabase);

            Log.clearLog();

            Task.Run(()=> {
                consoleMgr.run();
            });
            AsyncLoop().Wait();

            while (true) { };
        }

        private static bool beActive=true;
        async static Task AsyncLoop()
        {
            while(true&&beActive)
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
                catch(Exception e)
                {
                    Console.WriteLine("async block:"+e.Message);
                    beActive = false;

                    await Task.Delay(5000);
                }
            }
        }

        async static Task SyncBlockToHeight(int fromHeight, int toHeight)
        {
            List<Task> taskArr = new List<Task>();
            for (int i = fromHeight; i <= toHeight; i++)
            {
                Task newtask = Task.Factory.StartNew( async() =>
                {
                    var blockData = handleBlock.handle(i);
                    await handleTx.handle(blockData);
                    StateInfo.currentBlockHeight++;
                });
                taskArr.Add(newtask);

                if(taskArr.Count>=50|| taskArr.Count+ toHeight - fromHeight <50)
                {
                    await Task.WhenAll(taskArr);
                    taskArr.Clear();
                }
            }
        }

        //private static Queue<List<Task>> queueTask = new Queue<List<Task>>();
        //private static List<Task> currentArrTask = new List<Task>();
        //static void asyncBlockData(int fromHeight, int toHeight)
        //{
        //    for (int i = fromHeight; i <= toHeight; i++)
        //    {
        //        var blockData = handleBlock.handle(i);
        //        Task newtask = Task.Factory.StartNew(async () =>
        //        {
        //            //var blockData = handleBlock.handle(i);
        //            await handleTx.handle(blockData);
        //            StateInfo.currentBlockHeight++;
        //        });
        //        currentArrTask.Add(newtask);
        //        if(currentArrTask.Count>=10)
        //        {
        //            queueTask.Enqueue(currentArrTask);
        //            currentArrTask = new List<Task>();
        //        }
        //    }
        //}

        



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
                Console.WriteLine("load config failed." + e.Message);
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
