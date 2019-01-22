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

            StateInfo.init();

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
                    StateInfo.remoteBlockHeight = blockHeight;

                    if (blockHeight >= 0 && StateInfo.HandlingBlockCount < blockHeight)
                    {
                        SyncBlockToHeight(StateInfo.HandlingBlockCount + 1, blockHeight);
                    }
                    else
                    {
                        Thread.Sleep(100);
                    }
                }
                catch(Exception e)
                {
                    Console.WriteLine("async block:"+e.Message);
                    beActive = false;

                    Thread.Sleep(5000);
                }
            }
        }
        static SpinLock spinlock = new SpinLock();
        static void SyncBlockToHeight(int fromHeight, int toHeight)
        {
            List<Task> taskArr = new List<Task>();
            int taskCount = 5;
            for (int i = fromHeight; i <= toHeight; i++)
            {
                var j = i;
                Task newtask = Task.Factory.StartNew(() =>
                {
                    Thread.Sleep(((j-fromHeight)%taskCount)*3);

                    //var blockData =await handleBlock.handle(j);
                    var blockData =handleBlock.handle(j).Result;

                    handleTx.handle(blockData);

                    bool gotlock = false;
                    try
                    {
                        spinlock.Enter(ref gotlock);
                        StateInfo.HandledBlockCount++;
                    }
                    finally
                    {
                        if (gotlock) spinlock.Exit();
                    }
                });
                taskArr.Add(newtask);

                if(taskArr.Count>=5||i==toHeight)
                {
                    StateInfo.HandlingBlockCount += taskArr.Count;
                    Task.WaitAll(taskArr.ToArray());
                    taskArr.Clear();
                    //consoleMgr.showBlockCount();
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
