using System;
using MongoDB;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Timers;
using System.Threading;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.IO;
using MongoDB.Bson.IO;
using System.Linq;
using log4net;
using log4net.Repository;
using log4net.Config;

namespace NeoBlockMongoStorage
{
    class Program
    {
        static CoreHttpHelper chh = new CoreHttpHelper();
        static NEP5 nep5 = new NEP5();
        static ILog log;
        static string mongodbConnStr = string.Empty;
        static string mongodbDatabase = string.Empty;
        static string NeoCliJsonRPCUrl = string.Empty;
        static int sleepTime = 0;
        static bool utxoIsSleep = false;
        static bool isDoNotify = true;
        static bool isDoFullLogs = true;
        static string cliType = "neo"; //neo 原版 ；nel 改版
        static int batchSize = 100;

        static void Main(string[] args)
        {
            //初始化log4net
            ILoggerRepository repository = LogManager.CreateRepository("NeoBlockMongoStorage");
            XmlConfigurator.Configure(repository, new FileInfo("log4net.config"));
            log = LogManager.GetLogger(repository.Name, "NeoBlockMongoStorage_Log");

            var config = new ConfigurationBuilder()
                .AddInMemoryCollection()    //将配置文件的数据加载到内存中
                .SetBasePath(System.IO.Directory.GetCurrentDirectory())   //指定配置文件所在的目录
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)  //指定加载的配置文件
                .Build();    //编译成对象  
            mongodbConnStr = config["mongodbConnStr"];
            mongodbDatabase = config["mongodbDatabase"];
            NeoCliJsonRPCUrl = config["NeoCliJsonRPCUrl"];
            sleepTime = int.Parse(config["sleepTime"]);
            try
            {
                batchSize = int.Parse(config["batchSize"]);
            } catch
            {
                batchSize = 100;
            }
            

            //文本输出启动参数
            string logStartStr = "入库启动\r\n数据库连接：{0}\r\n数据库名：{1}\r\nCLI连接：{2}\r\n睡眠时间：{3}ms";
            logStartStr = string.Format(logStartStr, mongodbConnStr, mongodbDatabase, NeoCliJsonRPCUrl, sleepTime);
            log.Info(logStartStr);

            if (int.Parse(config["utxoIsSleep"]) == 1) {
                utxoIsSleep = true;
            }
            if (config["isDoNotify"] != null){
                if (int.Parse(config["isDoNotify"]) != 1)
                {
                    isDoNotify = false;
                }
            }
            if (config["isDoFullLogs"] != null)
            {
                if (int.Parse(config["isDoFullLogs"]) != 1)
                {
                    isDoFullLogs = false;
                }
            }
            if (config["cliType"] != null)
            {
                cliType = config["cliType"];
            }
            Console.WriteLine("NeoBlockMongoStorage Start!");
            Console.WriteLine("*************************************");
            Console.WriteLine("mongodbConnStr" + mongodbConnStr);
            Console.WriteLine("mongodbDatabase" + mongodbDatabase);
            Console.WriteLine("NeoCliJsonRPCUrl" + NeoCliJsonRPCUrl);
            Console.WriteLine("sleepTime" + sleepTime);
            Console.WriteLine("cliType " + cliType);
            Console.WriteLine("*************************************");

            Console.WriteLine("Block MaxIndex in DB:" + GetSystemCounter("block"));

            //每次启动初始化索引initIndex
            mongoIndexHelper mih = new mongoIndexHelper();
            mih.initIndex(mongodbConnStr, mongodbDatabase);
            Console.WriteLine("表索引初始化已完成！");

            //创建任务
            Task task_StorageUTXO = new Task(() => {
                
                Console.WriteLine("异步循环执行StorageUTXOData开始");
                try
                {
                    while (true)
                    {
                        DateTime start = DateTime.Now;

                        //统计处理UTXO数据
                        StorageUTXOData();

                        if (utxoIsSleep) { Thread.Sleep(sleepTime); }

                        DateTime end = DateTime.Now;
                        var doTime = (end - start).TotalMilliseconds;
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine("StorageUTXOData in " + doTime + "ms");
                    }
                }
                catch (Exception e)
                {
                    log.Error("task_StorageUTXO\r\nErrorMsg:\r\n" + e.Message);
                }
            });
            Task task_StorageNotify = new Task(() => {
                Console.WriteLine("异步循环执行StorageNotifyData开始");
                while (true)
                {
                    //处理notify数据
                    StorageNotifyData();

                    //借用utxo睡眠开关
                    if (utxoIsSleep) { Thread.Sleep(sleepTime); }

                    //if (cliType == "nel")//适配nel改版cli时Notify才睡眠，加快neo原版cli入库
                    //{
                    //    Thread.Sleep(sleepTime);
                    //}                       
                }
            });
            Task task_StorageNEP5 = new Task(() => {
                Console.WriteLine("异步循环执行StorageNEP5Data开始");
                while (true)
                {
                    //处理NEP5数据
                    StorageNEP5Data();

                    //借用utxo睡眠开关
                    if (utxoIsSleep) { Thread.Sleep(sleepTime); }
                }
            });
            Task task_StorageFulllog = new Task(() => {
                Console.WriteLine("异步循环执行StorageFulllogData开始");
                while (true)
                {
                    //处理fulllog数据
                    StorageFulllogData();

                    Thread.Sleep(sleepTime);
                }
            });
            Task task_StorageBlockTotalSysfee = new Task(() => {

                Console.WriteLine("异步循环执行StorageBlockTotalSysfee开始");
                while (true)
                {
                    DateTime start = DateTime.Now;

                    //统计处理块总系统费数据
                    StorageBlockTotalSysfee();

                    //借用utxo睡眠开关
                    if (utxoIsSleep) { Thread.Sleep(sleepTime); }

                    DateTime end = DateTime.Now;
                    var doTime = (end - start).TotalMilliseconds;
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("StorageBlockTotalSysfee in " + doTime + "ms");
                }
            });
            Task task_StorageNep5AddressInfo = new Task(() => {

                Console.WriteLine("异步循环执行StorageNep5Address开始");
                while (true)
                {
                    DateTime start = DateTime.Now;

                    //从Nep5视角统计地址和地址交易（按块处理）
                    //StorageAddressInfoByNEP5transfer();
                    StorageAddressInfoByNEP5transferNew();

                    //借用utxo睡眠开关
                    if (utxoIsSleep) { Thread.Sleep(sleepTime); }

                    DateTime end = DateTime.Now;
                    var doTime = (end - start).TotalMilliseconds;
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("StorageNep5AddressInfo in " + doTime + "ms");
                }
            });
            //启动任务
            task_StorageUTXO.Start();
            if (isDoNotify) { task_StorageNotify.Start(); }
            task_StorageNEP5.Start();
            if (isDoFullLogs) { task_StorageFulllog.Start(); }
            task_StorageBlockTotalSysfee.Start();
            task_StorageNep5AddressInfo.Start();

            //主进程(同步)
            while (true)
            {
                //处理块数据
                StorageBlockTXData();
                ////处理交易数据
                //StorageTxData(); 交易数据在处理块数据时同时处理

                ////统计处理UTXO数据
                //StorageUTXOData();
                ////处理notify数据
                //StorageNotifyData();
                ////处理fulllog数据
                //StorageFulllogData();

                Thread.Sleep(sleepTime);
            }



            //Timer t = new Timer(100);
            //t.Enabled = true;
            //t.Elapsed += T_Elapsed;          

            //Console.ReadKey();
        }

        //private static void T_Elapsed(object sender, ElapsedEventArgs e)
        //{
        //    StorageBaseData();
        //}
        
        private static void StorageBlockTXData()
        {
            int maxIndex = GetSystemCounter("block");
            //检查当前已有区块是否已存所有交易
            if (!IsDataExist("tx", "blockindex", maxIndex))
            {
                //已存区块没有存tx则再处理一遍
                DoStorageBlockTXData(maxIndex);
            }

            int storageIndex = maxIndex + 1;
            DoStorageBlockTXData(storageIndex);
        }

        private static void DoStorageBlockTXData(int doIndex)
        {
            DateTime start = DateTime.Now;

            //获取Cli block数据
            string resBlock = GetNeoCliData("getblock", new object[]
                {
                    doIndex,
                    1
                });

            //获取有效数据则存储Mongodb
            if (resBlock != string.Empty)
            {
                //只处理没有存储过的
                if (!IsDataExist("block", "index", doIndex))
                {
                    JObject blockJ = JObject.Parse(resBlock);
                    //去除非块原生数据
                    blockJ.Remove("confirmations");
                    blockJ.Remove("nextblockhash");

                    //存储区块数据
                    MongoInsertOne("block", blockJ);
                }

                //依据区块数据存储交易数据
                DoStorageTxDataByBlock(resBlock);

                DateTime end = DateTime.Now;
                var doTime = (end - start).TotalMilliseconds;
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("StorageBlockTxData On Block " + doIndex + " in " + doTime + "ms");
                Console.ForegroundColor = ConsoleColor.White;

                //更新已处理块高度
                SetSystemCounter("block", doIndex);
            }
        }

        private static void DoStorageTxDataByBlock(string block)
        {
            JObject blockJ = JObject.Parse(block);
            int blockIndex = (int)blockJ["index"];
            JArray blockTx = (JArray)blockJ["tx"];

            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>("tx");

            List<BsonDocument> listBson = new List<BsonDocument>();
            foreach (JObject j in blockTx)
            {
                j.Add("blockindex", blockIndex);
                listBson.Add(BsonDocument.Parse(Newtonsoft.Json.JsonConvert.SerializeObject(j)));
            }
            if (listBson.Count > 0)
            {                
                if (IsDataExist("tx", "txid", listBson[0]["txid"].AsString))
                {
                    //如果矿工交易重复，就抛弃矿工交易
                    listBson.RemoveAt(0);

                    if (listBson.Count > 0) {
                        //判断第一个非矿工交易是否重复，如果不重复就入库剩下的交易
                        if (!IsDataExist("tx", "txid", listBson[0]["txid"].AsString))
                        {
                            //批量写入块所有交易数据
                            collection.InsertMany(listBson);
                        }
                    }
                }
                else//如果矿工交易都不重复，直接入库
                {
                        //批量写入块所有交易数据
                        collection.InsertMany(listBson);
                }
               
            }

            client = null;
        }

        private static void DoAssetStorageByVout(string assetID) {
            DateTime start = DateTime.Now;

            //var client = new MongoClient(mongodbConnStr);
            //var database = client.GetDatabase(mongodbDatabase);
            //var collection = database.GetCollection<BsonDocument>("tx");
            //BsonDocument findB = BsonDocument.Parse("{type:'RegisterTransaction'}");
            //var query = collection.Find(findB).ToList();
            //if (query.Count > 0)
            //{
            //    collection = database.GetCollection<BsonDocument>("asset");

            //    foreach (var tx in query)
            //    {
            //        string txid = tx["txid"].AsString;
                    //只有asset没有记录才会处理
                    if (!IsDataExist("asset", "id", assetID)) {
                        //获取Cli asset数据
                        string resAsset = GetNeoCliData("getassetstate", new object[] { assetID });

                        //控制接口调用频度
                        Thread.Sleep(sleepTime);

                        //获取有效数据则存储asset
                        if (resAsset != string.Empty)
                        {
                            //var jsonWriterSettings = new JsonWriterSettings { OutputMode = JsonOutputMode.Strict };
                            //JObject assetJ = JObject.Parse(tx["asset"].ToJson(jsonWriterSettings));
                            //assetJ.Add("id", txid);

                            MongoInsertOne("asset",JObject.Parse(resAsset));

                            DateTime end = DateTime.Now;
                            var doTime = (end - start).TotalMilliseconds;
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine("StorageAssetData in " + doTime + "ms");
                        }
                //    }
                //}
                    }
        }

        private static void StorageBlockTotalSysfee()
        {
            var appName = "totalsysfee";
            var maxBlockindex = GetSystemCounter(appName);

            var storageBlockindex = maxBlockindex + 1;
            //处理块不能超过已入库的最大块
            if (storageBlockindex <= GetSystemCounter("block"))
            {
                DoStorageBlockTotalSysfeeByBlock(storageBlockindex, appName);
            }
        }

        private static void DoStorageBlockTotalSysfeeByBlock(int blockindex,string appName)
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>("block");

            var findBson = BsonDocument.Parse("{index:" + blockindex + "}");
            var query = collection.Find(findBson).ToList();

            bool isDone = IsDataExist("block_sysfee", "index", blockindex);

            if (query.Count > 0)
            {
                //判断块没有处理才处理
                if (!isDone) {
                    var jsonWriterSettings = new JsonWriterSettings { OutputMode = JsonOutputMode.Strict };
                    JArray txJA = JArray.Parse(query[0]["tx"].AsBsonArray.ToJson(jsonWriterSettings));
                    foreach (JObject j in txJA)
                    {
                        j.Remove("_id");
                    }

                    //linq筛选出系统费非零的交易
                    var linqQuery = from tx in txJA.Children()
                                    where (decimal)tx["sys_fee"] != 0
                                    select (decimal)tx["sys_fee"];

                    decimal totalSysfee = 0;
                    //如果块内有交易有系统费，则计算块总系统费
                    if (linqQuery.Count() > 0)
                    {
                        totalSysfee = linqQuery.Sum();
                    }

                    //记录的totalSysfee为当前高度累计总系统费，总是加上截止上一个块的总系统费
                    var collBlockSysfeefind = database.GetCollection<BsonDocument>("block_sysfee");
                    var blockSysfeeFindBson = BsonDocument.Parse("{index:" + (blockindex - 1) + "}");
                    var blockSysfeeQuery = collBlockSysfeefind.Find(blockSysfeeFindBson).ToList();
                    if (blockSysfeeQuery.Count > 0)
                    {
                        totalSysfee += decimal.Parse(blockSysfeeQuery[0]["totalSysfee"].AsString);
                    }

                    //写入数据
                    var collBlockSysfee = database.GetCollection<BlockSysfee>("block_sysfee");
                    BlockSysfee bsf = new BlockSysfee(blockindex);
                    bsf.totalSysfee = totalSysfee;
                    try
                    {
                        collBlockSysfee.InsertOne(bsf);
                    }
                    catch (Exception e)
                    {
                        var a = e.Message;
                    }
                }
               
                //更新已处理块高度
                SetSystemCounter(appName, blockindex);
            }

            client = null;
        }

        private static void StorageUTXOData()
        {
            var appName = "utxo";
            var maxBlockindex = GetSystemCounter(appName);
            //检查当前已有区块是否已处理所有交易utxo
            DoStorageByEveryTxInBlock(maxBlockindex, appName);

            var storageBlockindex = maxBlockindex + 1;
            DoStorageByEveryTxInBlock(storageBlockindex, appName);
        }

        private static void StorageFulllogData()
        {
            var appName = "fulllog";
            var maxBlockindex = GetSystemCounter(appName);
            //检查当前已有区块是否已处理所有交易fulllog
            DoStorageByEveryTxInBlock(maxBlockindex, appName);

            var storageBlockindex = maxBlockindex + 1;
            DoStorageByEveryTxInBlock(storageBlockindex, appName);
        }

        private static void DoStorageByEveryTxInBlock(int blockindex,string appName)
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>("block");

            var findBson = BsonDocument.Parse("{index:" + blockindex + "}");
            var query = collection.Find(findBson).ToList();
            if (query.Count > 0)
            {
                BsonDocument queryB = query[0].AsBsonDocument;
                BsonArray Txs = queryB["tx"].AsBsonArray;

                var i = 0;
                foreach (BsonValue bv in Txs)
                {
                    DateTime start = DateTime.Now;

                    var jsonWriterSettings = new JsonWriterSettings { OutputMode = JsonOutputMode.Strict };
                    JObject Tx =JObject.Parse(bv.ToJson(jsonWriterSettings));
                    ConsoleColor cc = ConsoleColor.White;
                    bool isShow = true;
                    switch (appName)
                    {
                        case "utxo":
                            DoStorageUTXOByTx(blockindex,Tx);
                            isShow = false;
                            //cc = ConsoleColor.Yellow;
                            break;
                        case "notify":                         
                            //只有合约调用交易才处理notify，加快速度
                            try
                            {
                                if ((string)Tx["type"] == "InvocationTransaction")
                                {
                                    DoStorageNotifyByTx(blockindex, Tx);
                                    Thread.Sleep(sleepTime);
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.ForegroundColor = ConsoleColor.Cyan;
                                Console.WriteLine("notify：" + ex.Message);
                            }
                            cc = ConsoleColor.Cyan;
                            break;
                        case "NEP5":
                            //只有合约调用交易才处理NEP5，加快速度
                            try
                            {
                                if ((string)Tx["type"] == "InvocationTransaction")
                                {
                                    DoStorageNEP5ByTx(blockindex, Tx);
                                    //Thread.Sleep(sleepTime);
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.ForegroundColor = ConsoleColor.Cyan;
                                Console.WriteLine("NEP5：" + ex.Message);
                            }
                            cc = ConsoleColor.Cyan;
                            break;
                        case "fulllog":
                            DoStorageFulllogByTx(Tx);
                            cc = ConsoleColor.Magenta;
                            Thread.Sleep(sleepTime);
                            break;
                    }

                    i++;

                    if (isShow == true)
                    {
                        DateTime end = DateTime.Now;
                        var doTime = (end - start).TotalMilliseconds;
                        Console.ForegroundColor = cc;
                        Console.WriteLine("Storage_" + appName + "_Data On Block " + blockindex + " On Tx(" + i + "/" + Txs.Count + ") in " + doTime + "ms");
                    }

                    //Thread.Sleep(sleepTime);
                }

                //更新已处理块高度
                SetSystemCounter(appName, blockindex);
            }

            client = null;
        }

        private static DateTime GetBlockTime(int blockindex)
        {
            //获取block时间（本地时区时区）
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collBlock = database.GetCollection<BsonDocument>("block");
            var queryBlock = collBlock.Find("{index:" + blockindex + "}").Project("{time:1}").ToList()[0];
            int blockTimeTS = queryBlock["time"].AsInt32;
            DateTime blockTime = TimeZoneInfo.ConvertTime(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc), TimeZoneInfo.Local).AddSeconds(blockTimeTS);

            client = null;

            return blockTime;
        }
        
        private static bool checkAddrTxExist(string addr,string txid) {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);

            var collAddrTx = database.GetCollection<NEP5.Transfer>("address_tx");

            var findStr = "{addr:'" + addr + "',txid:'" + txid + "'}";

            var query = collAddrTx.Find(findStr).ToList();

            if(query.Count > 0) {
                return true;
            }
            else {
                return false;
            }
        }
        
        private static void StorageAddressInfoByNEP5transferNew()
        {
            try
            {
                int NEP5Height = GetSystemCounter("NEP5");
                int NEP5addrInfoHeight = GetSystemCounter("Nep5AddrInfo");
                int blockHeight = GetSystemCounter("block");
                if(NEP5Height > blockHeight)
                {
                    NEP5Height = blockHeight;
                }
                NEP5addrInfoHeight = (NEP5addrInfoHeight == -1 ? 0 : NEP5addrInfoHeight);
                if (NEP5Height <= NEP5addrInfoHeight) return;
                for (int st = NEP5addrInfoHeight; st <= NEP5Height; st+= batchSize)
                {
                    int ne = st + batchSize;
                    int ed = ne < NEP5Height ? ne : NEP5Height;
                    // 处理utxo.addrInfo
                    storageUtxoAddressInfo(st, ed);
                    // 处理nep5transfer.addrInfo
                    storageNep5transferAddressInfo(st, ed);

                    //更新处理高度
                    SetSystemCounter("Nep5AddrInfo", ed);

                    log.Debug(string.Format("processed height:{0}/{1}", ed, NEP5Height));
                }/*
                // 处理utxo.addrInfo
                storageUtxoAddressInfo(NEP5addrInfoHeight, NEP5Height);
                // 处理nep5transfer.addrInfo
                storageNep5transferAddressInfo(NEP5addrInfoHeight, NEP5Height);

                //更新处理高度
                SetSystemCounter("Nep5AddrInfo", NEP5Height);
                
                 log.Debug("processed height:"+ NEP5Height);
                 */
            }
            catch (Exception e)
            {
                // 异常,可继续运行
                log.Error(e.Message);
                log.Error(e.StackTrace);
            }
        }
        private static void storageUtxoAddressInfo(int addrInfoHeight, int nep5Height)
        {
            string connStr = mongodbConnStr;
            string connDb = mongodbDatabase;
            var client = connStr == null ? new MongoClient() : new MongoClient(connStr);
            var database = client.GetDatabase(connDb);
            var collection = database.GetCollection<BsonDocument>("tx");

            // 查询总量
            string findStr = new JObject() { { "blockindex", new JObject() { { "$gt", addrInfoHeight }, { "$lte", nep5Height } } } }.ToString();
            long cnt = collection.Find(BsonDocument.Parse(findStr)).Count();
            if (cnt == 0) return;

            // 批量处理
            for(int startIndex=addrInfoHeight; startIndex<nep5Height; ++startIndex)
            {
                DateTime now = GetBlockTime(startIndex);
                Console.WriteLine("start to processUtxoHeight:"+startIndex);
                findStr = new JObject() { { "blockindex", startIndex } }.ToString();
                var rr = collection.Find(BsonDocument.Parse(findStr))
                    .Project(BsonDocument.Parse(new JObject() { { "_id", 0 }, { "blockindex", 1 }, {"txid", 1 }, { "vout.address", 1 }, { "vin", 1 } }.ToString()))
                    .Sort(BsonDocument.Parse(new JObject() { { "blockindex", 1 } }.ToString()))
                    .ToList();
                if (rr == null || rr.Count() == 0) continue;

                foreach (var item in rr)
                {
                    JObject tx = JObject.Parse(item.ToString());
                    string txid = (string)tx["txid"];
                    JArray vinJA = (JArray)tx["vin"];
                    JArray voutJA = (JArray)tx["vout"];
                    // vout
                    if(voutJA.Count > 0)
                    {
                        foreach (var vout in voutJA)
                        {
                            string address = vout["address"].ToString();
                            DoStorageAddressByVoutVin(startIndex, address, txid, null, now);
                        }
                    }

                    // vin
                    if(vinJA.Count > 0)
                    {
                        foreach(JObject vinJ in vinJA)
                        {
                            string voutTx = (string)vinJ["txid"];
                            int voutN = (int)vinJ["vout"];
                            var _collTx = database.GetCollection<BsonDocument>("tx");
                            var _queryTx = _collTx.Find(BsonDocument.Parse("{txid:'" + voutTx + "'}")).Project(BsonDocument.Parse("{vout:1}")).ToList()[0];
                            var _voutBA = _queryTx["vout"].AsBsonArray;
                            string voutAddr = string.Empty;
                            foreach (BsonValue _bv in _voutBA)
                            {
                                if ((int)_bv["n"] == voutN)
                                {
                                    voutAddr = _bv["address"].AsString;
                                    DoStorageAddressByVoutVin(startIndex, voutAddr, txid, null, now);
                                    break;
                                }
                                
                            }
                        }
                    }
                }

            }
        }
        private static void storageNep5transferAddressInfo(int addrInfoHeight, int nep5Height)
        {
            string connStr = mongodbConnStr;
            string connDb = mongodbDatabase;
            var client = connStr == null ? new MongoClient() : new MongoClient(connStr);
            var database = client.GetDatabase(connDb);
            var collection = database.GetCollection<BsonDocument>("NEP5transfer");
            var document = new BsonDocument { };
            
            // 查询总量
            string findStr = new JObject() { { "blockindex", new JObject() { { "$gt", addrInfoHeight }, { "$lte", nep5Height } } } }.ToString();
            long cnt = collection.Find(BsonDocument.Parse(findStr)).Count();
            if (cnt == 0) return;
            //addrInfoHeight = 440000;
            // 批量处理
            for (int startIndex = addrInfoHeight; startIndex < nep5Height; ++startIndex)
            {
                DateTime now = GetBlockTime(startIndex);
                Console.WriteLine("start to processNep5trHeight:" + startIndex);
                findStr = new JObject() { { "blockindex", startIndex } }.ToString();
                var rr = collection.Find(BsonDocument.Parse(findStr))
                    .Project(BsonDocument.Parse(new JObject() { { "_id", 0 }, { "blockindex", 1 }, { "txid", 1 }, { "from", 1 }, { "to", 1 } }.ToString()))
                    .Sort(BsonDocument.Parse(new JObject() { { "blockindex", 1 } }.ToString()))
                    .ToList();
                if (rr == null || rr.Count() == 0) continue;

                foreach (var item in rr)
                {
                    JObject tx = JObject.Parse(item.ToString());
                    string txid = tx["txid"].ToString();
                    string from = tx["from"].ToString();
                    string to = tx["to"].ToString();
                    if(from != null && from != "")
                    {
                        DoStorageAddressByVoutVin(startIndex, from, txid, null, now);
                    }
                    if (to != null && to != "")
                    {
                        DoStorageAddressByVoutVin(startIndex, to, txid, null, now);
                    }

                }
            }

        }
        public static JObject toFilter(string[] fieldValueArr, string fieldName, string logicalOperator = "$or")
        {
            if (fieldValueArr.Count() == 1)
            {
                return new JObject() { { fieldName, fieldValueArr[0] } };
            }
            return new JObject() { { logicalOperator, new JArray() { fieldValueArr.Select(item => new JObject() { { fieldName, item } }).ToArray() } } };
        }


        /*
        private static void StorageAddressInfoByNEP5transfer()
        {
            int NEP5Height = GetSystemCounter("NEP5");
            int NEP5addrInfoHeight = GetSystemCounter("Nep5AddrInfo");

            //NEP5地址处理高度不超过NEP5信息入库高度
            if (NEP5addrInfoHeight <= NEP5Height)
            {
                var client = new MongoClient(mongodbConnStr);
                var database = client.GetDatabase(mongodbDatabase);

                var collNEP5trasfer = database.GetCollection<NEP5.Transfer>("NEP5transfer");

                var findStrNEP5trasfer = "{blockindex:{'$gt':" + NEP5addrInfoHeight + "}}";
                var sortStrNEP5transfer = "{blockindex:1}";

                List<NEP5.Transfer>  queryNEP5transfer = collNEP5trasfer.Find(findStrNEP5trasfer).Sort(sortStrNEP5transfer).Limit(1).ToList();
                if (queryNEP5transfer.Count > 0)
                {
                    int unStorageFirstHeight = queryNEP5transfer.First().blockindex;

                    findStrNEP5trasfer = "{blockindex:" + unStorageFirstHeight + "}";
                    sortStrNEP5transfer = "{'blockindex' : 1,'txid' : 1,'n' : 1}";

                    List<NEP5.Transfer> queryNEP5transferS = collNEP5trasfer.Find(findStrNEP5trasfer).Sort(sortStrNEP5transfer).ToList();

                    if (queryNEP5transferS.Count > 0)
                    {
                        foreach (NEP5.Transfer NEP5tf in queryNEP5transferS)
                        {
                            string AddrFrom = NEP5tf.from;
                            string AddrTo = NEP5tf.to;
                            string Txid = NEP5tf.txid;
                            int Blockindex = NEP5tf.blockindex;

                            //NEP5 From
                            if (NEP5tf.from != string.Empty)
                            {
                                storageAddrAndAddrtx(AddrFrom, Txid, Blockindex);
                            }
                            //NEP5 To
                            if (NEP5tf.to != string.Empty)
                            {
                                storageAddrAndAddrtx(AddrTo, Txid, Blockindex);
                            }
                        }
                    }

                    //更新处理高度
                    SetSystemCounter("Nep5AddrInfo", unStorageFirstHeight);
                }             
            }     
        }
        */

        private static void DoStorageAddressByVoutVin(int blockindex, string VoutVin_addr,string VoutVin_txid,string assetID, DateTime blockTime)
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);

            //处理address入库
            var collAddr = database.GetCollection<Address>("address");
            var findBson = BsonDocument.Parse("{addr:'" + VoutVin_addr + "'}");
            var queryAddr = collAddr.Find(findBson).ToList();
            Address addr = new Address();
            if (queryAddr.Count == 0)
            {
                //DateTime blockTime = GetBlockTime(blockindex);

                //插入结构
                addr = new Address
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
                ////增加UTXO资产余额信息
                //var abNew = new assetBalance(mongodbConnStr, mongodbDatabase, VoutVin_addr, assetID);
                //if (abNew.balance != "0")
                //{
                //    addr.balanceOfUTXO.Add(abNew);
                //}

                collAddr.InsertOne(addr);
                addAddressTx(addr);//加入地址交易表记录
            }
            else if(queryAddr.Count>0) {
                //更新结构
                addr = queryAddr[0];
                if (addr.lastuse.txid != VoutVin_txid && addr.lastuse.blockindex < blockindex) {
                    addr.lastuse = new AddrUse
                    {
                        txid = VoutVin_txid,
                        blockindex = blockindex,
                        blocktime = blockTime
                    };
                    addr.txcount++;

                    ////增加或更新UTXO资产余额信息
                    //if (addr.balanceOfUTXO.Count > 0) {
                    //    List<assetBalance> temp = new List<assetBalance>();
                    //    foreach (assetBalance ab in addr.balanceOfUTXO)//如果已有相关资产余额信息则先删除
                    //    {
                    //        if (ab.assetid != assetID)
                    //        {
                    //            temp.Add(ab);
                    //        }
                    //    }
                    //    addr.balanceOfUTXO = temp;
                    //}
                    //var abNew = new assetBalance(mongodbConnStr, mongodbDatabase, VoutVin_addr, assetID);
                    //if (abNew.balance != "0")
                    //{
                    //    addr.balanceOfUTXO.Add(abNew);
                    //}
                    
                    collAddr.ReplaceOne(findBson, addr);
                }
                addr.lastuse = new AddrUse
                {
                    txid = VoutVin_txid,
                    blockindex = blockindex,
                    blocktime = blockTime
                };
                addAddressTx(addr);//加入地址交易表记录
            }

            client = null;
        }

        private static void addAddressTx(Address addr)
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collAddrTx = database.GetCollection<BsonDocument>("address_tx");
            var findBson = BsonDocument.Parse("{addr:'" + addr.addr + "',txid:'" + addr.lastuse.txid + "'}");
            var queryAddrTx = collAddrTx.Find(findBson).ToList();

            if (queryAddrTx.Count == 0)
            {
                BsonDocument B = new BsonDocument();
                B.Add("addr", addr.addr);
                B.Add("txid", addr.lastuse.txid);
                B.Add("blockindex", addr.lastuse.blockindex);
                B.Add("blocktime", addr.lastuse.blocktime);

                collAddrTx.InsertOne(B);
            }

            client = null;
        }


        private static void DoStorageUTXOByTx(int blockindex, JObject TxJ)
        {
            string txid = (string)TxJ["txid"];
            JArray vinJA = (JArray)TxJ["vin"];
            JArray voutJA = (JArray)TxJ["vout"];

            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collUTXO = database.GetCollection<UTXO>("utxo");

            //先处理UTXO生成
            if (voutJA.Count > 0)
            {
                foreach (JObject voutJ in voutJA)
                {
                    UTXO utxo = new UTXO
                    {
                        addr = (string)voutJ["address"],
                        txid = txid,
                        n = (int)voutJ["n"],
                        asset = (string)voutJ["asset"],
                        value = (decimal)voutJ["value"],
                        createHeight = blockindex
                    };

                    //尝试入库新的asset
                    DoAssetStorageByVout((string)voutJ["asset"]);

                    //检查是否已有入库,无则入库
                    string findStr = "{{txid:'{0}',n:{1}}}";
                    findStr = string.Format(findStr, utxo.txid, utxo.n);
                    BsonDocument findB = BsonDocument.Parse(findStr);
                    List<UTXO> query = collUTXO.Find(findB).ToList();
                    if (query.Count == 0)
                    {
                        collUTXO.InsertOne(utxo);
                    }

                    //DoStorageAddressByVoutVin(blockindex, utxo.addr, txid ,utxo.asset);
                }
            }

            //处理UTXO使用
            if (vinJA.Count > 0)
            {
                foreach (JObject vinJ in vinJA)
                {
                    string voutTx = (string)vinJ["txid"];
                    int voutN = (int)vinJ["vout"];

                    //查找UTXO创建记录
                    string findStr = "{{txid:'{0}',n:{1}}}";
                    findStr = string.Format(findStr, voutTx, voutN);
                    BsonDocument findB = BsonDocument.Parse(findStr);
                    UTXO utxo = collUTXO.Find(findB).ToList()[0];
                    if (utxo != null)
                    {
                        //只有不重复才更新
                        if (utxo.used == string.Empty)
                        {
                            utxo.used = txid;
                            utxo.useHeight = blockindex;
                            collUTXO.ReplaceOne(findB, utxo);
                        }
                    }
                    /*
                    try
                    {
                        //查找前序交易vout对应的addr
                        var _collTx = database.GetCollection<BsonDocument>("tx");
                        var _queryTx = _collTx.Find(BsonDocument.Parse("{txid:'" + voutTx + "'}")).ToList()[0];
                        var _voutBA = _queryTx["vout"].AsBsonArray;
                        string voutAddr = string.Empty;
                        foreach (BsonValue _bv in _voutBA)
                        {
                            if ((int)_bv["n"] == voutN)
                            {
                                voutAddr = _bv["address"].AsString;
                                break;
                            }
                        }

                        DoStorageAddressByVoutVin(blockindex, voutAddr, txid, utxo.asset);
                    }
                    catch { }*/
                }
            }

            if (TxJ["claims"] != null) {
                //记录GAS领取
                JArray claimJA = (JArray)TxJ["claims"];
                
                if (claimJA.Count > 0)
                {
                    foreach (JObject claimJ in claimJA)
                    {
                        string voutTx = (string)claimJ["txid"];
                        int voutN = (int)claimJ["vout"];

                        //查找UTXO创建记录
                        string findStr = "{{txid:'{0}',n:{1}}}";
                        findStr = string.Format(findStr, voutTx, voutN);
                        BsonDocument findB = BsonDocument.Parse(findStr);
                        UTXO utxo = collUTXO.Find(findB).ToList()[0];
                        if (utxo != null)
                        {
                            //只有不重复才更新
                            if (utxo.claimed == string.Empty)
                            {
                                utxo.claimed = txid;
                                collUTXO.ReplaceOne(findB, utxo);
                            }
                        }
                    }
                }
            }

            client = null;   
        }

        private static void DoStorageNotifyByTx(int blockindex,JObject TxJ) {
            //获取数据库Tx数据
            string doTxid = (string)TxJ["txid"];
            string resNotify = string.Empty;
            JObject resJ = new JObject();
            
            JObject postData = new JObject();
            postData.Add("jsonrpc", "2.0");
            postData.Add("method", "getapplicationlog");
            postData.Add("params", new JArray() { doTxid });
            postData.Add("id", 1);
            string postDataStr = Newtonsoft.Json.JsonConvert.SerializeObject(postData);
            //获取Cli Notify数据
            resNotify = chh.Post(NeoCliJsonRPCUrl, postDataStr, Encoding.UTF8,1);
            resJ = new JObject();
            try
            {
                resJ = JObject.Parse(resNotify);
            }
            catch(Exception ex)
            {
                var e = ex.Message;
                //待加入异常记录
                return;
            }
            if (resJ["result"] != null)
            {
                resNotify = JObject.Parse(resNotify)["result"].ToString();
            }
            else { resNotify = null; }
            if (resNotify != null)
            {
                if (!IsDataExist("notify", "txid", doTxid))
                {
                    JObject resNotifyJ = JObject.Parse(resNotify);
                    resNotifyJ.Add("blockindex", blockindex);
                    MongoInsertOne("notify", resNotifyJ);
                }
            }
        }

        private static void DoStorageNEP5ByTx(int blockindex, JObject TxJ)
        {
            //获取数据库Tx数据
            string doTxid = (string)TxJ["txid"];

            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collNotify = database.GetCollection<BsonDocument>("notify");

            var findBson = BsonDocument.Parse("{txid:'" + doTxid + "'}");
            var query = collNotify.Find(findBson).ToList();

            if (query.Count > 0)
            {
                var jsonWriterSettings = new JsonWriterSettings { OutputMode = JsonOutputMode.Strict };
                JObject notifyJ = JObject.Parse(query[0].ToJson(jsonWriterSettings));

                string aaaa = (string)notifyJ["executions"][0]["vmstate"];

                //只有合约执行成功的才处理
                if ((string)notifyJ["executions"][0]["vmstate"] != "FAULT, BREAK")
                {
                    //测试nep5
                    if (notifyJ["executions"][0]["notifications"] != null)
                    {
                        JArray notificationsJA = (JArray)notifyJ["executions"][0]["notifications"];
                        if (notificationsJA.Count > 0)
                        {
                            int n = 0;
                            foreach (JObject notificationJ in notificationsJA)
                            {
                                if (nep5.checkTransfer(notificationJ))
                                {
                                    //获取nep5资产信息测试
                                    string nep5AssetID = (string)notificationJ["contract"];

                                    var collNEP5AssetBson = database.GetCollection<BsonDocument>("NEP5asset");
                                    var findBsonNEP5AssetBson = BsonDocument.Parse("{assetid:'" + nep5AssetID + "'}");
                                    var queryNEP5AssetBson = collNEP5AssetBson.Find(findBsonNEP5AssetBson).ToList();

                                    int NEP5decimals = 0;//NEP5资产精度，后面处理资产value有用
                                    if (queryNEP5AssetBson.Count == 0)//不重复才存
                                    {
                                        NEP5.Asset asset = new NEP5.Asset(mongodbDatabase, nep5AssetID);

                                        //try
                                        //{
                                            var collNEP5Asset = database.GetCollection<NEP5.Asset>("NEP5asset");
                                            collNEP5Asset.InsertOne(asset);
                                        /*
                                        }
                                        catch (Exception ex)
                                        {
                                            var a = ex.Message;
                                        }*/

                                        NEP5decimals = asset.decimals;
                                    }
                                    else
                                    {
                                        NEP5decimals = queryNEP5AssetBson[0]["decimals"].ToInt32();
                                    }

                                    var collNEP5TransferBson = database.GetCollection<BsonDocument>("NEP5transfer");
                                    var findBsonNEP5TransferBson = BsonDocument.Parse("{txid:'" + doTxid + "',n:" + n + "}");
                                    var queryNEP5TransferBson = collNEP5TransferBson.Find(findBsonNEP5TransferBson).ToList();

                                    if (queryNEP5TransferBson.Count == 0)//不重复才存
                                    {
                                        NEP5.Transfer tf = new NEP5.Transfer(blockindex, doTxid, n, notificationJ, NEP5decimals);

                                        var collNEP5Transfer = database.GetCollection<NEP5.Transfer>("NEP5transfer");
                                        collNEP5Transfer.InsertOne(tf);
                                    }
                                }

                                n++;
                            }

                        }
                    }
                }
            }

            client = null;
        }

        private static void DoStorageFulllogByTx(JObject TxJ)
        {
            //获取数据库Tx数据
            string doTxid = (string)TxJ["txid"];

            JObject postData = new JObject();
            postData.Add("jsonrpc", "2.0");
            postData.Add("method", "getfullloginfo");
            postData.Add("params", new JArray() { doTxid });
            postData.Add("id", 1);
            string postDataStr = Newtonsoft.Json.JsonConvert.SerializeObject(postData);
            //获取Cli FullLogs数据
            string resFulllog = chh.Post(NeoCliJsonRPCUrl, postDataStr, Encoding.UTF8, 1);
            JObject resJ = new JObject();
            try {
                resJ = JObject.Parse(resFulllog);
            }
            catch {
                //待加入异常记录
                return;
            }         
            if (resJ["result"] != null)
            {
                resFulllog = JObject.Parse(resFulllog)["result"].ToString();
            }
            else { resFulllog = null; }
            if (resFulllog != null)
            {
                if (!IsDataExist("fulllog", "txid", doTxid))
                {
                    string fulllog7z = resFulllog;
                    JObject j = new JObject
                    {
                        { "txid", doTxid },
                        { "fulllog7z", fulllog7z }
                    };
                    MongoInsertOne("fulllog", j);
                }
            }
        }

        private static void StorageNotifyData()
        {
            //适配nel改版cli
            if (cliType == "nel") {
                var maxBlockindex = GetSystemCounter("notify");
                //检查当前已有区块是否已处理所有交易notify
                DoStorageNotify(maxBlockindex);

                var storageBlockindex = maxBlockindex + 1;
                DoStorageNotify(storageBlockindex);
            }

            //适配原版cli
            if (cliType == "neo") {
                var appName = "notify";
                var maxBlockindex = GetSystemCounter(appName);
                //检查当前已有区块是否已处理所有交易utxo
                DoStorageByEveryTxInBlock(maxBlockindex, appName);

                var storageBlockindex = maxBlockindex + 1;
                DoStorageByEveryTxInBlock(storageBlockindex, appName);
            }
        }

        private static void StorageNEP5Data()
        {
            var appName = "NEP5";
            var maxBlockindex = GetSystemCounter(appName);
            //检查当前已有区块是否已处理所有交易utxo
            DoStorageByEveryTxInBlock(maxBlockindex, appName);

            var storageBlockindex = maxBlockindex + 1;
            if (storageBlockindex <= GetSystemCounter("notify"))//nep5处理高度不能超过notify高度
            {              
                DoStorageByEveryTxInBlock(storageBlockindex, appName);
            }
        }

        private static void DoStorageNotify(int doBlockIndex)
        {
            DateTime start = DateTime.Now;

            //处理的notify的块不能大于区块高度
            if (doBlockIndex <= GetSystemCounter("block"))
            {
                JObject postData = new JObject();
                postData.Add("jsonrpc", "2.0");
                postData.Add("method", "getnotifyinfo");
                postData.Add("params", new JArray() { doBlockIndex });
                postData.Add("id", 1);
                string postDataStr = Newtonsoft.Json.JsonConvert.SerializeObject(postData);
                //获取Cli Notify数据
                string resNotify = chh.Post(NeoCliJsonRPCUrl, postDataStr, Encoding.UTF8, 1);
                JObject resJ = new JObject();
                try
                {
                    resJ = JObject.Parse(resNotify);
                }
                catch
                {
                    //待加入异常记录
                    return;
                }
                resNotify = Newtonsoft.Json.JsonConvert.SerializeObject(resJ["result"]);
                //GetNeoCliData("getnotifyinfo", new object[]
                //{
                //    doBlockIndex
                //});
                //获取有效数据则存储Mongodb
                if (resNotify != "null")
                {
                    JArray txJA = JArray.Parse(resNotify);
                    long blocktime = (long)txJA[0]["time"];
                    List<JObject> listJ = new List<JObject>();
                    foreach (JToken jk in txJA)
                    {
                        var isListBexist = false;//判断是否已存在txid
                                                 //如果已有txid则添加
                        if (listJ.Count > 0)
                        {
                            foreach (JObject j in listJ)
                            {
                                if ((string)j["txid"] == (string)jk["txid"])
                                {
                                    JObject statesJ = new JObject();
                                    if ((string)jk["state"]["type"] == "Array")
                                    {
                                        statesJ = new JObject
                                    {
                                        { "contract",(string)jk["contract"]},
                                        { "type",(string)jk["state"]["type"]},
                                        { "values",(JArray)jk["state"]["value"] }
                                    };
                                    }
                                    else
                                    {
                                        statesJ = new JObject
                                    {
                                        { "contract",(string)jk["contract"]},
                                        { "type",(string)jk["state"]["type"]},
                                        { "values",(string)jk["state"]["value"] }
                                    };
                                    }

                                    JArray statesJA = (JArray)j["states"];
                                    statesJA.Add(statesJ);

                                    isListBexist = true;
                                    break;
                                }
                            }
                        }
                        //如果没有txid则创建
                        if (listJ.Count == 0 || isListBexist == false)
                        {
                            JObject j = new JObject();
                            if ((string)jk["state"]["type"] == "Array")
                            {
                                j = new JObject
                            {
                                { "txid", (string)jk["txid"] },
                                { "blocktime",blocktime},
                                { "states",new JArray{new JObject{
                                    { "contract",(string)jk["contract"]},
                                    { "type",(string)jk["state"]["type"]},
                                    { "values",(JArray)jk["state"]["value"] }
                                }
                                } }
                            };
                            }
                            else
                            {
                                j = new JObject
                            {
                                { "txid", (string)jk["txid"] },
                                { "blocktime",blocktime},
                                { "states",new JArray{new JObject{
                                    { "contract",(string)jk["contract"]},
                                    { "type",(string)jk["state"]["type"]},
                                    { "values",(string)jk["state"]["value"] }
                                }
                                } }
                            };
                            }


                            listJ.Add(j);
                        }
                    }

                    //每个txid逐一处理，存入数据库
                    foreach (JObject notifyJ in listJ)
                    {
                        if (!IsDataExist("notify", "txid", (string)notifyJ["txid"]))
                        {//判断是否重复
                            MongoInsertOne("notify", notifyJ);
                        }
                    }
                }
                //更新最新处理区块索引
                SetSystemCounter("notify", doBlockIndex);

                DateTime end = DateTime.Now;
                var doTime = (end - start).TotalMilliseconds;
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("StorageNotifyData On Block " + doBlockIndex + " in " + doTime + "ms");
            }
        }

        private static string GetNeoCliData(string method, object[] paras)
        {
            string result = string.Empty;

            JObject postData = new JObject();
            postData.Add("jsonrpc", "2.0");
            postData.Add("method", method);
            postData.Add("params", new JArray() { paras });
            postData.Add("id", 1);
            string postDataStr = Newtonsoft.Json.JsonConvert.SerializeObject(postData);

            //获取Cli Notify数据
            string resp = chh.Post(NeoCliJsonRPCUrl, postDataStr, Encoding.UTF8, 1);

            JObject resJ = new JObject();
            try
            {
                resJ = JObject.Parse(resp);
            }
            catch (Exception ex)
            {
                var e = ex.Message;
            }

            if (resJ["result"] != null)
            {
                result = resJ["result"].ToString();
            }

            return result;
        }

        private static void MongoInsertOne(string collName, JObject J,bool isAsyn = false)
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>(collName);

            var document = BsonDocument.Parse(Newtonsoft.Json.JsonConvert.SerializeObject(J));

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

        private static int GetSystemCounter(string counter)
        {
            int maxIndex = -1;
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>("system_counter");

            var queryBson = BsonDocument.Parse("{counter:'" + counter + "'}");
            var query = collection.Find(queryBson).ToList();
            if (query.Count == 0) { maxIndex = -1; }
            else
            {
                maxIndex = (int)query[0]["lastBlockindex"];
            }

            client = null;
            return maxIndex;
        }

        private static void SetSystemCounter(string counter, int lastBlockindex)
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>("system_counter");

            var setBson = BsonDocument.Parse("{counter:'" + counter + "',lastBlockindex:" + lastBlockindex + "}");

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

        private static bool IsDataExist(string coll, string key, object value)
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
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
