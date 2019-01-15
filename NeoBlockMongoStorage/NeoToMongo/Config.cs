using System;
using System.IO;

namespace NeoToMongo
{
    class Config
    {
        public static string mongodbConnStr = string.Empty;
        public static string mongodbDatabase = string.Empty;
        public static string NeoCliJsonRPCUrl = string.Empty;
        public static int sleepTime = 0;
        public static bool beUtxoSleep = false;
        /// <summary>
        /// 加载配置
        /// </summary>
        public static void loadFromPath(string path)
        {
            string result = File.ReadAllText(path);
            MyJson.JsonNode_Object config = MyJson.Parse(result) as MyJson.JsonNode_Object;
            mongodbConnStr = config.AsDict().GetDictItem("mongodbConnStr").AsString();
            mongodbDatabase = config.AsDict().GetDictItem("mongodbDatabase").AsString();
            NeoCliJsonRPCUrl = config.AsDict().GetDictItem("NeoCliJsonRPCUrl").AsString();
            sleepTime = config.AsDict().GetDictItem("sleepTime").AsInt();
        }

    }
}
