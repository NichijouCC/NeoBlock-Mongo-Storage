using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace NeoToMongo
{
    class Rpc
    {
        async public static Task<int> getblockcount(string url)
        {
            var gstr =makeRpcUrlGet(url, "getblockcount");
            try
            {
                var str = await downLoadString(gstr);
                var json = MyJson.Parse(str);

                int height = json.AsDict().GetDictItem("result").AsInt();
                return height;
            }
            catch (Exception err)
            {
                Console.WriteLine("Fail to get block count. **************" + err.ToString());
                return -1;
            }
        }

        async public static Task<MyJson.JsonNode_Object> getblock(string url,int blockIndex)
        {
            var gstr = makeRpcUrlGet(url, "getblock", blockIndex.ToString(), "1");
            try
            {
                var str = await downLoadString(gstr);
                var json = MyJson.Parse(str);
                var result = json.AsDict().GetDictItem("result") as MyJson.JsonNode_Object;
                return result;
            }
            catch (Exception err)
            {
                Console.WriteLine("failed to load block data.Info:" + err.ToString());
                return null;
            }
        }

        async public static Task<MyJson.JsonNode_Object> getassetstate(string url,string assetid)
        {
            var gstr = makeRpcUrlGet(url, "getassetstate",assetid);
            try
            {
                var str = await downLoadString(gstr);
                var json = MyJson.Parse(str);
                var result = json.AsDict().GetDictItem("result") as MyJson.JsonNode_Object;
                return result;
            }
            catch (Exception err)
            {
                Console.WriteLine("failed to get asset state.Info:" + err.ToString());
                return null;
            }
        }


        /// <summary>
        /// 拼装 url  get
        /// </summary>
        /// <param name="method"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        static string makeRpcUrlGet(string url, string method, params string[] param)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(url + "?jsonrpc=2.0&id=1&method=" + method + "&params=[");
            for (int i = 0; i < param.Length; i++)
            {
                if (i != param.Length - 1)
                {
                    sb.Append(param[i] + ",");
                }
                else
                {
                    sb.Append(param[i]);
                }
            }
            sb.Append("]");
            return sb.ToString();
        }

        /// <summary>
        /// 加载数据
        /// </summary>
        /// <param name="url"></param>
        /// <returns></returns>
        async public static Task<string> downLoadString(string url)
        {
            var wc = new WebClient();
            var str = await wc.DownloadStringTaskAsync(url);
            return str;
        }
    }
}
