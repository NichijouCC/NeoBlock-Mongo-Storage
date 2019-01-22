using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace TestNetAPI
{
    class RPC
    {
        public static MyJson.IJsonNode sendrawtransaction(string url, string rawdata)
        {
            string urldata = Tool.MakeRpcUrlPost("sendrawtransaction", rawdata);

            try
            {
                string str = Tool.PostData(url, urldata).Result;
                var json = MyJson.Parse(str).AsDict();
                if (json.ContainsKey("error"))
                {
                    Console.WriteLine("getassetutxobyaddress:" + json["error"].ToString());
                    return null;
                }
                else
                {
                    var result = json["result"];
                    return result;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("sendrawtransaction:" + e.Message);
                return null;
            }
        }


        public  static MyJson.IJsonNode getassetutxobyaddress(string url, string address, string asset)
        {
            string urldata = Tool.MakeRpcUrlPost("getassetutxobyaddress", address, asset);
            try
            {
                string str = Tool.PostData(url, urldata).Result;
                var json = MyJson.Parse(str).AsDict();
                if(json.ContainsKey("error"))
                {
                    Console.WriteLine("getassetutxobyaddress:" +json["error"].ToString());
                    return null;
                }else
                {
                    var result = json["result"];
                    return result;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("getassetutxobyaddress:" + e.Message);
                return null;
            }
        }

        public static MyJson.IJsonNode invokescript(string url, string script)
        {
            var urldata = Tool.MakeRpcUrlPost("invokescript", script);
            try
            {
                string str = Tool.PostData(url, urldata).Result;
                var json = MyJson.Parse(str).AsDict();
                if (json.ContainsKey("error"))
                {
                    Console.WriteLine("getassetutxobyaddress:" + json["error"].ToString());
                    return null;
                }
                else
                {
                    var result = json["result"];
                    return result;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("getassetutxobyaddress:" + e.Message);
                return null;
            }
        }

        /// <summary>
        /// 已考虑 精度
        /// </summary>
        /// <param name="url"></param>
        /// <param name="address"></param>
        /// <param name="asset"></param>
        /// <returns></returns>
        public static MyJson.IJsonNode getnep5balancebyaddress(string url,string address,string asset)
        {
            string urldata = Tool.MakeRpcUrlPost("getnep5balancebyaddress", address, asset);
            try
            {
                string str = Tool.PostData(url, urldata).Result;
                var json = MyJson.Parse(str).AsDict();
                if (json.ContainsKey("error"))
                {
                    Console.WriteLine("getassetutxobyaddress:" + json["error"].ToString());
                    return null;
                }
                else
                {
                    var result = json["result"];
                    return result;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("getassetutxobyaddress:" + e.Message);
                return null;
            }
        }

    }
}
