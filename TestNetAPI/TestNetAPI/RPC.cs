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
                var result = json["result"];
                return result;
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
                var result = json["result"];
                return result;
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
                var result = json["result"];
                return result;
            }
            catch (Exception e)
            {
                Console.WriteLine("getassetutxobyaddress:" + e.Message);
                return null;
            }
        }

        public static MyJson.IJsonNode getNep5Balancebyaddress(string url, string address, string asset)
        {
            string urldata = Tool.MakeRpcUrlPost("getassetutxobyaddress", address, asset);
            try
            {
                string str = Tool.PostData(url, urldata).Result;
                var json = MyJson.Parse(str).AsDict();
                var result = json["result"];
                return result;
            }
            catch (Exception e)
            {
                Console.WriteLine("getassetutxobyaddress:" + e.Message);
                return null;
            }
        }

    }
}
