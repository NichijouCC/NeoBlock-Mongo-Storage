using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace NetAPI
{
    class Rpc
    {
        async public static Task<int> getblockcount(string url)
        {
            var gstr =makeRpcUrlGet(url, "getblockcount");
            try
            {
                var str = await getData(gstr);
                var json = MyJson.Parse(str);

                int height = json.AsDict().GetDictItem("result").AsInt();
                return height;
            }
            catch (Exception err)
            {
                var errorMsg = "Fail to get block count.Info:" + err.ToString();
                Console.WriteLine(errorMsg);
                return -1;
            }
        }

        async public static Task<MyJson.JsonNode_Object> getblock(string url,int blockIndex)
        {
            var gstr = makeRpcUrlGet(url, "getblock", blockIndex.ToString(), "1");
            try
            {
                var str = await getData(gstr);
                var json = MyJson.Parse(str);
                var result = json.AsDict().GetDictItem("result") as MyJson.JsonNode_Object;
                return result;
            }
            catch (Exception err)
            {
                var errorMsg = "failed to get block. param(blockIndex):."+blockIndex + "Info:" + err.ToString();
                Console.WriteLine(errorMsg);
                return null;
            }
        }

        async public static Task<MyJson.JsonNode_Object> getassetstate(string url,string assetid)
        {
            var gstr = MakeRpcUrlPost("getassetstate",assetid);
            try
            {
                var str = await postData(url,gstr);
                var json = MyJson.Parse(str);
                var result = json.AsDict().GetDictItem("result") as MyJson.JsonNode_Object;
                return result;
            }
            catch (Exception err)
            {
                var errorMsg = "failed to get asset state,param(assetid):"+assetid+".Info:" + err.ToString();
                Console.WriteLine(errorMsg);
                return null;
            }
        }

        async public static Task<MyJson.JsonNode_Object> getapplicationlog(string url,string txid)
        {
            var gstr = MakeRpcUrlPost("getapplicationlog",txid);
            string str="";
            try
            {
                str = await postData(url,gstr);
                var json = MyJson.Parse(str) as MyJson.JsonNode_Object;
                var result = json.AsDict().GetDictItem("result") as MyJson.JsonNode_Object;
                return result;
            }
            catch (Exception err)
            {
                var errorMsg = "failed to get application log." + "param(txid):" + txid + "resStr:"+ str;
                Console.WriteLine(errorMsg);
                return null;
            }
        }

        async public static Task<MyJson.JsonNode_Object> getfullloginfo(string url,string txid)
        {
            var gstr = MakeRpcUrlPost("getfullloginfo", txid);
            try
            {
                var str = await postData(url, gstr);
                var json = MyJson.Parse(str);
                var result = json.AsDict().GetDictItem("result") as MyJson.JsonNode_Object;
                return result;
            }
            catch (Exception err)
            {
                var errorMsg = "failed to get full log info.param(txid):"+txid+".Info:" + err.ToString();
                Console.WriteLine(errorMsg);
                return null;
            }
        }

        //async public static Task<MyJson.JsonNode_Object> callcontractfortest(string url,string method)
        //{
        //    var gstr = MakeRpcUrlPost("getfullloginfo", txid);
        //    try
        //    {
        //        var str = await postData(url, gstr);
        //        var json = MyJson.Parse(str);
        //        var result = json.AsDict().GetDictItem("result") as MyJson.JsonNode_Object;
        //        return result;
        //    }
        //    catch (Exception err)
        //    {
        //        var errorMsg = "failed to call contract for test.Info:" + err.ToString();
        //        Console.WriteLine(errorMsg);
        //        Log.WriteLog(errorMsg);
        //        return null;
        //    }
        //}
        async public static Task<MyJson.JsonNode_Object> invokescript(string url,string scripthash,string method)
        {
            var sb = new ThinNeo.ScriptBuilder();
            ThinNeo.Hash160 shash = new ThinNeo.Hash160(scripthash);
            sb.EmitParamJson(new MyJson.JsonNode_Array());
            sb.EmitParamJson(new MyJson.JsonNode_ValueString("(str)"+method));//调用的方法
            sb.EmitAppCall(shash);//nep5脚本

            var data = sb.ToArray();
            var script = ThinNeo.Helper.Bytes2HexString(data);

            var urldata = MakeRpcUrlPost("invokescript", script);
            try
            {
                var str = await postData(url, urldata);
                var json = MyJson.Parse(str);
                var result = json.AsDict().GetDictItem("result") as MyJson.JsonNode_Object;
                return result;
            }
            catch (Exception err)
            {
                var errorMsg = "failed to invokescript method:"+method+".Info:" + err.ToString();
                Console.WriteLine(errorMsg);
                return null;
            }
        }

        async public static Task<MyJson.JsonNode_Object> invokescript(string url, string script)
        {
            var urldata = MakeRpcUrlPost("invokescript", script);
            //var res = await this.PostData(urldata);
            try
            {
                var str = await postData(url, urldata);
                var json = MyJson.Parse(str);
                var result = json.AsDict().GetDictItem("result") as MyJson.JsonNode_Object;
                return result;
            }
            catch (Exception err)
            {
                var errorMsg = "failed to invokescript"+".Info:" + err.ToString();
                Console.WriteLine(errorMsg);
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

        static string MakeRpcUrlPost(string method, params string[] _params)
        {
            var json = new MyJson.JsonNode_Object();
            json.SetDictValue("id", 1);
            json.SetDictValue("jsonrpc", "2.0");
            json.SetDictValue("method", method);

            var array = new MyJson.JsonNode_Array();
            for (var i = 0; i < _params.Length; i++)
            {
                array.Add(new MyJson.JsonNode_ValueString(_params[i]));
            }
            json.SetDictValue("params", array);
            string urldata = json.ToString();
            return urldata;
        }

        /// <summary>
        /// 加载数据
        /// </summary>
        /// <param name="url"></param>
        /// <returns></returns>
        async static Task<string> getData(string url)
        {
            //var wc = new WebClient();
            //var str = await wc.DownloadStringTaskAsync(url);
            var str = await httpGetData(url);
            return str;
        }

        async static Task<string> postData(string url,string postData)
        {
            //byte[] postdata = System.Text.Encoding.UTF8.GetBytes(postData);

            //WebClient wc = new WebClient();
            //wc.Encoding = Encoding.UTF8;
            //wc.Headers.Add("content-type", "text/plain;charset=UTF-8");
            //byte[] retdata = await wc.UploadDataTaskAsync(url, "POST", postdata);
            //return System.Text.Encoding.UTF8.GetString(retdata);

            string str = await httpPostData(url,postData);
            return str;
        }


        public static HttpClient wc = new HttpClient();
        async static Task<string> httpPostData(string url, string data)
        {
            //HttpClient wc = new HttpClient();
            HttpResponseMessage httpResponseMessage =await wc.PostAsync(url, new StringContent(data));
            string json =await httpResponseMessage.Content.ReadAsStringAsync();
            return json;
        }
        async static Task<string> httpGetData(string url)
        {
            
            var request = new HttpRequestMessage()
            {
                RequestUri = new Uri(url),
                Method = HttpMethod.Get,
            };
            HttpResponseMessage httpResponseMessage = await wc.SendAsync(request);
            string json = await httpResponseMessage.Content.ReadAsStringAsync();
            return json;
        }
    }
}
