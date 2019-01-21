using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace TestNetAPI
{
    class Tool
    {

        /// <summary>
        /// 拼装 url  psot
        /// </summary>
        /// <param name="method"></param>
        /// <param name="_params"></param>
        /// <returns></returns>
        public static string MakeRpcUrlPost(string method, params string[] _params)
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

        public static async Task<string> PostData(string url, string data)
        {
            WebClient wc = new WebClient();
            wc.Encoding = Encoding.UTF8;
            wc.Headers.Add("content-type", "text/plain;charset=UTF-8");
            byte[] postdata = System.Text.Encoding.UTF8.GetBytes(data);

            byte[] retdata = await wc.UploadDataTaskAsync(url, "POST", postdata);
            return Encoding.UTF8.GetString(retdata);
        }
    }
}
