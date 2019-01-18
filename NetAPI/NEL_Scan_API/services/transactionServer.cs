using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NetAPI.helper;
using System.IO;
using mongoHelper = NetAPI.helper.mongoHelper;
using NetAPI.mongo;
using MongoDB.Bson;
using System.Numerics;

namespace NetAPI.services
{
    public class transactionServer
    {
        public string Block_mongodbConnStr { get; set; }
        public string Block_mongodbDatabase { get; set; }
        public string neoCliJsonRPCUrl;
        public transactionServer(string net)
        {
            if(net== "mainnet")
            {
                this.Block_mongodbConnStr = Config.mongodbConnStr;
                this.Block_mongodbDatabase = Config.mongodbDatabase;
                this.neoCliJsonRPCUrl = Config.NeoCliJsonRPCUrl;
            }
        }
        public mongoHelper mh = new mongoHelper();


        public JArray getutxolistbyaddress(string address, int pageNum = 1, int pageSize = 10)
        {

            string findStr = new JObject() { { "addr", address } }.ToString();
            long count = mh.GetDataCount(Block_mongodbConnStr, Block_mongodbDatabase, "utxo", findStr);

            string fieldStr = MongoFieldHelper.toReturn(new string[] { "asset", "txid", "value" }).ToString();
            string sortStr = new JObject() { { "createHeight", -1 } }.ToString();
            JArray query = mh.GetDataPagesWithField(Block_mongodbConnStr, Block_mongodbDatabase, "utxo", fieldStr, pageSize, pageNum, sortStr, findStr);

            // assetId --> assetName
            if (query != null && query.Count > 0)
            {
                string[] assetIds = query.Select(p => p["asset"].ToString()).Distinct().ToArray();
                query = formatAssetNameByIds(query, assetIds);
            }

            return new JArray
            {
                new JObject(){{"count", count }, { "list", query}}
            };
        }
        public JArray gettransactionlist(int pageNum = 1, int pageSize = 10, string type = "")
        {
            string findStr = "{}";
            bool addType = type != "" && type != null && type != "all";
            if (addType)
            {
                findStr = new JObject() { { "type", type } }.ToString();
            }
            long count = mh.GetDataCount(Block_mongodbConnStr, Block_mongodbDatabase, "tx", findStr);
            string fieldStr = MongoFieldHelper.toReturn(new string[] { "type", "txid", "blockindex", "size" }).ToString();
            string sortStr = new JObject() { { "blockindex", -1 } }.ToString();
            JArray query = mh.GetDataPagesWithField(Block_mongodbConnStr, Block_mongodbDatabase, "tx", fieldStr, pageSize, pageNum, sortStr, findStr);

            return new JArray
            {
                new JObject(){{"count", count }, { "list", query}}
            };
        }

        public JArray sendrawtransaction(string txSigned)
        {
            //httpHelper hh = new httpHelper();
            var resp = httpHelper.Post(neoCliJsonRPCUrl, "{'jsonrpc':'2.0','method':'sendrawtransaction','params':['" + txSigned + "'],'id':1}", System.Text.Encoding.UTF8, 1);

            JObject Jresult = new JObject();
            bool isSendSuccess = false;
            var res = JObject.Parse(resp);
            if (res["error"] != null && res["error"]["message"] != null)
            {
                isSendSuccess = false;
                Jresult.Add("errorMessage", res["error"]["message"]);
            }
            else
            {
                isSendSuccess = (bool)JObject.Parse(resp)["result"];
            }
            //bool isSendSuccess = (bool)JObject.Parse(resp)["result"];
            //JObject Jresult = new JObject();
            Jresult.Add("sendrawtransactionresult", isSendSuccess);
            if (isSendSuccess)
            {
                ThinNeo.Transaction lastTran = new ThinNeo.Transaction();
                lastTran.Deserialize(new MemoryStream(txSigned.HexString2Bytes()));
                string txid = lastTran.GetHash().ToString();

                ////从已签名交易体分析出未签名交易体，并做Hash获得txid
                //byte[] txUnsigned = txSigned.Split("014140")[0].HexString2Bytes();
                //string txid = ThinNeo.Helper.Sha256(ThinNeo.Helper.Sha256(txUnsigned)).Reverse().ToArray().ToHexString();

                Jresult.Add("txid", txid);
            }
            else
            {
                //上链失败则返回空txid
                Jresult.Add("txid", string.Empty);
            }

            return new JArray{Jresult};
        }

        //private long getBlockTime(long index)
        //{
        //    string findStr = new JObject() { { "index", index } }.ToString();
        //    string fieldStr = new JObject() { { "time", 1 } }.ToString();
        //    var query = mh.GetDataWithField(Block_mongodbConnStr, Block_mongodbDatabase, "block", fieldStr, findStr);
        //    return long.Parse(query[0]["time"].ToString());
        //}

        private Dictionary<string, string> getAssetName(string[] assetIds)
        {
            string findStr = MongoFieldHelper.toFilter(assetIds.Distinct().ToArray(), "id").ToString();
            string fieldStr = new JObject() { { "name.name", 1 }, { "id", 1 } }.ToString();
            var query = mh.GetDataWithField(Block_mongodbConnStr, Block_mongodbDatabase, "asset", fieldStr, findStr);
            var nameDict =
                query.ToDictionary(k => k["id"].ToString(), v =>
                {
                    string id = v["id"].ToString();
                    if (id == AssetConst.id_neo)
                    {
                        return AssetConst.id_neo_nick;
                    }
                    if (id == AssetConst.id_gas)
                    {
                        return AssetConst.id_gas_nick;
                    }
                    string name = v["name"][0]["name"].ToString();
                    return name;
                });
            return nameDict;
        }

        //private JObject[] formatAssetName(JObject[] query, Dictionary<string, string> nameDict)
        //{
        //    return query.Select(p =>
        //    {
        //        JObject jo = p;
        //        string id = jo["asset"].ToString();
        //        string idName = nameDict.GetValueOrDefault(id);
        //        jo.Remove("asset");
        //        jo.Add("asset", idName);
        //        return jo;
        //    }).ToArray();
        //}

        private JArray formatAssetNameByIds(JArray query, string[] assetIds)
        {
            var nameDict = getAssetName(assetIds);
            return new JArray
                {
                    query.Select(p =>
                    {
                        JObject jo = (JObject)p;
                        string id = jo["asset"].ToString();
                        string idName = nameDict.GetValueOrDefault(id);
                        jo.Remove("asset");
                        jo.Add("asset", idName);
                        return jo;
                    }).ToArray()
                };
        }

        private JArray getAddressUtxoAsset(string address,List<string> assetId=null)
        {

            JArray utxoArr;
            if(assetId==null)
            {
                string findStr = "{{addr:'{0}',used:{1}}}";
                findStr = string.Format(findStr, address, "","{"+assetId.ToString()+"}");
                utxoArr = mh.GetData(Block_mongodbConnStr, Block_mongodbDatabase, "utxo", findStr);
            }
            else
            {
                string findStr = "{{addr:'{0}',used:{1},asset:'{2}'}}";
                findStr = string.Format(findStr, address,"");
                utxoArr = mh.GetData(Block_mongodbConnStr, Block_mongodbDatabase, "utxo", findStr);
            }

            Dictionary<string, List<JObject>> assetDic = new Dictionary<string, List<JObject>>();
            foreach (JObject utxoitem in utxoArr)
            {
                string assetid = utxoitem["asset"].ToString();
                if (assetDic.ContainsKey(assetid))
                {
                    assetDic[assetid].Add(utxoitem);
                }
                else
                {
                    assetDic[assetid] = new List<JObject>();
                }
            }
            JArray result = new JArray();
            foreach(var item in assetDic)
            {
                JObject assetarr = new JObject();
                result.Add(assetarr);
                JArray utxarr = new JArray();
                assetarr.Add(item.Key, utxarr);
                foreach(var utxo in item.Value)
                {
                    utxarr.Add(utxo);
                }
            }
            return result;
        }

        private decimal getAddressNep5Asset(string address, string nep5Hash)
        {
            string script = null;
            using (var sb = new ThinNeo.ScriptBuilder())
            {
                ThinNeo.Hash160 shash = new ThinNeo.Hash160(nep5Hash);

                MyJson.JsonNode_Array JAParams = new MyJson.JsonNode_Array();
                JAParams.Add(new MyJson.JsonNode_ValueString("(address)" + address));
                sb.EmitParamJson(JAParams);
                sb.EmitParamJson(new MyJson.JsonNode_ValueString("(str)balanceOf"));
                sb.EmitAppCall(shash);
                var data = sb.ToArray();
                script = ThinNeo.Helper.Bytes2HexString(data);

            }
            var res = Rpc.invokescript(Config.NeoCliJsonRPCUrl, script).Result;
            var arr=res.GetDictItem("stack").AsList().ToArray();
            var valueString = arr[0].AsDict()["value"].AsString();

            //-----------精度
            string findStr = "{{assetid:'{0}'}}";
            findStr = string.Format(findStr, nep5Hash);
            JArray utxoArr=mh.GetData(Block_mongodbConnStr, Block_mongodbDatabase, "NEP5asset", findStr);
            int decimals=(int)((JObject)utxoArr[0])["decimals"];


            decimal value = decimal.Parse(Number.getNumStrFromHexStr(valueString, decimals));

            return value;
        }




    }
}

