using NetAPI.helper;
using NetAPI.RPC;
using NetAPI.services;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;

namespace NetAPI.ctr
{
    public class Api
    {
        public static Api mainApi = new Api("mainnet");


        private string netnode { get; set; }
        private transactionServer transactionServer;

        private mongoHelper mh = new mongoHelper();

        private Monitor monitor;

        public Api(string node)
        {
            initMonitor();
            netnode = node;
            switch (netnode)
            {
                case "mainnet":
                    transactionServer = new transactionServer(netnode);
                    break;
            }
        }

        public object getRes(JsonRPCrequest req, string reqAddr)
        {
            JArray result = null;
            try
            {
                point(req.method);
                switch (req.method)
                {
                    case "getassetutxobyaddress":

                        //JsonPRCresponse ress = new JsonPRCresponse()
                        //{
                        //    jsonrpc = req.jsonrpc,
                        //    id = req.id,
                        //    result = new JArray() { new JObject() { { "result", "getMsg!!" } } }
                        //};
                        //return ress;
                        result= transactionServer.getAddressAssetUtxo(req.@params[0].ToString(), req.@params[1].ToString());
                        break;
                    case "gettransactionlist":
                        if (req.@params.Length < 2)
                        {
                            result = transactionServer.gettransactionlist();
                        }
                        else if (req.@params.Length < 3)
                        {
                            result = transactionServer.gettransactionlist(int.Parse(req.@params[0].ToString()), int.Parse(req.@params[1].ToString()));
                        }
                        else
                        {
                            result = transactionServer.gettransactionlist(int.Parse(req.@params[0].ToString()), int.Parse(req.@params[1].ToString()), req.@params[2].ToString());
                        }
                        break;
                    case "sendrawtransaction":
                        result = transactionServer.sendrawtransaction((string)req.@params[0]);
                        break;

                    case "getnep5balancebyaddress":
                        result = transactionServer.getAddressNep5Asset(req.@params[0].ToString(), req.@params[1].ToString());
                        break;
                    case "getnep5decimals":
                        result = transactionServer.getnep5decimals(req.@params[0].ToString());
                        break;
                }
                if (result.Count == 0)
                {
                    JsonPRCresponse_Error resE = new JsonPRCresponse_Error(req.id, -1, "No Data", "Data does not exist");
                    return resE;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("errMsg:{0},errStack:{1}", e.Message, e.StackTrace);
                JsonPRCresponse_Error resE = new JsonPRCresponse_Error(req.id, -100, "Parameter Error", e.Message);
                return resE;
            }

            JsonPRCresponse res = new JsonPRCresponse()
            {
                jsonrpc = req.jsonrpc,
                id = req.id,
                result = result
            };
            return res;
        }

        private void initMonitor()
        {
            if (Config.startMonitorFlag)
            {
                monitor = new Monitor();
            }
        }
        private void point(string method)
        {
            if (monitor != null)
            {
                monitor.point(netnode, method);
            }
        }
    }
}
