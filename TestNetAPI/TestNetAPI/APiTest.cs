using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace TestNetAPI
{
    class Asset
    {
        public static string id_GAS = "0x602c79718b16e442de58778e148d0b1084e3b2dffd5de6b7b16cee7969282de7";
        public static string id_neo = "0xc56f33fc6ecfcd0c225c4ab356fee59390af8560be0e930faebe74a6daff7c9b";
        public static string id_pet = "0x6112d5ec36d299a6a8c87ebde6f3782f7ac74118";
    }

    class Account
    {
        public static string address = "AcjVGYytBysSdQTLZXpLarvVVYYNUiiUgG";
        public static string wif = "KwUhZzS6wrdsF4DjVKt2XQd3QJoidDhckzHfZJdQ3gzUUJSr8MDd";
        public static string targetAddress = "AH2ADnKSuJrhHefqeJ9j83HcNXPfipwr6V";
    }

    public class UTXO
    {
        public UTXO()
        {
            addr = string.Empty;
            txid = string.Empty;
            n = -1;
            asset = string.Empty;
            value = 0;
            createHeight = -1;
            used = string.Empty;
            useHeight = -1;
            claimed = string.Empty;
        }

        public string addr { get; set; }
        public string txid { get; set; }
        public int n { get; set; }
        public string asset { get; set; }
        public decimal value { get; set; }
        public int createHeight { get; set; }
        public string used { get; set; }
        public int useHeight { get; set; }
        public string claimed { get; set; }
    }


    class APiTest
    {
        static string apiurl = "http://192.144.165.72:7878/api/mainnet";

        /// <summary>
        /// 测试gas交易
        /// </summary>
        /// <param name="count"></param>
        public static void test_UtxoTransaction(int count)
        {
            byte[] prikey = ThinNeo.Helper.GetPrivateKeyFromWIF(Account.wif);
            byte[] pubkey = ThinNeo.Helper.GetPublicKeyFromPrivateKey(prikey);
            string address = ThinNeo.Helper.GetAddressFromPublicKey(pubkey);
            //---------获得资产
            var assetDic = getAssetUtxo(address,Asset.id_GAS);

            //拼装交易体
            string[] targetAddrs = new string[1] { Account.targetAddress };
            ThinNeo.Transaction tran = makeTran(assetDic, targetAddrs, new ThinNeo.Hash256(Asset.id_GAS), (decimal)count);
            tran.version = 0;
            tran.type = ThinNeo.TransactionType.ContractTransaction;

            //消息
            byte[] msg = tran.GetMessage();//tran是交易，getmessage是得到未签名交易的二进制数据块
                                           //string msgstr = ThinNeo.Helper.Bytes2HexString(msg);//??

            byte[] signdata = ThinNeo.Helper.Sign(msg, prikey);//私钥签名消息

            tran.AddWitness(signdata, pubkey, address);//添加普通账户鉴证人，私钥公钥地址，全都是一个人
            //string txid = tran.GetHash().ToString();//??
            byte[] data = tran.GetRawData();//得到签名交易的二进制数据块
            string rawdata = ThinNeo.Helper.Bytes2HexString(data);


            //rawdata = "80000001195876cb34364dc38b730077156c6bc3a7fc570044a66fbfeeea56f71327e8ab0000029b7cffdaa674beae0f930ebe6085af9093e5fe56b34a5c220ccdcf6efc336fc500c65eaf440000000f9a23e06f74cf86b8827a9108ec2e0f89ad956c9b7cffdaa674beae0f930ebe6085af9093e5fe56b34a5c220ccdcf6efc336fc50092e14b5e00000030aab52ad93f6ce17ca07fa88fc191828c58cb71014140915467ecd359684b2dc358024ca750609591aa731a0b309c7fb3cab5cd0836ad3992aa0a24da431f43b68883ea5651d548feb6bd3c8e16376e6e426f91f84c58232103322f35c7819267e721335948d385fae5be66e7ba8c748ac15467dcca0693692dac";
            //var testulr="http://api.allpet66.com/api/testnet";
            var testulr = "http://localhost:54918/api/mainnet";
            var result = RPC.sendrawtransaction(apiurl,rawdata);
            Console.WriteLine(result.ToString());
        }
        /// <summary>
        /// 测试NEP5交易
        /// </summary>
        /// <param name="count"></param>
        public static void test_PetTransaction(int count)
        {
            string id_GAS = "0x602c79718b16e442de58778e148d0b1084e3b2dffd5de6b7b16cee7969282de7";
            string tokenScript = Asset.id_pet;

            //-----------------交易账户
            string wif1 = Account.wif;//地址 AU5kNBWTYepzfS76DBwGKW3E3aRuFjhmAc
            byte[] prikey = ThinNeo.Helper.GetPrivateKeyFromWIF(wif1);
            byte[] pubkey = ThinNeo.Helper.GetPublicKeyFromPrivateKey(prikey);
            string address = ThinNeo.Helper.GetAddressFromPublicKey(pubkey);

            string toaddr = "AXdWU5vYe3Ja9n778RpgJrrCUjAsfQgT1r";

            string targeraddr = address;  //Transfer it to yourself. 
            //-----------------获取地址的资产列表
            Dictionary<string, List<UTXO>> assets = getAssetUtxo(address, Asset.id_GAS);

            //--------------拼装交易
            ThinNeo.Transaction tran =makeTran(assets, new string[1] { targeraddr }, new ThinNeo.Hash256(id_GAS), decimal.Zero);
            tran.type = ThinNeo.TransactionType.InvocationTransaction;

            ThinNeo.ScriptBuilder sb = new ThinNeo.ScriptBuilder();
            var scriptaddress = new ThinNeo.Hash160(tokenScript);
            //Parameter inversion 
            MyJson.JsonNode_Array JAParams = new MyJson.JsonNode_Array();
            JAParams.Add(new MyJson.JsonNode_ValueString("(address)" + address));
            JAParams.Add(new MyJson.JsonNode_ValueString("(address)" + toaddr));
            JAParams.Add(new MyJson.JsonNode_ValueString("(integer)" + count));
            sb.EmitParamJson(JAParams);//Parameter list 
            sb.EmitPushString("transfer");//Method
            sb.EmitAppCall(scriptaddress);  //Asset contract 

            ThinNeo.InvokeTransData extdata = new ThinNeo.InvokeTransData();
            extdata.script = sb.ToArray();
            extdata.gas = 1;
            tran.extdata = extdata;

            byte[] msg = tran.GetMessage();
            byte[] signdata = ThinNeo.Helper.Sign(msg, prikey);
            tran.AddWitness(signdata, pubkey, address);
            string txid = tran.GetHash().ToString();
            byte[] data = tran.GetRawData();
            string scripthash = ThinNeo.Helper.Bytes2HexString(data);

            var result = RPC.sendrawtransaction(apiurl, scripthash);

            Console.WriteLine(result.ToString());
        }

        public static void test_quryNep5Balance()
        {
            string tokenScript = Asset.id_pet;

            string wif1 = Account.wif;//地址 AU5kNBWTYepzfS76DBwGKW3E3aRuFjhmAc
            byte[] prikey = ThinNeo.Helper.GetPrivateKeyFromWIF(wif1);
            byte[] pubkey = ThinNeo.Helper.GetPublicKeyFromPrivateKey(prikey);
            string fromAddress = ThinNeo.Helper.GetAddressFromPublicKey(pubkey);

            string toAddr = "AXdWU5vYe3Ja9n778RpgJrrCUjAsfQgT1r";

            var frombalance=RPC.getnep5balancebyaddress(apiurl,fromAddress,tokenScript);
            var tobalance = RPC.getnep5balancebyaddress(apiurl, toAddr, tokenScript);

            if(frombalance!=null)
            {
                Console.WriteLine(frombalance.ToString());
            }
            if(tobalance!=null)
            {
                Console.WriteLine(tobalance.ToString());
            }
        }

        /// <summary>
        /// 查询账户neo、gas的utxo
        /// </summary>
        /// <param name="address"></param>
        /// <param name="asset"></param>
        /// <returns></returns>
        public static Dictionary<string, List<UTXO>> getAssetUtxo(string address, string asset)
        {
            //----------发请求
            MyJson.JsonNode_Array assetlist = RPC.getassetutxobyaddress(apiurl,address,asset) as MyJson.JsonNode_Array;

            Dictionary<string, List<UTXO>> assetDic = new Dictionary<string, List<UTXO>>();
            foreach (MyJson.JsonNode_Object item in assetlist)
            {
                var assetId = item["asset"].AsString();
                var assetArr = item["arr"].AsList();
                foreach (MyJson.JsonNode_Object utxo in assetArr)
                {
                    var _utxo = new UTXO()
                    {
                        addr = utxo["addr"].AsString(),
                        txid = utxo["txid"].AsString(),
                        n = utxo["n"].AsInt(),
                        asset = utxo["asset"].AsString(),
                        value = decimal.Parse(utxo["value"].AsString()),
                        createHeight = utxo["createHeight"].AsInt(),
                        used = utxo["used"].AsString(),
                        useHeight = utxo["useHeight"].AsInt(),
                        claimed = utxo["claimed"].AsString()
                    };
                    if (!assetDic.ContainsKey(assetId))
                    {
                        assetDic[assetId] = new List<UTXO>();
                    }
                    assetDic[assetId].Add(_utxo);
                }
            }
            return assetDic;
        }


        //拼交易体
        public static ThinNeo.Transaction makeTran(Dictionary<string, List<UTXO>> assets, string[] targetaddrs, ThinNeo.Hash256 assetid, decimal sendcount)
        {
            if (!assets.ContainsKey(assetid.ToString()))
                throw new Exception("no enough money.");

            List<UTXO> utxos = assets[assetid.ToString()];
            utxos.Sort((a, b) =>
            {
                if (a.value > b.value)
                    return 1;
                else if (a.value < b.value)
                    return -1;
                else
                    return 0;
            });
            var tran = new ThinNeo.Transaction();
            tran.type = ThinNeo.TransactionType.ContractTransaction;
            tran.version = 0;//0 or 1
            tran.extdata = null;

            tran.attributes = new ThinNeo.Attribute[0];
            decimal count = decimal.Zero;
            string scraddr = "";
            List<ThinNeo.TransactionInput> list_inputs = new List<ThinNeo.TransactionInput>();
            for (var i = 0; i < utxos.Count; i++)
            {
                ThinNeo.TransactionInput input = new ThinNeo.TransactionInput();
                input.hash = new ThinNeo.Hash256(utxos[i].txid);
                input.index = (ushort)utxos[i].n;
                list_inputs.Add(input);
                count += utxos[i].value;
                scraddr = utxos[i].addr;
                if (count >= (sendcount))
                {
                    break;
                }
            }

            tran.inputs = list_inputs.ToArray();

            if (count >= sendcount)//输入大于等于输出
            {
                List<ThinNeo.TransactionOutput> list_outputs = new List<ThinNeo.TransactionOutput>();
                //输出
                if (sendcount > decimal.Zero && targetaddrs != null && targetaddrs.Length > 0)
                {
                    foreach (string targetaddr in targetaddrs)
                    {
                        ThinNeo.TransactionOutput output = new ThinNeo.TransactionOutput();
                        output.assetId = assetid;
                        output.value = sendcount;
                        output.toAddress = ThinNeo.Helper.GetPublicKeyHashFromAddress(targetaddr);
                        list_outputs.Add(output);
                    }
                }

                //找零
                var change = count - sendcount;
                if (change > decimal.Zero)
                {
                    ThinNeo.TransactionOutput outputchange = new ThinNeo.TransactionOutput();
                    outputchange.toAddress = ThinNeo.Helper.GetPublicKeyHashFromAddress(scraddr);
                    outputchange.value = change;
                    outputchange.assetId = assetid;
                    list_outputs.Add(outputchange);
                }
                tran.outputs = list_outputs.ToArray();
            }
            else
            {
                throw new Exception("no enough money.");
            }
            return tran;
        }
    }
}
