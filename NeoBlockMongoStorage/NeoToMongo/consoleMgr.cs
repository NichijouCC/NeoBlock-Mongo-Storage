﻿using System;
using System.Collections.Generic;
using System.Text;

namespace NeoToMongo
{
    class consoleMgr
    {

        public static void run()
        {
            while (true)
            {
                Console.Write("cmd>");
                string cmd = Console.ReadLine();
                cmd = cmd.Replace(" ", "");
                if (cmd == "") continue;
                switch (cmd)
                {
                    case "1":
                        showBlockCount();          
                        break;
                }
            }
        }

        static void showBlockCount()
        {
            Console.WriteLine("current handled Block count:{0}/{1}",StateInfo.remoteBlockHeight, StateInfo.HandledBlockCount);
            //showCollecTionCouterInfo(handleBlock.collectionType);
            //showCollecTionCouterInfo(handleTx.collectionType);

        }

        static void showCollecTionCouterInfo(string type)
        {
            var count2 = Mongo.GetSystemCounter(handleTx.collectionType);
            Console.WriteLine(handleTx.collectionType + "Blockindex:" + count2.lastBlockindex+",txIndex:"+count2.lastTxindex);
        }
    }
}
