using System;
using System.Collections.Generic;
using System.Text;

namespace NeoToMongo
{
    class StateInfo
    {
        private static int _currentBlock=0;
        public static int currentBlockHeight
        {
            get
            {
                return _currentBlock;
            }
            set
            {
                _currentBlock = value;
            }
        }

        private static Dictionary<string, Mongo.Couter> info;

        public static void stop()
        {

        }

        public static void init()
        {
            if(info==null)
            {
                info = new Dictionary<string, Mongo.Couter>();
            }
            info.Add(handleBlock.collectionType, Mongo.GetSystemCounter(handleBlock.collectionType));
            info.Add(handleTx.collectionType, Mongo.GetSystemCounter(handleTx.collectionType));
        }




    }
}
