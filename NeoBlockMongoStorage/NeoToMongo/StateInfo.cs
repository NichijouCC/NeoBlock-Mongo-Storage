using System;
using System.Collections.Generic;
using System.Text;

namespace NeoToMongo
{
    class StateInfo
    {
        //public static int loadedBlockHeight = 0;
        //public static int handledBlockCount = 0;

        public static int HandlingBlockCount = 0;
        public static int HandledBlockCount = 0;
        public static int remoteBlockHeight = 0;

        public static void init()
        {
            HandlingBlockCount = Config.startBlockHeight;
            HandledBlockCount = Config.startBlockHeight;
        }

    }
}
