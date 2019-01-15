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




    }
}
