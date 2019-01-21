using System;

namespace TestNetAPI
{
    class Program
    {
        static void Main(string[] args)
        {
            while(true)
            {
                Console.Write("cmd>");
                string cmd = Console.ReadLine();
                switch (cmd)
                {
                    case "1":
                        APiTest.test_UtxoTransaction(5);
                        break;
                    case "2":
                        APiTest.test_UtxoTransaction(1000);

                        break;

                    case "3":
                        APiTest.test_PetTransaction(10);
                        break;
                }
            }
            }
    }
}
