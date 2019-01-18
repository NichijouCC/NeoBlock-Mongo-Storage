using System.Net;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;

namespace NetAPI
{
    public class Program
    {
        static string configPath = "setting/appsettings.json";
        public static void Main(string[] args)
        {
            Config.loadFromPath(configPath);
            BuildWebHost(args).Run();
        }

        public static IWebHost BuildWebHost(string[] args) =>
            WebHost.CreateDefaultBuilder(args)
                .UseStartup<Startup>()
                .UseKestrel(options =>
                {
                    options.Listen(IPAddress.Any, 86);
                })
                .Build();
    }
}
