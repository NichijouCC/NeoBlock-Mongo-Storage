using System;
using System.Collections.Generic;
using System.IO;
using System.Text;


public class Log
{

    static string path = "log.txt";
    public static void WriteLog(string strLog)
    {
        FileStream fs;
        StreamWriter sw;
        if (File.Exists(path))
        //验证文件是否存在，有则追加，无则创建
        {
            fs = new FileStream(path, FileMode.Append, FileAccess.Write);
        }
        else
        {
            fs = new FileStream(path, FileMode.Create, FileAccess.Write);
        }
        sw = new StreamWriter(fs);
        sw.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH-mm-ss") + "   ---   " + strLog);
        sw.Close();
        fs.Close();
    }
}
