using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

public class Log
{

    static string path = "log.txt";
    static ReaderWriterLockSlim LogWriteLock = new ReaderWriterLockSlim();
    public static void WriteLog(string strLog)
    {
        try
        {
            LogWriteLock.EnterWriteLock();

            if (!File.Exists(path))
            //验证文件是否存在，有则追加，无则创建
            {
                FileStream fs = new FileStream(path, FileMode.Create);
                fs.Close();
                fs.Dispose();
            }
            using (StreamWriter writer = new StreamWriter(path, true))
            {
                writer.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH-mm-ss") + "   ---   " + strLog);
            }
        }catch
        {

        }
        finally
        {
            LogWriteLock.ExitWriteLock();

        }
    }

    public static void clearLog()
    {
        if(File.Exists(path))
        {
            File.Delete(path);
            File.CreateText(path);
        }
    }
}
