using Newtonsoft.Json;
using System;

namespace FasterDictionary.Tests
{
    internal class FasterLogger<TKey, TValue> : FasterDictionary<TKey, TValue>.ILogger
    {
        public void Debug(params string[] info)
        {
            Log("DEBUG", JsonConvert.SerializeObject(info));
        }

        private void Log(string level, string text)
        {
            Console.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} - {level} - {text}");
        }

        public void Info(params string[] info)
        {
            Log("INFO", JsonConvert.SerializeObject(info));
        }

        public void Trace(params string[] info)
        {
            Log("TRACE", JsonConvert.SerializeObject(info));
        }
    }
}