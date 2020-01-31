using FASTER.core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FasterDictionary
{
    public partial class FasterDictionary<TKey, TValue>
    {
        public static JsonSerializerOptions JsonOptions;
        static FasterDictionary()
        {
            JsonOptions = new JsonSerializerOptions()
            {
                IgnoreNullValues = true
            };
        }
    }
}
