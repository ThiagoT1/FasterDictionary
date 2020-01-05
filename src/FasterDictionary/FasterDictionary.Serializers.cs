using FASTER.core;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace FasterDictionary
{
    public partial class FasterDictionary<TKey, TValue>
    {
        static UTF8Encoding UTF8;
        static FasterDictionary()
        {
            UTF8 = new UTF8Encoding(false);
        }

        static class ByteSerializer<TContent>
        {
            static bool IsBytePayload;
            static bool IsIntPayload;
            static ByteSerializer()
            {
                if (typeof(TContent) == typeof(byte[]))
                    IsBytePayload = true;
                else if (typeof(TContent) == typeof(int))
                    IsIntPayload = true;
            }

            public static void Deserialize(ref byte[] source, ref TContent target)
            {
                if (IsBytePayload)
                {
                    target = (TContent)(object)source;
                }
                else if (IsIntPayload)
                {
                    target = (TContent)(object)BitConverter.ToInt32(source, 0);
                }
                else
                {
                    target = JsonConvert.DeserializeObject<TContent>(UTF8.GetString(source));
                }
            }

            public static void Serialize(ref TContent source, ref byte[] target)
            {
                if (IsBytePayload)
                {
                    target = source as byte[];
                }
                else if (IsIntPayload)
                {
                    target = BitConverter.GetBytes((int)(object)source);
                }
                else
                {
                    target = UTF8.GetBytes(JsonConvert.SerializeObject(source));
                }
            }
        }
    }
}
