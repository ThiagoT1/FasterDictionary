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

        class KeySerializer : BaseSerializer<KeyEnvelope, TKey>
        {
            public override void Deserialize(ref KeyEnvelope obj) => Deserialize(ref obj.Content);
            public override void Serialize(ref KeyEnvelope obj) => Serialize(ref obj.Content);
        }
        class ValueSerializer : BaseSerializer<ValueEnvelope, TValue>
        {
            public override void Deserialize(ref ValueEnvelope obj) => Deserialize(ref obj.Content);
            public override void Serialize(ref ValueEnvelope obj) => Serialize(ref obj.Content);
        }

        abstract class BaseSerializer<TEnvelope, TContent> : IObjectSerializer<TEnvelope>
        {
            static bool IsBytePayload;
            static bool IsIntPayload;
            static BaseSerializer()
            {
                if (typeof(TContent) == typeof(byte[]))
                    IsBytePayload = true;
                //else if (typeof(TContent) == typeof(int))
                //    IsIntPayload = true;
            }

            protected Stream Writer;
            protected Stream Reader;

            public void BeginDeserialize(Stream stream) => Reader = stream;
            public void BeginSerialize(Stream stream) => Writer = stream;

            public abstract void Deserialize(ref TEnvelope obj);
            public abstract void Serialize(ref TEnvelope obj);

            public virtual void Deserialize(ref TContent obj)
            {
                int size = Reader.ReadInt();
                byte[] payload = new byte[size];
                Reader.Read(payload, 0, payload.Length);

                if (IsBytePayload)
                {
                    obj = (TContent)(object)payload;
                }
                else if (IsIntPayload)
                {
                    obj = (TContent)(object)BitConverter.ToInt32(payload, 0);
                }
                else
                {
                    obj = JsonConvert.DeserializeObject<TContent>(UTF8.GetString(payload));
                }
            }
            public virtual void Serialize(ref TContent content)
            {
                int size = 0;
                byte[] payload = null;

                if (IsBytePayload)
                {
                    payload = content as byte[];
                    if (payload != null)
                        size = payload.Length;
                }
                else if (IsIntPayload)
                {
                    payload = BitConverter.GetBytes((int)(object)content);
                    size = payload.Length;
                }
                else
                {
                    payload = UTF8.GetBytes(JsonConvert.SerializeObject(content));
                    size = payload.Length;
                }

                Writer.WriteInt(size);
                Writer.Write(payload, 0, payload.Length);
            }

            public void EndDeserialize() => Reader = null;
            public void EndSerialize() => Writer = null;
        }

        
    }
}
