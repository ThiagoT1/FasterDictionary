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
            public override void Deserialize(ref KeyEnvelope obj)
            {
                var keyType = (KeyTypes)Reader.ReadByte();
                if (keyType == KeyTypes.Content)
                    Deserialize(ref obj.Content);
                else
                    Deserialize(ref obj.BucketId);
            }

            public override void Serialize(ref KeyEnvelope obj)
            {
                Writer.WriteByte((byte)obj.Type);
                if (obj.Type == KeyTypes.Content)
                    Serialize(ref obj.Content);
                else
                    Serialize(ref obj.BucketId);

            }
        }

        class ValueSerializer : BaseSerializer<ValueEnvelope, TValue>
        {
            public override void Deserialize(ref ValueEnvelope obj)
            {
                var keyType = (KeyTypes)Reader.ReadByte();
                if (keyType == KeyTypes.Content)
                    Deserialize(ref obj.Content);
                else
                    Deserialize(ref obj.Bucket);

            }

            public override void Serialize(ref ValueEnvelope obj)
            {
                Writer.WriteByte((byte)obj.Type);

                if (obj.Type == KeyTypes.Content)
                {
                    var payload = obj.ConsumeSerialized();
                    var size = payload.Length;


                    Writer.WriteInt32(size);
                    Writer.Write(payload, 0, payload.Length);
                }
                else
                {
                    Serialize(ref obj.Bucket);
                }
            }
        }

        abstract class BaseSerializer<TEnvelope, TContent> : IObjectSerializer<TEnvelope>
        {
            static bool IsBytePayload;
            static bool IsIntPayload;
            static BaseSerializer()
            {
                if (typeof(TContent) == typeof(byte[]))
                    IsBytePayload = true;
                else if (typeof(TContent) == typeof(int))
                    IsIntPayload = true;
            }

            protected Stream Writer;
            protected Stream Reader;

            public void BeginDeserialize(Stream stream) => Reader = stream;
            public void BeginSerialize(Stream stream) => Writer = stream;

            public abstract void Deserialize(ref TEnvelope obj);
            public abstract void Serialize(ref TEnvelope obj);

            public virtual void Deserialize(ref TContent obj)
            {
                int size = Reader.ReadInt32();
                byte[] payload = new byte[size];

                Reader.Read(payload, 0, payload.Length);

                if (IsBytePayload)
                {
                    obj = (TContent)(object)payload;
                }
                else
                {
                    obj = JsonConvert.DeserializeObject<TContent>(UTF8.GetString(payload));
                }
            }


            public virtual void Deserialize(ref int obj)
            {
                int size = Reader.ReadInt32();
                byte[] payload = new byte[size];
                Reader.Read(payload, 0, payload.Length);

                obj = BitConverter.ToInt32(payload, 0);
            }

            public virtual void Deserialize(ref BucketInfo obj)
            {
                int size = Reader.ReadInt32();
                byte[] payload = new byte[size];

                Reader.Read(payload, 0, payload.Length);

                obj = JsonConvert.DeserializeObject<BucketInfo>(UTF8.GetString(payload));

                ComsumePadding(size, BucketInfo.PaddingSize);
            }

            private void ComsumePadding(int size, int paddingSize)
            {
                while (size > paddingSize)
                    size -= paddingSize;

                size = paddingSize - size;

                Reader.Position += size;
            }

            public virtual void Serialize(ref BucketInfo content)
            {
                byte[] payload = content.ConsumeSerialized();

                var size = payload.Length;

                Writer.WriteInt32(size);
                Writer.Write(payload, 0, payload.Length);

                ProducePadding(size, BucketInfo.PaddingSize);
            }

            private void ProducePadding(int size, int paddingSize)
            {
                while (size > paddingSize)
                    size -= paddingSize;

                size = paddingSize - size;

                while (size-- >= 0)
                    Writer.WriteByte(0);
            }

            public virtual void Serialize(ref int content)
            {
                var payload = BitConverter.GetBytes(content);
                var size = payload.Length;

                Writer.WriteInt32(size);
                Writer.Write(payload, 0, payload.Length);
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

                Writer.WriteInt32(size);
                Writer.Write(payload, 0, payload.Length);
            }

            public void EndDeserialize() => Reader = null;
            public void EndSerialize() => Writer = null;
        }

        
    }
}
