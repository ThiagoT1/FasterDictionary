using FASTER.core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace FasterDictionary
{
    public partial class FasterDictionary<TKey, TValue>
    {
        abstract class BaseSerializer<T> : IObjectSerializer<T>
        {
            protected Stream Writer;
            protected Stream Reader;
            public void BeginDeserialize(Stream stream) => Reader = stream;
            public void BeginSerialize(Stream stream) => Writer = stream;

            public abstract void Deserialize(ref T obj);
            public abstract void Serialize(ref T obj);

            public void EndDeserialize() => Reader = null;
            public void EndSerialize() => Writer = null;
        }


        class KeySerializer : BaseSerializer<KeyEnvelope>
        {
            static bool IsBytePayload;
            static KeySerializer()
            {
                if (typeof(TKey) == typeof(byte[]))
                {
                    IsBytePayload = 
                }
            }

            public override void Deserialize(ref KeyEnvelope obj)
            {
                throw new NotImplementedException();
            }

            public override void Serialize(ref KeyEnvelope obj)
            {
                throw new NotImplementedException();
            }
        }

    }
}
