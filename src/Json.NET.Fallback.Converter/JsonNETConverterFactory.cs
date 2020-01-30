using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Json.NET.Fallback.Converter
{
    public class JsonNETConverterFactory : JsonConverterFactory
    {
        public override bool CanConvert(Type typeToConvert)
        {
            return (typeToConvert.GetCustomAttributes(typeof(JsonNETAttribute), false)?.Length ?? 0) > 0;
        }

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            JsonConverter converter = (JsonConverter)Activator.CreateInstance(
                typeof(JsonNETConverter<>).MakeGenericType(
                    new Type[] { typeToConvert }),
                BindingFlags.Instance | BindingFlags.Public,
                binder: null,
                args: null,
                culture: null);

            return converter;
        }

        static Newtonsoft.Json.JsonSerializer jsonSerializer = new Newtonsoft.Json.JsonSerializer();

        class JsonNETConverter<T> : JsonConverter<T>
        {
            public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                using (var jsonDocument = JsonDocument.ParseValue(ref reader))
                using (var memoryStream = new MemoryStream((int)reader.BytesConsumed))
                using (var utf8JsonWriter = new Utf8JsonWriter(memoryStream))
                {
                    jsonDocument.WriteTo(utf8JsonWriter);
                    utf8JsonWriter.Flush();
                    memoryStream.Position = 0;
                    using (var streamReader = new StreamReader(memoryStream))
                    using (var jsonReader = new Newtonsoft.Json.JsonTextReader(streamReader))
                        return jsonSerializer.Deserialize<T>(jsonReader);

                }

            }

            public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
            {
                using (var stream = new MemoryStream(64))
                using (var streamWriter = new StreamWriter(stream))
                {
                    jsonSerializer.Serialize(streamWriter, value);
                    streamWriter.Flush();
                    stream.Position = 0;
                    using (var jsonDocument = JsonDocument.Parse(stream))
                        jsonDocument.WriteTo(writer);
                }
            }
        }
    }
}
