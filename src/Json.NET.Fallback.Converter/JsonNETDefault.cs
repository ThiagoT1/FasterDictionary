using Json.NET.Fallback.Converter;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace Json.NET.Fallback
{
    public static class JsonNETDefault
    {
        public static JsonSerializerOptions Options { get; set; }
        static JsonNETDefault()
        {
            Options = new JsonSerializerOptions()
            {
                IgnoreNullValues = true
            };
            Options.Converters.Add(new JsonNETConverterFactory());
        }
    }
}
