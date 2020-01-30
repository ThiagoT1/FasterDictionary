using System;
using System.Text.Json;
using Xunit;

namespace Json.NET.Fallback.Tests
{
    public class JsonNETTests
    {
        [JsonNET]
        public struct FieldStruct
        {
            public int Field1;
            public DateTime Field2;
        }

        [Fact]
        public void FieldStructTest()
        {
            var data = new FieldStruct()
            {
                Field1 = 1,
                Field2 = DateTime.Now
            };
            var json = JsonSerializer.Serialize(data, JsonNETDefault.Options);

            var deserialized = JsonSerializer.Deserialize<FieldStruct>(json, JsonNETDefault.Options);

            Assert.Equal(data.Field2, deserialized.Field2);
            Assert.Equal(data.Field1, deserialized.Field1);

        }


        [JsonNET]
        public readonly struct CtorStruct
        {
            public readonly int Field1;
            public readonly DateTime Field2;

            [Newtonsoft.Json.JsonConstructor]
            public CtorStruct(int field1, DateTime field2)
            {
                Field1 = field1;
                Field2 = field2;
            }
        }

        [Fact]
        public void CtorStructTest()
        {
            var data = new CtorStruct(1, DateTime.Now);
            
            var json = JsonSerializer.Serialize(data, JsonNETDefault.Options);

            var deserialized = JsonSerializer.Deserialize<FieldStruct>(json, JsonNETDefault.Options);

            Assert.Equal(data.Field2, deserialized.Field2);
            Assert.Equal(data.Field1, deserialized.Field1);

        }


    }
}
