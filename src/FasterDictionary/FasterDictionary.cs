using FASTER.core;
using System;

namespace FasterDictionary
{
    public enum MemorySizes
    {
        MB16 = 27,
        MB32 = MB16 + 1,
        MB64 = MB32 + 1,
        MB128 = MB64 + 1,
        MB256 = MB128 + 1,
        MB512 = MB256 + 1,
        MB1024 = MB512 + 1,
        GB1 = MB1024,
        GB2 = GB1 + 1
    }

    

    public partial class FasterDictionary<TKey, TValue>
    {
       
        public interface IKeyComparer
        {

        }

        

        


        

        public FasterDictionary()
        {
            _options = Options.Default;
            Initialize();
        }

        private void Initialize()
        {
            var functions = new SimpleFunctions<>

            var log = Devices.CreateLogDevice(Path.GetTempPath() + "hlog.log");
            var objlog = Devices.CreateLogDevice(Path.GetTempPath() + "hlog.obj.log");

            var h = new FasterKV
                <MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions>
                (1L << 20, new MyFunctions(),
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MemorySizeBits = 29 },
                null,
                new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                );

            KV = new 
            throw new NotImplementedException();
        }
    }
}
