using FASTER.core;
using System;
using System.IO;

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
            var functions = new Functions(_options.Logger);

            Directory.CreateDirectory(Path.Combine(_options.PersistDirectoryPath, "Logs"));

            var indexLogPath = Path.Combine(_options.PersistDirectoryPath, "Logs", $"{_options.DictionaryName}-index.log");
            var objectLogPath = Path.Combine(_options.PersistDirectoryPath, "Logs", $"{_options.DictionaryName}-object.log");

            var indexLog = Devices.CreateLogDevice(indexLogPath, true, _options.DeleteOnClose, -1, true);
            var objectLog = Devices.CreateLogDevice(objectLogPath, true, _options.DeleteOnClose, -1, true);

            KV = new FasterKV
                <KeyEnvelope, ValueEnvelope, InputEnvelope, OutputEnvelope, Context, Functions>(
                    1L << 20, functions,
                    new LogSettings {
                        LogDevice = indexLog,
                        ObjectLogDevice = objectLog,
                        SegmentSizeBits = (int)_options.SegmentSize,
                        PageSizeBits = (int)_options.PageSize,
                        MemorySizeBits = (int)_options.MemorySize 
                    },
                    null,
                    new SerializerSettings<KeyEnvelope, ValueEnvelope> { 
                        keySerializer = () => new KeySerializer(), 
                        valueSerializer = () => new ValueSerializer() 
                    }
                );
        }

        
    }
}
