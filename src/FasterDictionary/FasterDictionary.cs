using FASTER.core;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;

namespace FasterDictionary
{
    public partial class FasterDictionary<TKey, TValue>
    {
       


        public FasterDictionary()
        {
            _options = Options.Default;
            JobQueue = new BlockingCollection<Job>();
            
            StartConsumer();
        }

        BlockingCollection<Job> JobQueue;
        Task JobConsumerTask;

        private void StartConsumer()
        {
            JobConsumerTask = Task.Factory.StartNew(ConsumeJobs, TaskCreationOptions.LongRunning);
        }

        private void ConsumeJobs()
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
                    new LogSettings
                    {
                        LogDevice = indexLog,
                        ObjectLogDevice = objectLog,
                        SegmentSizeBits = (int)_options.SegmentSize,
                        PageSizeBits = (int)_options.PageSize,
                        MemorySizeBits = (int)_options.MemorySize
                    },
                    null,
                    new SerializerSettings<KeyEnvelope, ValueEnvelope>
                    {
                        keySerializer = () => new KeySerializer(),
                        valueSerializer = () => new ValueSerializer()
                    }
                );

            KVSession = KV.NewSession();

            foreach (var job in JobQueue.GetConsumingEnumerable())
                ServeJob(job);
        }

        long serialNum = 0;

        private long GetSerialNum()
        {
            return serialNum++;
        }

        private void ServeJob(Job job)
        {
            switch (job.Type)
            {
                case JobTypes.Upsert:
                    ServeUpsert(job);
                    break;
            }

        }

        private void ServeUpsert(Job job)
        {
            KeyEnvelope keyEnvelope = new KeyEnvelope(job.Key);
            ValueEnvelope valueEnvelope = new ValueEnvelope(job.Input);
            try
            {
                KVSession.Upsert(ref keyEnvelope, ref valueEnvelope, Context.Empty, GetSerialNum());
            }
            catch (Exception e)
            {
                job.Complete(e);
            }
            finally
            {
                job.Complete(job.Input);
            }
        }

        

        enum JobTypes
        {
            None = 0,
            Upsert = 1,
            Remove = 2,
            Get = 3
        }
        class Job
        {
            public TaskCompletionSource<TValue> TaskSource;
            public TKey Key;
            public TValue Input;
            public JobTypes Type;

            public Job(TKey key, TValue input, JobTypes type)
            {
                TaskSource = new TaskCompletionSource<TValue>(TaskCreationOptions.RunContinuationsAsynchronously);
                Key = key;
                Input = input;
                Type = type;
            }

            public void Complete(TValue value) => TaskSource.TrySetResult(value);
            public void Complete(Exception e) => TaskSource.TrySetException(e);
        }

        public readonly struct QueryResult 
        {
            public readonly TValue Value;
            public readonly bool Found;

            
            public QueryResult(bool found,  TValue value = default)
            {
                Value = value;
                Found = true;
            }
        }


        private Task<TValue> Enqueue(Job job)
        {
            throw new Exception();
        }

    }
}
