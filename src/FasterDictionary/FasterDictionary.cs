using FASTER.core;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;

namespace FasterDictionary
{
    public partial class FasterDictionary<TKey, TValue>
    {
       


        public FasterDictionary(Options options = default)
        {
            _options = Options.Default;
            if (options.PersistDirectoryPath != null)
            {
                _options.DeleteOnClose = options.DeleteOnClose;
                _options.DictionaryName = options.DictionaryName;
                _options.KeyComparer = options.KeyComparer;
                _options.Logger = options.Logger;
                _options.PersistDirectoryPath = options.PersistDirectoryPath;
                
                if (options.MemorySize != 0)
                    _options.MemorySize = options.MemorySize;

                if (options.PageSize != 0)
                    _options.PageSize = options.PageSize;

                if (options.SegmentSize != 0)
                    _options.SegmentSize = options.SegmentSize;
            }

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

            UnsafeContext = new Context();

            Log = new LogSettings
            {
                LogDevice = indexLog,
                ObjectLogDevice = objectLog,
                SegmentSizeBits = (int)_options.SegmentSize,
                PageSizeBits = (int)_options.PageSize,
                MemorySizeBits = (int)_options.MemorySize
            };

            KV = new FasterKV
                <KeyEnvelope, ValueEnvelope, InputEnvelope, OutputEnvelope, Context, Functions>(
                    1L << 20, functions,
                    Log,
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
                
                case JobTypes.Get:
                    ServeGet(job);
                    break;
                
                case JobTypes.Remove:
                    ServeRemove(job);
                    break;

                case JobTypes.Ping:
                    ServePing(job);
                    break;

                case JobTypes.Dispose:
                    ServeDispose(job);
                    break;
            }

        }

        private void ServePing(Job job)
        {
            job.Complete(false);
        }

        private void ServeDispose(Job job)
        {
            try
            {

                JobQueue.CompleteAdding();

                Log.LogDevice.Close();
                Log.ObjectLogDevice.Close();

                KVSession.Dispose();
                KV.Dispose();

                job.Complete(false);

            }
            catch (Exception e)
            {
                job.Complete(e);
            }
        }

        private void ServeRemove(Job job)
        {
            KeyEnvelope keyEnvelope = new KeyEnvelope(job.Key);
            OutputEnvelope outputEnvelope = default;
            Status status = Status.ERROR;
            try
            {
                status = KVSession.Delete(ref keyEnvelope, UnsafeContext, GetSerialNum());
                if (status == Status.PENDING)
                {
                    KVSession.CompletePending(true, true);
                    status = UnsafeContext.Consume(out outputEnvelope);
                }
            }
            catch (Exception e)
            {
                job.Complete(e);
            }

            switch (status)
            {
                case Status.ERROR:
                    job.Complete(new Exception($"read error => {JsonConvert.SerializeObject(job.Key)}"));
                    break;
                case Status.NOTFOUND:
                    job.Complete(false);
                    break;
                case Status.OK:
                    job.Complete(true, default);
                    break;
            }
        }

        private void ServeGet(Job job)
        {
            KeyEnvelope keyEnvelope = new KeyEnvelope(job.Key);
            InputEnvelope inputEnvelope = default;
            OutputEnvelope outputEnvelope = default;
            Status status = Status.ERROR;
            try
            {
                status = KVSession.Read(ref keyEnvelope, ref inputEnvelope, ref outputEnvelope, UnsafeContext, GetSerialNum());
                if (status == Status.PENDING)
                {
                    KVSession.CompletePending(true, true);
                    status = UnsafeContext.Consume(out outputEnvelope);
                }
            }
            catch (Exception e)
            {
                job.Complete(e);
            }

            switch (status)
            {
                case Status.ERROR:
                    job.Complete(new Exception($"read error => {JsonConvert.SerializeObject(job.Key)}"));
                    break;
                case Status.NOTFOUND:
                    job.Complete(false);
                    break;
                case Status.OK:
                    job.Complete(true, outputEnvelope.Content.Content);
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
                job.Complete(true, job.Input);
            }
        }

        

        enum JobTypes
        {
            None = 0,
            Upsert = 1,
            Remove = 2,
            Get = 3,
            Ping = 4,
            Dispose = 9
        }
        class Job
        {
            public TaskCompletionSource<ReadResult> TaskSource;
            public TKey Key;
            public TValue Input;
            public JobTypes Type;

            public Job(TKey key, TValue input, JobTypes type)
            {
                TaskSource = new TaskCompletionSource<ReadResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                Key = key;
                Input = input;
                Type = type;
            }

            public Job(TKey key, JobTypes type)
            {
                TaskSource = new TaskCompletionSource<ReadResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                Key = key;
                Type = type;
            }

            public Job(JobTypes type)
            {
                TaskSource = new TaskCompletionSource<ReadResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                Type = type;
            }

            public void Complete(bool found, TValue value = default) => TaskSource.TrySetResult(new ReadResult(found, value));
            public void Complete(Exception e) => TaskSource.TrySetException(e);
        }

        public readonly struct ReadResult 
        {
            public readonly TValue Value;
            public readonly bool Found;

            
            public ReadResult(bool found,  TValue value = default)
            {
                Value = value;
                Found = found;
            }
        }


        private Task<ReadResult> Enqueue(Job job)
        {
            JobQueue.Add(job);
            return job.TaskSource.Task;
        }

    }
}
