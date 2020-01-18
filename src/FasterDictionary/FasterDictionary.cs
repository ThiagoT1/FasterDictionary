using FASTER.core;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace FasterDictionary
{
    public partial class FasterDictionary<TKey, TValue>
    {



        public FasterDictionary(IFasterEqualityComparer<TKey> keyComparer, Options options = default)
        {
            if (keyComparer == null)
                throw new ArgumentNullException(nameof(keyComparer));

            _keyComparer = new KeyComparerAdapter(keyComparer);
            _options = Options.Default;
            if (options.PersistDirectoryPath != null)
            {
                _options.DeleteOnClose = options.DeleteOnClose;
                _options.DictionaryName = options.DictionaryName;

                _options.Logger = options.Logger;
                _options.PersistDirectoryPath = options.PersistDirectoryPath;

                if (options.CheckPointType != CheckpointType.Snapshot)
                    _options.CheckPointType = options.CheckPointType;

                if (options.MemorySize != 0)
                    _options.MemorySize = options.MemorySize;

                if (options.PageSize != 0)
                    _options.PageSize = options.PageSize;

                if (options.SegmentSize != 0)
                    _options.SegmentSize = options.SegmentSize;
            }

            JobQueue = Channel.CreateUnbounded<Job>(new UnboundedChannelOptions()
            {
                AllowSynchronousContinuations = true,
                SingleReader = true,
                SingleWriter = false

            });

            StartConsumer();
        }

        Channel<Job> JobQueue;
        Task JobConsumerTask;
        ExceptionDispatchInfo ExceptionInfo;

        private void StartConsumer()
        {
            JobConsumerTask = Task.Factory.StartNew(ConsumeJobs, TaskCreationOptions.LongRunning);
        }

        const string CheckPoints = nameof(CheckPoints);

        const string CprCheckPoints = "cpr-checkpoints";
        const string IndexCheckPoints = "index-checkpoints";

        const string Logs = nameof(Logs);

        private async Task ConsumeJobs()
        {
            try
            {

                var functions = new Functions(_options.Logger);

                var checkpointDir = Path.Combine(_options.PersistDirectoryPath, CheckPoints);
                var logDir = Path.Combine(_options.PersistDirectoryPath, Logs);

                Directory.CreateDirectory(logDir);
                Directory.CreateDirectory(checkpointDir);

                var indexLogPath = Path.Combine(logDir, $"{_options.DictionaryName}-index.log");
                var objectLogPath = Path.Combine(logDir, $"{_options.DictionaryName}-object.log");

                IndexLog = Devices.CreateLogDevice(indexLogPath, true, _options.DeleteOnClose, -1, true);
                ObjectLog = Devices.CreateLogDevice(objectLogPath, true, _options.DeleteOnClose, -1, true);

                UnsafeContext = new Context();

                Log = new LogSettings
                {
                    LogDevice = IndexLog,
                    ObjectLogDevice = ObjectLog,
                    SegmentSizeBits = (int)_options.SegmentSize,
                    PageSizeBits = (int)_options.PageSize,
                    MemorySizeBits = (int)_options.MemorySize,
                    ReadCacheSettings = new ReadCacheSettings()
                    {
                        MemorySizeBits = (int)_options.MemorySize + 1,
                        PageSizeBits = (int)_options.PageSize + 1,
                        SecondChanceFraction = .2
                    }
                };

                var checkpointSettings = new CheckpointSettings()
                {
                    CheckpointDir = checkpointDir,
                    CheckPointType = _options.CheckPointType
                };

                KV = new FasterKV
                    <KeyEnvelope, ValueEnvelope, ValueEnvelope, ValueEnvelope, Context, Functions>(
                        1L << 20, functions,
                        Log,
                        checkpointSettings,
                        new SerializerSettings<KeyEnvelope, ValueEnvelope>
                        {
                            keySerializer = () => new KeySerializer(),
                            valueSerializer = () => new ValueSerializer()
                        },
                        _keyComparer
                    );

                var logCount = Directory.GetDirectories(checkpointDir).Length;
                if (logCount > 0)
                    KV.Recover();

                KVSession = KV.NewSession();

                var reader = JobQueue.Reader;

                while (await reader.WaitToReadAsync())
                {
                    while (reader.TryRead(out Job item))
                    {
                        ServeJob(item);
                        if (item.JobType == JobTypes.Dispose)
                            return;
                    }
                }
            }
            catch (Exception e)
            {
                ExceptionInfo = ExceptionDispatchInfo.Capture(e);
            }
        }

        long serialNum = 0;



        private long GetSerialNum()
        {
            return serialNum++;
        }

        private void ServeJob(Job job)
        {
            switch (job.JobType)
            {
                case JobTypes.Upsert:
                    ServeUpsert(job);
                    break;

                case JobTypes.Get:
                    ServeGetContent(job);
                    break;

                case JobTypes.Remove:
                    ServeRemove(job);
                    break;

                case JobTypes.Ping:
                    ServePing(job);
                    break;

                case JobTypes.Save:
                    if (StartSave(job, out Guid token))
                        Task.Run(async () => await FinishServeSave(job, token)).Wait();
                    break;

                case JobTypes.AquireIterator:
                    ServeAquireIterator(job);
                    break;

                case JobTypes.ReleaseIterator:
                    ServeReleaseIterator(job);
                    break;

                case JobTypes.Iterate:
                    ServeIteration(job);
                    break;

                case JobTypes.Dispose:
                    ServeDispose(job);
                    break;
            }


        }

        private bool StartSave(Job job, out Guid token)
        {
            token = default;
            try
            {
                KV.TakeFullCheckpoint(out token);
                return true;

            }
            catch (Exception e)
            {
                job.Complete(e);
                return false;
            }
        }

        private async Task FinishServeSave(Job job, Guid token)
        {
            try
            {

                await KV.CompleteCheckpointAsync();

                var searchPath = Path.Combine(_options.PersistDirectoryPath, CheckPoints, IndexCheckPoints);

                string[] currentShots = null;

                
                if (_options.CheckPointType == CheckpointType.Snapshot && Directory.Exists(searchPath))
                {
                    currentShots = Directory
                        .GetDirectories(searchPath)
                        .Select(x => Path.GetFileName(x))
                        .Where(x => x != token.ToString())
                        .ToArray();
                }

                if (currentShots != null)
                    foreach (var currentShot in currentShots)
                    {
                        Directory.Delete(Path.Combine(_options.PersistDirectoryPath, CheckPoints, CprCheckPoints, currentShot), true);
                        Directory.Delete(Path.Combine(_options.PersistDirectoryPath, CheckPoints, IndexCheckPoints, currentShot), true);
                    }

                job.Complete(false);

            }
            catch (Exception e)
            {
                job.Complete(e);
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

                JobQueue.Writer.Complete();

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
            ValueEnvelope outputEnvelope = default;

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
                    job.Complete(new Exception($"read error => {JsonSerializer.Serialize(job.Key)}"));
                    break;
                case Status.NOTFOUND:
                    job.Complete(false);
                    break;
                case Status.OK:
                    job.Complete(true, default);
                    break;
            }
        }

        private void ServeGetContent(Job job)
        {
            KeyEnvelope keyEnvelope = new KeyEnvelope(job.Key);

            ValueEnvelope inputEnvelope = default;
            ValueEnvelope outputEnvelope = default;

            Status status = Status.ERROR;

            try
            {
                status = ExecuteGet(ref keyEnvelope, ref inputEnvelope, ref outputEnvelope);

                switch (status)
                {
                    case Status.ERROR:
                        job.Complete(new Exception($"read error => {JsonSerializer.Serialize(job.Key)}"));
                        break;
                    case Status.NOTFOUND:
                        job.Complete(false);
                        break;
                    case Status.OK:
                        job.Complete(true, outputEnvelope.Content);
                        break;
                    default:
                        job.Complete(new Exception($"Read WTF => {status} - {JsonSerializer.Serialize(job.Key)}"));
                        break;
                }
            }
            catch (Exception e)
            {
                job.Complete(e);
            }
        }

        private Status ExecuteGet(ref KeyEnvelope keyEnvelope, ref ValueEnvelope inputEnvelope, ref ValueEnvelope outputEnvelope)
        {
            Status status = KVSession.Read(ref keyEnvelope, ref inputEnvelope, ref outputEnvelope, UnsafeContext, GetSerialNum());
            if (status == Status.PENDING)
            {
                KVSession.CompletePending(true, true);
                status = UnsafeContext.Consume(out outputEnvelope);
            }

            return status;
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
            Save = 5,
            AquireIterator = 6,
            Iterate = 7,
            ReleaseIterator = 8,
            Dispose = 9
        }
        class Job
        {
            const bool ContinueAsync = true;
            const bool ContinueSync = false;

            public AsyncOperation<ReadResult> AsyncOp;

            public TKey Key;
            public TValue Input;
            public JobTypes JobType;

            public Job(TKey key, TValue input, JobTypes type)
            {
                AsyncOp = new AsyncOperation<ReadResult>(ContinueAsync);
                //TaskSource = new TaskCompletionSource<ReadResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                Key = key;
                Input = input;
                JobType = type;
            }

            public Job(TKey key, JobTypes type)
            {
                AsyncOp = new AsyncOperation<ReadResult>(ContinueAsync);
                Key = key;
                JobType = type;
            }

            public Job(JobTypes type)
            {
                AsyncOp = new AsyncOperation<ReadResult>(ContinueAsync);
                JobType = type;
            }

            public void Complete(bool found, TValue value = default) => AsyncOp?.TrySetResult(new ReadResult(found, Key, value));
            public void Complete(Exception e) => AsyncOp?.TrySetException(e);
        }

        public readonly struct ReadResult
        {
            public readonly TKey Key;
            public readonly TValue Value;
            public readonly bool Found;


            public ReadResult(bool found, TKey key = default, TValue value = default)
            {
                Value = value;
                Key = key;
                Found = found;
            }
        }


        private ValueTask<ReadResult> Enqueue(Job job)
        {
            if (ExceptionInfo != null)
                ExceptionInfo.Throw();

            JobQueue.Writer.TryWrite(job);
            if (job.AsyncOp == null)
                return new ValueTask<ReadResult>(default(ReadResult));
            return job.AsyncOp.ValueTaskOfT;
        }

    }
}
