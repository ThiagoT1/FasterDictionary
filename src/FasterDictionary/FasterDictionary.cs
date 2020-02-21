using FASTER.core;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace FasterDictionary
{
    public partial class FasterDictionary<TKey, TValue>
    {

        public static bool ValueIsByteArray = typeof(TValue) == typeof(byte[]);

        public FasterDictionary(IFasterEqualityComparer<TKey> keyComparer, Options options = default)
        {
            if (keyComparer == null)
                throw new ArgumentNullException(nameof(keyComparer));



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

                IndexLog = Devices.CreateLogDevice(indexLogPath, true, _options.DeleteOnClose, -1, true);

                UnsafeContext = new Context();

                Log = new LogSettings
                {
                    LogDevice = IndexLog,
                    SegmentSizeBits = (int)_options.SegmentSize,
                    PageSizeBits = (int)_options.PageSize,
                    MemorySizeBits = (int)_options.MemorySize
                    //,ReadCacheSettings = new ReadCacheSettings()
                    //{
                    //    MemorySizeBits = (int)_options.MemorySize + 1,
                    //    PageSizeBits = (int)_options.PageSize + 1,
                    //    SecondChanceFraction = .2
                    //}
                };

                var checkpointSettings = new CheckpointSettings()
                {
                    CheckpointDir = checkpointDir,
                    CheckPointType = _options.CheckPointType
                };

                var variableLengthStructSettings = new VariableLengthStructSettings<VariableEnvelope, VariableEnvelope>()
                {
                    keyLength = VariableEnvelope.Settings,
                    valueLength = VariableEnvelope.Settings
                };

                KV = new FasterKV
                    <VariableEnvelope, VariableEnvelope, byte[], byte[], Context, Functions>(
                        1L << 20, functions,
                        Log,
                        checkpointSettings,
                        null,
                        new VariableEnvelopeComparer(),
                        variableLengthStructSettings
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

                case JobTypes.IterateKey:
                    ServeKeyIteration(job);
                    break;

                case JobTypes.IteratePair:
                    ServePairIteration(job);
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

            //Hell! I need to tunr this to a method

            var keyJson = JsonSerializer.SerializeToUtf8Bytes(job.Key, JsonOptions);

            Span<byte> keyTarget = stackalloc byte[keyJson.Length + sizeof(int)];

            ref var keyEnvelope = ref MemoryMarshal.AsRef<VariableEnvelope>(keyTarget);

            keyTarget = keyTarget.Slice(sizeof(int), keyTarget.Length - sizeof(int));

            keyEnvelope.Size = keyJson.Length;

            keyJson.CopyTo(keyTarget);



            byte[] output = default;

            Status status = Status.ERROR;
            try
            {
                status = KVSession.Delete(ref keyEnvelope, UnsafeContext, GetSerialNum());
                if (status == Status.PENDING)
                {
                    KVSession.CompletePending(true, true);
                    status = UnsafeContext.Consume(out output);
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
            //Hell! I need to tunr this to a method

            var keyJson = JsonSerializer.SerializeToUtf8Bytes(job.Key, JsonOptions);

            Span<byte> keyTarget = stackalloc byte[keyJson.Length + sizeof(int)];

            ref var keyEnvelope = ref MemoryMarshal.AsRef<VariableEnvelope>(keyTarget);

            keyTarget = keyTarget.Slice(sizeof(int), keyTarget.Length - sizeof(int));

            keyEnvelope.Size = keyJson.Length;

            keyJson.CopyTo(keyTarget);

            byte[] inputEnvelope = default;
            byte[] outputEnvelope = default;


            try
            {

                var readTask = KVSession.ReadAsync(ref keyEnvelope, ref inputEnvelope, null);
                if (readTask.IsCompleted)
                {
                    CompleteGetContent(readTask.Result.Item1, readTask.Result.Item2, job);
                }
                else
                {
                    readTask.AsTask().ContinueWith(t => CompleteGetContent(t.Result.Item1, t.Result.Item2, job));
                }

            }
            catch (Exception e)
            {
                job.Complete(e);
            }
        }

        private void CompleteGetContent(Status status, byte[] outputEnvelope, Job job)
        {
            switch (status)
            {
                case Status.ERROR:
                    job.Complete(new Exception($"read error => {JsonSerializer.Serialize(job.Key)}"));
                    break;
                case Status.NOTFOUND:
                    job.Complete(false);
                    break;
                case Status.OK:
                    if (ValueIsByteArray)
                    {
                        job.Complete(true, (TValue)(object)outputEnvelope);
                        break;
                    }

                    var readOnly = new ReadOnlySpan<byte>(outputEnvelope);
                    TValue value = JsonSerializer.Deserialize<TValue>(readOnly);
                    job.Complete(true, value);
                    break;
                default:
                    job.Complete(new Exception($"Read WTF => {status} - {JsonSerializer.Serialize(job.Key)}"));
                    break;
            }
        }


        private void ServeUpsert(Job job)
        {
            //Hell! I need to tunr this to a method

            var keyJson = JsonSerializer.SerializeToUtf8Bytes(job.Key, JsonOptions);

            Span<byte> keyTarget = new byte[keyJson.Length + sizeof(int)];

            ref var keyEnvelope = ref MemoryMarshal.AsRef<VariableEnvelope>(keyTarget);

            keyTarget = keyTarget.Slice(sizeof(int), keyTarget.Length - sizeof(int));

            keyEnvelope.Size = keyJson.Length;

            keyJson.CopyTo(keyTarget);


            byte[] valueBytes;

            Span<byte> valueTarget;

            if (ValueIsByteArray)
            {
                valueBytes = (byte[])(object)job.Input;
            }
            else
            {
                valueBytes = JsonSerializer.SerializeToUtf8Bytes(job.Input, JsonOptions);
            }

            valueTarget = new byte[valueBytes.Length + sizeof(int)];

            ref var valueEnvelope = ref MemoryMarshal.AsRef<VariableEnvelope>(valueTarget);

            valueTarget = valueTarget.Slice(sizeof(int), valueTarget.Length - sizeof(int));

            valueEnvelope.Size = valueBytes.Length;

            valueBytes.CopyTo(valueTarget);


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
            IteratePair = 7,
            IterateKey = 8,
            ReleaseIterator = 9,
            Dispose = 10
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
