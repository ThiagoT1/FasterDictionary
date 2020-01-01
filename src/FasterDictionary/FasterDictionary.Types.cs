using FASTER.core;
using Newtonsoft.Json;
using System;
namespace FasterDictionary
{
    public partial class FasterDictionary<TKey, TValue>
    {
        public struct KeyEnvelope
        {
            public TKey Content;
            public KeyEnvelope(TKey content)
            {
                Content = content;
            }
        }

        public struct ValueEnvelope
        {
            public TValue Content;

            public ValueEnvelope(TValue content)
            {
                Content = content;
            }

            internal bool SameSize(ref TValue content)
            {
                return false;
            }
        }

        public struct InputEnvelope
        {
            public TValue Content;
        }

        public struct OutputEnvelope
        {
            public ValueEnvelope Content;
        }

        public class Context
        {
            public static Context Empty = new Context();

            Status Status = Status.NOTFOUND;
            OutputEnvelope Value;

            internal void CompleteRead(ref Status status, ref OutputEnvelope output)
            {
                Status = status;
                Value = output;
            }

            internal Status Consume(out OutputEnvelope output)
            {
                output = Value;
                Value = default;
                var status = Status;
                Status = Status.NOTFOUND;
                return status;
            }
        }

        public interface ILogger
        {
            void Info(params string[] info);
            void Debug(params string[] info);
            void Trace(params string[] info);
        }


        public class Functions : IFunctions<KeyEnvelope, ValueEnvelope, InputEnvelope, OutputEnvelope, Context>
        {
            public Functions(ILogger logger)
            {
                Logger = logger;
            }

            public ILogger Logger { get; }

            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
            {
                Logger.Info(nameof(CheckpointCompletionCallback), $"SessionId: {sessionId}", $"CommitPoint: {JsonConvert.SerializeObject(commitPoint)}");
            }

            public void ConcurrentReader(ref KeyEnvelope key, ref InputEnvelope input, ref ValueEnvelope value, ref OutputEnvelope dst)
            {
                dst.Content = value;
            }

            public bool ConcurrentWriter(ref KeyEnvelope key, ref ValueEnvelope src, ref ValueEnvelope dst)
            {
                if (!src.SameSize(ref dst.Content))
                    return false;
                
                dst = src;
                return true;
            }

            public bool InPlaceUpdater(ref KeyEnvelope key, ref InputEnvelope input, ref ValueEnvelope value)
            {
                if (!value.SameSize(ref input.Content))
                    return false;

                value.Content = input.Content;
                return true;
            }

            public void CopyUpdater(ref KeyEnvelope key, ref InputEnvelope input, ref ValueEnvelope oldValue, ref ValueEnvelope newValue)
            {
                newValue = oldValue;
            }

            public void DeleteCompletionCallback(ref KeyEnvelope key, Context ctx)
            {
                Logger.Trace(nameof(DeleteCompletionCallback), $"Key: {key.Content}");
            }

            public void InitialUpdater(ref KeyEnvelope key, ref InputEnvelope input, ref ValueEnvelope value)
            {
                value.Content = input.Content;
            }

            

            public void ReadCompletionCallback(ref KeyEnvelope key, ref InputEnvelope input, ref OutputEnvelope output, Context ctx, Status status)
            {
                ctx.CompleteRead(ref status, ref output);
            }

            public void RMWCompletionCallback(ref KeyEnvelope key, ref InputEnvelope input, Context ctx, Status status)
            {
                Logger.Trace(nameof(RMWCompletionCallback), $"Key: {key.Content}");
            }

            public void SingleReader(ref KeyEnvelope key, ref InputEnvelope input, ref ValueEnvelope value, ref OutputEnvelope dst)
            {
                dst.Content = value;
            }

            public void SingleWriter(ref KeyEnvelope key, ref ValueEnvelope src, ref ValueEnvelope dst)
            {
                dst = src;
            }

            public void UpsertCompletionCallback(ref KeyEnvelope key, ref ValueEnvelope value, Context ctx)
            {
                Logger.Trace(nameof(UpsertCompletionCallback), $"Key: {key.Content}");
            }
        }

        LogSettings Log;

        FasterKV<KeyEnvelope, ValueEnvelope, InputEnvelope, OutputEnvelope, Context, Functions> KV;

        ClientSession<KeyEnvelope, ValueEnvelope, InputEnvelope, OutputEnvelope, Context, Functions> KVSession;

        Context UnsafeContext;

        
    }
}

