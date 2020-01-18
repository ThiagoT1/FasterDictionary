using FASTER.core;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace FasterDictionary
{

   
    public partial class FasterDictionary<TKey, TValue>
    {

        public struct StubEnvelope { }

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
        }

        public class Context
        {
            public static Context Empty = new Context();

            Status Status = Status.NOTFOUND;
            ValueEnvelope Value;

            internal void CompleteRead(ref Status status, ref ValueEnvelope output)
            {
                Status = status;
                Value = output;
            }

            internal Status Consume(out ValueEnvelope output)
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


        public class Functions : IFunctions<KeyEnvelope, ValueEnvelope, ValueEnvelope, ValueEnvelope, Context>
        {
            public Functions(ILogger logger)
            {
                Logger = logger;
            }

            public ILogger Logger { get; }

            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
            {
                Logger.Info(nameof(CheckpointCompletionCallback), $"SessionId: {sessionId}", $"CommitPoint: {JsonSerializer.Serialize(commitPoint)}");
            }

            public void ConcurrentReader(ref KeyEnvelope key, ref ValueEnvelope input, ref ValueEnvelope value, ref ValueEnvelope dst)
            {
                dst = value;
            }

            public bool ConcurrentWriter(ref KeyEnvelope key, ref ValueEnvelope src, ref ValueEnvelope dst)
            {
                //return true;

                //if (!src.SameSize(ref dst))
                //    return false;
                
                dst = src;
                return true;
            }

            public bool InPlaceUpdater(ref KeyEnvelope key, ref ValueEnvelope input, ref ValueEnvelope value)
            {
                //return true;

                //if (!value.SameSize(ref input))
                //    return false;

                value = input;
                return true;
            }

            public void CopyUpdater(ref KeyEnvelope key, ref ValueEnvelope input, ref ValueEnvelope oldValue, ref ValueEnvelope newValue)
            {
                newValue = oldValue;
            }

            public void DeleteCompletionCallback(ref KeyEnvelope key, Context ctx)
            {
                Logger.Trace(nameof(DeleteCompletionCallback), $"Key: {key.Content}");
            }

            public void InitialUpdater(ref KeyEnvelope key, ref ValueEnvelope input, ref ValueEnvelope value)
            {
                value = input;
            }

            

            public void ReadCompletionCallback(ref KeyEnvelope key, ref ValueEnvelope input, ref ValueEnvelope output, Context ctx, Status status)
            {
                ctx.CompleteRead(ref status, ref output);
            }

            public void RMWCompletionCallback(ref KeyEnvelope key, ref ValueEnvelope input, Context ctx, Status status)
            {
                Logger.Trace(nameof(RMWCompletionCallback), $"Key: {key.Content}");
            }

            public void SingleReader(ref KeyEnvelope key, ref ValueEnvelope input, ref ValueEnvelope value, ref ValueEnvelope dst)
            {
                dst = value;
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
        IDevice IndexLog;
        IDevice ObjectLog;

        FasterKV<KeyEnvelope, ValueEnvelope, ValueEnvelope, ValueEnvelope, Context, Functions> KV;

        ClientSession<KeyEnvelope, ValueEnvelope, ValueEnvelope, ValueEnvelope, Context, Functions> KVSession;

        Context UnsafeContext;

        
    }
}

