using System;

namespace Json.NET.Fallback
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Enum | AttributeTargets.Interface | AttributeTargets.Struct, AllowMultiple = false)]
    public sealed class JsonNETAttribute : Attribute
    {
    }
}
