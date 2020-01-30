using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using System.Text.Json;

namespace Json.NET.Fallback.Converter
{
    public static class Reflection
    {

        internal static void Get<TTArget, TPropety>(ref TTArget target, string propertyName, out TPropety propetyValue)
        {
            propetyValue = Cache<TTArget, TPropety>.Get(target, propertyName);
        }

        static class Cache<TTarget, TProperty>
        {
            static Cache()
            {
                Getters = new Dictionary<string, Func<TTarget, TProperty>>();
            }

            static Dictionary<string, Func<TTarget, TProperty>> Getters;

            internal static TProperty Get(TTarget instance, string propertyName)
            {
                if (!Getters.TryGetValue(propertyName, out Func<TTarget, TProperty> getter))
                    lock (Getters)
                        if (!Getters.TryGetValue(propertyName, out getter))
                            Getters.Add(propertyName, getter = CreateGetter(propertyName));
                return getter(instance);
            }

            public static Func<TTarget, TProperty> CreateGetter(string propertyName)
            {
                var targetType = typeof(TTarget);
                var propertyInfo = targetType.GetProperty(propertyName, typeof(TProperty));

                if (propertyInfo == null || !propertyInfo.CanRead)
                    throw new Exception($"Type must have a prop with name {propertyName}, returning {typeof(TProperty).Name}");

                var parameter = Expression.Parameter(propertyInfo.DeclaringType, "x");

                var property = Expression.Property(parameter, propertyInfo);

                var convert = Expression.ConvertChecked(property, typeof(TProperty));

                return (Func<TTarget, TProperty>)Expression.Lambda(convert, parameter).Compile();
            }
        }
    }

    
}
