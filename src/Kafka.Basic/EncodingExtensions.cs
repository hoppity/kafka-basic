using System.Text;

namespace Kafka.Basic
{
    internal static class EncodingExtensions
    {
        public static byte[] Encode(this string value)
        {
            return value == null ? null : Encoding.UTF8.GetBytes(value);
        }

        public static string Decode(this byte[] value)
        {
            return value == null ? null : Encoding.UTF8.GetString(value);
        }
    }
}