using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;

namespace Tiny.Bson
{
	using BsonDecoder = Func<Type, object, object>;
	using BsonEncoder = Action<object, Stream>;	

	public static class BsonMapper
	{
		static BindingFlags defaultBindFlags =
			BindingFlags.Public
			| BindingFlags.NonPublic
			| BindingFlags.Instance
			| BindingFlags.DeclaredOnly;

		#region PUBLIC

		static BsonMapper()
		{
			RegisterDefaultBsonDecoder();
			RegisterDefaultBsonEncoder();
		}

		public static byte[] Encode(object obj)
		{
			Type type = obj.GetType();
			BsonEncoder encoder = GetBsonEncoder(type);
			MemoryStream memoryStream = new MemoryStream();
			encoder(obj, memoryStream);

			return memoryStream.GetBuffer();
		}

		public static T Decode<T>(byte[] byteArray)
		{
			Type type = typeof(T);
			IDictionary<string, object> bsonObj = DecodeDocument(byteArray);
			BsonDecoder decoder = GetDecoder(type);
			//return (T)decoder(type , )
			return (T)decoder(type, bsonObj);
		}

		#endregion PUBLIC

		#region Decode

		internal static BsonDecoder genericBsonDecoder = null;
		internal static Dictionary<Type, BsonDecoder> bsonDecoders
			= new Dictionary<Type, BsonDecoder>();

		public static void RegisterBsonDecoder<T>(BsonDecoder decoder)
		{
			Type type = typeof(T);
			if (type == typeof(object))
			{
				genericBsonDecoder = decoder;
			}
			else
			{
				bsonDecoders[type] = decoder;
			}
		}

		static void RegisterDefaultBsonDecoder()
		{
			RegisterBsonDecoder<object>((type, bsonObj) =>
			{
				object instance = Activator.CreateInstance(type, true);
				if (bsonObj is IDictionary)
				{
					foreach (DictionaryEntry item in (IDictionary)bsonObj)
					{
						string name = item.Key as string;
						if (!BsonMapper.DecodeValue(instance, name, item.Value))
						{
							Console.WriteLine("couldn't decode field \"" + name + "\" of " + type);
						}
					}
				}

				return instance;
			});

			RegisterBsonDecoder<IEnumerable>((type, bsonObject) => {
				if (typeof(IEnumerable).IsAssignableFrom(type))
				{
					if (bsonObject is IList)
					{
						IList bsonList = bsonObject as IList;
						int count = bsonList.Count;
						if (type.IsArray)
						{
							Type elementType = type.GetElementType();
							bool isNullable = Nullable.GetUnderlyingType(elementType) != null
								|| !elementType.IsPrimitive;
							Array array = Array.CreateInstance(elementType, count);
							for (int i = 0; i < count; i++)
							{
								object value = DecodeValue(bsonList[i], elementType);
								if (null != value || isNullable)
								{
									array.SetValue(value, i);
								}
							}
							return array;
						}
						else if (type.GetGenericArguments().Length == 1)
						{
							IList instance = null;
							Type genericType = type.GetGenericArguments()[0];
							bool nullable = Nullable.GetUnderlyingType(genericType) != null || !genericType.IsPrimitive;
							if(type!= typeof(IList) && typeof(IList).IsAssignableFrom(type))
							{
								instance = Activator.CreateInstance(type, true) as IList;
							}
							else
							{
								Type genericListType = typeof(List<>).MakeGenericType(genericType);
								instance = Activator.CreateInstance(genericListType) as IList;
							}

							foreach(var item in bsonList)
							{
								object value = DecodeValue(item, genericType);
								if(null != value && nullable)
								{
									instance.Add(value);
								}
							}

							return instance;
						}
					}
				}

				return null;
			});
		}

		static bool DecodeValue(object target, string name, object value)
		{
			Type targetType = target.GetType();
			while (null != targetType)
			{
				FieldInfo fieldInfo = targetType.GetField(name, defaultBindFlags);

				if (null == fieldInfo)
				{
					fieldInfo = targetType.GetField(WrapFieldName(name), defaultBindFlags);
				}

				if (null != fieldInfo
					&& null != value
					&& fieldInfo.GetCustomAttributes(typeof(System.NonSerializedAttribute), true).Length == 0)
				{
					Type fieldType = fieldInfo.FieldType;
					object fieldValue = DecodeValue(value, fieldType);
					if (null != fieldValue &&
						fieldInfo.FieldType.IsAssignableFrom(fieldValue.GetType()))
					{
						fieldInfo.SetValue(target, fieldValue);
						return true;
					}
					else
					{
						fieldInfo.SetValue(target, null);
					}

				}
				targetType = targetType.BaseType;
			}
			return false;
		}

		static object DecodeValue(object value, Type targetType)
		{
			if (value == null) return null;

			Type valueType = value.GetType();
			if (!targetType.IsAssignableFrom(valueType))
			{
				BsonDecoder decoder = GetDecoder(targetType);
				value = decoder(targetType, value);
			}

			if (null != value && targetType.IsAssignableFrom(value.GetType()))
			{
				return value;
			}
			else
			{
				Console.WriteLine("couldn't decode: " + targetType);
				return null;
			}
		}

		static BsonDecoder GetDecoder(Type type)
		{
			if (bsonDecoders.ContainsKey(type))
			{
				return bsonDecoders[type];
			}

			foreach (var entry in bsonDecoders)
			{
				Type baseType = entry.Key;
				if (baseType.IsAssignableFrom(type))
				{
					return entry.Value;
				}
			}
			return genericBsonDecoder;
		}

		static IDictionary<string, object> DecodeDocument(byte[] byteArray)
		{
			if (null != byteArray)
			{
				MemoryStream decodeStream = new MemoryStream(byteArray);
				BinaryReader reader = new BinaryReader(decodeStream);
				return DecodeDocument(reader);
			}
			else
			{
				return null;
			}
		}

		private static KeyValuePair<string, object> DecodeElement(BinaryReader reader)
		{
			string name = string.Empty;
			object value = null;
			byte elementType = reader.ReadByte();

			switch (elementType)
			{
				case 0x01:
					{
						// double
						name = GetDecodeName(reader);
						value = reader.ReadDouble();
						break;
					}
				case 0x02:
					{
						// String
						name = GetDecodeName(reader);
						value = GetString(reader);
						break;
					}
				case 0x03:
					{
						// Document
						name = GetDecodeName(reader);
						value = DecodeDocument(reader);
						break;
					}
				case 0x04:
					{
						// Array
						name = GetDecodeName(reader);
						// TODO : how to handle array.
						Dictionary<string, object> arrayDict = DecodeDocument(reader);
						int count = arrayDict.Count;
						object[] objArray = new object[count];
						for (int i = 0; i < count; i++)
						{
							string key = i.ToString();
							if (arrayDict.ContainsKey(key))
							{
								objArray[i] = arrayDict[key];
							}
						}
						value = objArray;
						break;
					}
				case 0x05:
					{
						// Binary
						name = GetDecodeName(reader);
						int length = reader.ReadInt32();
						// TODO : what the hell is type?
						byte binaryType = reader.ReadByte();
						value = reader.ReadBytes(length);
						break;
					}
				case 0x08:
					{
						// boolean
						name = GetDecodeName(reader);
						value = reader.ReadBoolean();
						break;
					}
				case 0x09:
					{
						// DateTime
						name = GetDecodeName(reader);
						long timeValue = reader.ReadInt64();
						value = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)
							+ new TimeSpan(timeValue * 10000);
						break;
					}
				case 0x0A:
					{
						// None
						name = GetDecodeName(reader);
						break;
					}
				case 0x10:
					{
						// Int32
						name = GetDecodeName(reader);
						value = reader.ReadInt32();
						break;
					}
				case 0x12:
					{
						// Int64
						name = GetDecodeName(reader);
						value = reader.ReadInt64();
						break;
					}
			}
			if (!string.IsNullOrEmpty(name))
			{
				return new KeyValuePair<string, object>(name, value);
			}
			else
			{
				throw new Exception(string.Format("Type:{0} has no name", elementType));
			}
		}

		private static string GetString(BinaryReader reader)
		{
			int length = reader.ReadInt32();
			byte[] buffer = reader.ReadBytes(length);
			return Encoding.UTF8.GetString(buffer);
		}

		private static string GetDecodeName(BinaryReader reader)
		{
			MemoryStream outStream = new MemoryStream();

			while (true)
			{
				byte buf = reader.ReadByte();
				if (buf == 0)
				{
					break;
				}
				outStream.WriteByte(buf);
			}

			return Encoding.UTF8.GetString(outStream.GetBuffer(), 0, (int)outStream.Position);
		}

		private static Dictionary<string, object> DecodeDocument(BinaryReader reader)
		{
			Dictionary<string, object> dict = new Dictionary<string, object>();
			int length = reader.ReadInt32() - 4;
			long endPos = reader.BaseStream.Position + length - 1;
			while (reader.BaseStream.Position < endPos)
			{
				KeyValuePair<string, object> pair = DecodeElement(reader);
				if (dict.ContainsKey(pair.Key))
				{
					dict[pair.Key] = pair.Value;
					Console.WriteLine(
						"Duplicate Key {0} -> {1}",
						pair.Key,
						pair.Value
						);
				}
				else
				{
					dict.Add(pair.Key, pair.Value);
				}
			}

			reader.ReadByte();	// 
			return dict;
		}


		#endregion Decode

		#region Encode

		internal static BsonEncoder genericBsonEncoder;
		internal static Dictionary<Type, BsonEncoder> bsonEncoders
			= new Dictionary<Type, BsonEncoder>();

		static BsonEncoder GetBsonEncoder(Type type)
		{
			if (bsonEncoders.ContainsKey(type))
			{
				return bsonEncoders[type];
			}
			else
			{
				return genericBsonEncoder;
			}
		}

		static void RegisterDefaultBsonEncoder()
		{
			RegisterBsonEncoder<object>((obj, stream) =>
			{
				Type type = obj.GetType();
				EncodeDocument(stream, obj);
			});

			RegisterBsonEncoder<IEnumerable>((obj, stream)=>
			{
				EncodeArray(obj as IEnumerable, stream);
			}
			);
		}

		static void RegisterBsonEncoder<T>(BsonEncoder bsonEncoder)
		{
			Type type = typeof(T);
			if (type == typeof(object))
			{
				genericBsonEncoder = bsonEncoder;
			}
			else
			{
				if (bsonEncoders.ContainsKey(type))
				{
					Console.WriteLine("{0} has already exist.", type.FullName);
					bsonEncoders[type] = bsonEncoder;
				}
				else
				{
					bsonEncoders.Add(type, bsonEncoder);
				}
			}
		}

		private static void EncodeField(Stream ms, FieldInfo fieldInfo, object obj)
		{
			string name = UnwrapFieldName(fieldInfo.Name);
			Type type = fieldInfo.FieldType;
			
			object value = fieldInfo.GetValue(obj);
			EncodeElement(ms, name, type, value);
		}

		private static void EncodeElement(Stream ms, string name, Type type, object value)
		{
			TypeCode typeCode = Type.GetTypeCode(type);
			switch (typeCode)
			{
				case TypeCode.Single:	// TODO : single is not in the specification.
				case TypeCode.Double:
					ms.WriteByte(0x01);
					encodeCString(ms, name);
					encodeDouble(ms, (double)System.Convert.ToDouble(value));
					return;
				case TypeCode.String:
					ms.WriteByte(0x02);
					encodeCString(ms, name);
					encodeString(ms, value as string);
					return;
				case TypeCode.Object:
					if (type == typeof(byte[]))
					{
						ms.WriteByte(0x05);
						encodeCString(ms, name);
						encodeBinary(ms, value as byte[]);
					}
					else if (type.IsArray || type.IsGenericType)
					{
						ms.WriteByte(0x04);
						encodeCString(ms, name);
						EncodeArray(value as IEnumerable, ms);
					}
					else
					{
						ms.WriteByte(0x03);
						encodeCString(ms, name);
						// TODO : encode object.
						EncodeDocument(ms, value);
					}
					return;
				case TypeCode.Boolean:
					ms.WriteByte(0x08);
					encodeCString(ms, name);
					encodeBool(ms, (bool)value);
					return;
				case TypeCode.DateTime:
					ms.WriteByte(0x09);
					encodeCString(ms, name);
					encodeUTCDateTime(ms, (DateTime)value);
					return;
				case TypeCode.Empty:
					ms.WriteByte(0x0A);
					encodeCString(ms, name);
					return;

				case TypeCode.Byte:	// TODO : Byte is not in the specification.
				case TypeCode.Int32:
					ms.WriteByte(0x10);
					encodeCString(ms, name);
					encodeInt32(ms, System.Convert.ToInt32(value));
					return;
				case TypeCode.Int64:
					ms.WriteByte(0x12);
					encodeCString(ms, name);
					encodeInt64(ms, (long)value);
					return;
				default:

					if (type.IsArray)
					{
						ms.WriteByte(0x04);
						encodeCString(ms, name);
						// TODO : encode array.
						//encodeArray(ms, value);
						return;
					}
					break;

			};
		}

		private static void EncodeArray(IEnumerable objs, Stream ms)
		{
			MemoryStream arrayStream = new MemoryStream();
			

			//for (int i = 0, length = array.Length; i < length; i++)
			int i = 0;
			foreach (var obj in objs)
			{
				if (null != obj)
				{
					Type objType = obj.GetType();
					EncodeElement(arrayStream, i.ToString(), objType, obj);
				}
				else
				{
					EncodeElement(arrayStream, i.ToString(), typeof(Nullable), null);
				}
				i++;
			}

			BinaryWriter bw = new BinaryWriter(ms);
			bw.Write((Int32)(arrayStream.Position + 4 + 1));
			bw.Write(arrayStream.GetBuffer(), 0, (int)arrayStream.Position);
			bw.Write((byte)0);
 
		}

		private static void EncodeDocument(Stream ms, object obj)
		{
			MemoryStream dms = new MemoryStream();

			Type type = obj.GetType();

			while (null != type)
			{
				FieldInfo[] fieldInfos = type.GetFields(defaultBindFlags);
				foreach (FieldInfo field in fieldInfos)
				{
					EncodeField(dms, field, obj);
				}
				type = type.BaseType;
			}

			BinaryWriter bw = new BinaryWriter(ms);
			bw.Write((Int32)(dms.Position + 4 + 1));
			bw.Write(dms.GetBuffer(), 0, (int)dms.Position);
			bw.Write((byte)0);
		}
		
		private static void encodeBinary(Stream ms, byte[] buf)
		{
			byte[] aBuf = BitConverter.GetBytes(buf.Length);
			ms.Write(aBuf, 0, aBuf.Length);
			ms.WriteByte(0);
			ms.Write(buf, 0, buf.Length);
		}

		private static void encodeCString(Stream ms, string v)
		{
			byte[] buf = new UTF8Encoding().GetBytes(v);
			ms.Write(buf, 0, buf.Length);
			ms.WriteByte(0);
		}

		private static void encodeString(Stream ms, string v)
		{
			byte[] strBuf = new UTF8Encoding().GetBytes(v);
			byte[] buf = BitConverter.GetBytes(strBuf.Length + 1);

			ms.Write(buf, 0, buf.Length);
			ms.Write(strBuf, 0, strBuf.Length);
			ms.WriteByte(0);
		}

		private static void encodeDouble(Stream ms, double v)
		{
			byte[] buf = BitConverter.GetBytes(v);
			ms.Write(buf, 0, buf.Length);
		}

		private static void encodeBool(Stream ms, bool v)
		{
			byte[] buf = BitConverter.GetBytes(v);
			ms.Write(buf, 0, buf.Length);
		}

		private static void encodeInt32(Stream ms, Int32 v)
		{
			byte[] buf = BitConverter.GetBytes(v);
			ms.Write(buf, 0, buf.Length);
		}

		private static void encodeInt64(Stream ms, Int64 v)
		{
			byte[] buf = BitConverter.GetBytes(v);
			ms.Write(buf, 0, buf.Length);
		}

		private static void encodeUTCDateTime(Stream ms, DateTime dt)
		{
			TimeSpan span;
			if (dt.Kind == DateTimeKind.Local)
			{
				span = (dt - new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).ToLocalTime());
			}
			else
			{
				span = dt - new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
			}
			byte[] buf = BitConverter.GetBytes((Int64)(span.TotalSeconds * 1000));
			ms.Write(buf, 0, buf.Length);
		}

		#endregion Encode

		#region FUNCTION

		static string WrapFieldName(string name)
		{
			return string.Format("<{0}>k__BackingField", name);
		}

		static string UnwrapFieldName(string name)
		{
			if (name.StartsWith("<") && name.Contains(">"))
			{
				return name.Substring(name.IndexOf("<") + 1, name.IndexOf(">") - 1);
			}
			return name;
		}

		#endregion FUNCTION
	}
}
