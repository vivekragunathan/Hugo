using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using Hugo.Data.Json;
using System.Reflection;
using System.IO;

namespace Hugo.App
{
	public class Program
	{
		private const uint NO_OF_CLIENTS = 1000;


		private static IEnumerable<Client> CreateClients(uint count)
		{
			for (int i = 0; i < count; i++)
			{
				yield return new Client
				{
					ClientId = i,
					FirstName = "First Name " + i,
					LastName = "Last Name " + i,
					Email = "sender" + i + "@Example.com"
				};
			}
		}

		public static void Main(string[] args)
		{
			var jsonDbDirPath = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
			Console.WriteLine($"JSON Store Dir: {jsonDbDirPath}");

			var sw = new Stopwatch();

			sw.Start();
			var itemStore = new JsonStore<Client>(new JsonDbCore(jsonDbDirPath, "hugo"));
			sw.Stop();
			Console.WriteLine("Initialized new Json store in {0} ms", sw.ElapsedMilliseconds);
			itemStore.DeleteAll();

			var clients    = CreateClients(NO_OF_CLIENTS);
			var singleItem = clients.ElementAt(0);

			sw.Reset();
			sw.Start();
			itemStore.Add(singleItem);
			sw.Stop();
			Console.WriteLine("Inserted single item in {0} ms", sw.ElapsedMilliseconds);

			int savedId = singleItem.ClientId;

			sw.Reset();
			sw.Start();
			var savedItems = itemStore.TryLoadData();
			sw.Stop();
			Console.WriteLine("Loaded {0} items in {1} ms", savedItems.Count, sw.ElapsedMilliseconds);

			singleItem = savedItems.FirstOrDefault(itm => itm.ClientId == savedId);

			singleItem.LastName = "Updated Last";
			singleItem.FirstName = "Updated First";
			singleItem.Email = "updated@example.com";

			sw.Reset();
			sw.Start();
			itemStore.Update(singleItem);
			sw.Stop();
			Console.WriteLine("Updated single item in {0} ms", sw.ElapsedMilliseconds);

			var notReferenceEqualItem = new Client
			{
				ClientId = singleItem.ClientId,
				FirstName = "Copy First",
				LastName = "Copy Last",
				Email = "Copy Email"
			};

			sw.Reset();
			sw.Start();
			itemStore.Update(notReferenceEqualItem);
			sw.Stop();
			Console.WriteLine("Updated copy of single item in {0} ms", sw.ElapsedMilliseconds);

			sw.Reset();
			sw.Start();
			savedItems = itemStore.TryLoadData();
			sw.Stop();
			Console.WriteLine("Loaded {0} items in {1} ms", savedItems.Count, sw.ElapsedMilliseconds);

			singleItem = savedItems.FirstOrDefault(itm => itm.ClientId == savedId);

			sw.Reset();
			sw.Start();
			itemStore.Delete(singleItem);
			sw.Stop();
			Console.WriteLine("Deleted single item in {0} ms", sw.ElapsedMilliseconds);

			sw.Reset();
			sw.Start();
			itemStore.Add(clients);
			sw.Stop();
			Console.WriteLine("Inserted {0} item in {1} ms", savedItems.Count, sw.ElapsedMilliseconds);

			sw.Reset();
			sw.Start();
			savedItems = itemStore.TryLoadData();
			sw.Stop();
			Console.WriteLine("Loaded {0} items in {1} ms", savedItems.Count, sw.ElapsedMilliseconds);

			foreach (var item in savedItems)
			{
				int itemIndex = savedItems.IndexOf(item);
				item.LastName = string.Format("Updated Last {0}", itemIndex);
				item.FirstName = string.Format("Updated First {0}", itemIndex);
				item.Email = string.Format("Updated email {0}", itemIndex);
			}

			sw.Reset();
			sw.Start();
			itemStore.Update(savedItems);
			sw.Stop();
			Console.WriteLine("Updated {0} items in {1} ms", savedItems.Count, sw.ElapsedMilliseconds);

			sw.Reset();
			sw.Start();
			savedItems = itemStore.TryLoadData();
			sw.Stop();
			Console.WriteLine("Loaded {0} updated items in {1} ms", savedItems.Count, sw.ElapsedMilliseconds);

			sw.Reset();
			sw.Start();
			itemStore.Delete(savedItems);
			sw.Stop();
			Console.WriteLine("Deleted {0} updated items in {1} ms", savedItems.Count, sw.ElapsedMilliseconds);

			Console.Read();
		}
	}

}
