using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hugo.Extensions;
using System.Data;
using Hugo.Core;
using System.Reflection;

namespace Hugo.Data.Sqlite
{
	public class SqliteRelationalStore<T> : RelationalStoreBase<T> where T : new()
	{
		public SqliteRelationalStore(IDbCore dbCore)
		  : base(dbCore)
		{
		}

		public SqliteRelationalStore(string connectionStringName)
		  : base(new SqliteDbCore(connectionStringName))
		{
		}

		public override List<T> TryLoadData()
		{
			string sql = string.Format("SELECT * FROM {0}", this.TableMapping.DelimitedTableName);
			var result = new List<T>();
			using (var dr = this.Database.OpenReader(sql))
			{
				while (dr.Read())
				{
					var newItem = this.MapReaderToObject<T>(dr);
					result.Add(newItem);
				}
			}
			return result;
		}

		public override IEnumerable<IDbCommand> CreateInsertCommands(IEnumerable<T> items)
		{
			var commands = new List<IDbCommand>();
			if (items.Count() > 0)
			{
				// Reserve serial ID's if specified;
				if (TableMapping.PrimaryKeyMapping[ 0 ].IsAutoIncementing)
				{
					ReserveAutoIdsForItems(items);
				}
				foreach (var item in items)
				{
					var properties = item.GetType().GetProperties();
					var itemEx = item.ToExpando();
					var itemSchema = itemEx as IDictionary<string, object>;

					var insertColumns = new List<string>();
					var args = new List<object>();
					var paramIndex = 0;
					var parameterPlaceholders = new List<string>();
					foreach (var property in properties)
					{
						var matchingColumn = this.TableMapping.ColumnMappings.FindByProperty(property.Name);
						if (matchingColumn != null)
						{
							insertColumns.Add(matchingColumn.DelimitedColumnName);
							args.Add(itemSchema[ property.Name ]);
							parameterPlaceholders.Add("@" + paramIndex++.ToString());
						}
					}
					string insertBase = "INSERT INTO {0} ({1}) VALUES ({2})";
					string sql = string.Format(
				insertBase,
				this.TableMapping.DelimitedTableName,
				string.Join(", ", insertColumns.ToArray()),
				string.Join(", ", parameterPlaceholders.ToArray())
			 );
					commands.Add(Database.BuildCommand(sql, args.ToArray()));
				}
			}
			return commands;
		}

		public override IEnumerable<IDbCommand> CreateUpdateCommands(IEnumerable<T> items)
		{
			var commands = new List<IDbCommand>();
			if (items.Count() > 0)
			{
				foreach (var item in items)
				{
					string ParameterAssignmentFormat = "{0} = @{1}";
					string updateSqlFormat = "UPDATE {0} SET {1} WHERE {2};";
					var results = new List<int>();
					var paramIndex = 0;
					var args = new List<object>();
					var sb = new StringBuilder();

					var ex = item.ToExpando();
					var dc = ex as IDictionary<string, object>;
					var setValueStatements = new List<string>();
					var pkColumnMapping = this.TableMapping.PrimaryKeyMapping[0];

					// Build the SET statements:
					foreach (var kvp in dc)
					{
						if (kvp.Key != pkColumnMapping.PropertyName)
						{
							args.Add(kvp.Value);
							string delimitedColumnName = this.TableMapping.ColumnMappings.FindByProperty(kvp.Key).ColumnName;
							string setItem = string.Format(ParameterAssignmentFormat, delimitedColumnName, paramIndex++.ToString());
							setValueStatements.Add(setItem);
						}
					}
					var pkValue = dc[pkColumnMapping.PropertyName];
					args.Add(pkValue);

					string commaDelimitedSetStatements = string.Join(",", setValueStatements);
					string whereCriteria = string.Format(ParameterAssignmentFormat, pkColumnMapping.DelimitedColumnName, paramIndex++.ToString());
					sb.AppendFormat(updateSqlFormat, this.TableMapping.DelimitedTableName, commaDelimitedSetStatements, whereCriteria);
					commands.Add(Database.BuildCommand(sb.ToString(), args.ToArray()));
				}
			}
			return commands;
		}

		public override IEnumerable<IDbCommand> CreateDeleteCommands(IEnumerable<T> items)
		{
			var commands = new List<IDbCommand>();
			if (items.Count() > 0)
			{
				string keyColumnNames;
				var keyList = new List<string>();

				// The first pk in the list is what we want, to build a standard IN statement
				// like this: DELETE FROM myTable WHERE pk1 IN (value1, value2, value3, value4, ...)
				keyColumnNames = this.TableMapping.PrimaryKeyMapping[ 0 ].DelimitedColumnName;
				foreach (var item in items)
				{
					var expando = item.ToExpando();
					var dict = (IDictionary<string, object>)expando;
					var pk = this.TableMapping.PrimaryKeyMapping[0];
					if (pk.DataType == typeof(string))
					{
						// Wrap in single quotes
						keyList.Add(string.Format("'{0}'", dict[ pk.PropertyName ].ToString()));
					}
					else
					{
						// Don't wrap:
						keyList.Add(dict[ pk.PropertyName ].ToString());
					}
				}
				string sqlFormat = "DELETE FROM {0} Where {1} ";
				var keySet = String.Join(",", keyList.ToArray());
				var inStatement = keyColumnNames + "IN (" + keySet + ")";
				string sql = string.Format(sqlFormat, this.TableMapping.DelimitedTableName, inStatement);
				commands.Add(Database.BuildCommand(sql));
			}
			return commands;
		}

		public override System.Data.IDbCommand CreateDeleteAllCommand()
		{
			string sql = string.Format("DELETE FROM {0}", this.TableMapping.DelimitedTableName);
			return Database.BuildCommand(sql);
		}

		public void ReserveAutoIdsForItems(IEnumerable<T> items)
		{
			int nextReservedId = 0;
			// Find the last inserted id value:
			string sqlLastVal = string.Format("SELECT seq FROM sqlite_sequence WHERE name = '{0}'", this.TableMapping.DBTableName);
			object val = null;
			using (var conn = Database.CreateConnection(Database.ConnectionString))
			{
				conn.Open();
				using (var transaction = conn.BeginTransaction())
				{
					using (var cmd = Database.BuildCommand(sqlLastVal))
					{
						cmd.Connection = conn;
						cmd.Transaction = transaction;
						try
						{
							val = cmd.ExecuteScalar();
						}
						catch (Exception ex)
						{
							if (ex.Message.Contains("sqlite_sequence"))
							{
								val = 0;
							}
							else
							{
								transaction.Rollback();
								conn.Close();
								throw ex;
							}
						}
					}
					int lastVal = val == null ? 0 : (int)Convert.ChangeType(val, typeof(int));
					nextReservedId = lastVal + 1;
					// Update the SQLite Sequence table:
					int qtyToAdd = items.Count();
					string sqlSeq = string.Format("UPDATE sqlite_sequence SET seq = {0} WHERE name = '{1}'", lastVal + qtyToAdd, this.TableMapping.DBTableName);

					using (var cmd = Database.BuildCommand(sqlSeq))
					{
						cmd.Connection = conn;
						cmd.Transaction = transaction;
						try
						{
							val = cmd.ExecuteNonQuery();
						}
						catch (Exception ex)
						{
							transaction.Rollback();
							conn.Close();
							throw ex;
						}
					}
					transaction.Commit();
				}
				conn.Close();
			}
			string autoPkPropertyName = "";
			if (TableMapping.PrimaryKeyMapping[ 0 ].IsAutoIncementing)
			{
				autoPkPropertyName = TableMapping.PrimaryKeyMapping[ 0 ].PropertyName;
			}
			foreach (var item in items)
			{
				if (this.KeyIsAutoIncrementing)
				{
					var pkProperty = item.GetType().GetProperties().FirstOrDefault(p => p.Name == autoPkPropertyName);
					pkProperty.SetValue(item, nextReservedId, null);
					nextReservedId++;
				}
			}
		}

	}
}