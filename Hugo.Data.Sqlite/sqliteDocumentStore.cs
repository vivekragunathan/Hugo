using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Text;
using Hugo.Core;
using Newtonsoft.Json;
using System.Data;

namespace Hugo.Data.Sqlite {
  public class SqliteDocumentStore<T> : DocumentStoreBase<T> where T : new() {

    public SqliteDocumentStore()
      : base(new SqliteDbCore("data.db")) { }

    public SqliteDocumentStore(string tableName)
      : base(new SqliteDbCore("data.db"), tableName) { }

    public SqliteDocumentStore(SqliteDbCore dbCore) 
      : base(dbCore) { }

    public SqliteDocumentStore(SqliteDbCore dbCore, string tableName) 
      : base(dbCore, tableName) { }

    public override List<T> TryLoadData() {
      var result = new List<T>();
      var tableName = DecideTableName();
      try {
        var sql = "SELECT * FROM " + tableName;
        var data = Database.ExecuteDynamic(sql);
        //hopefully we have data
        foreach (var item in data) {
          //pull out the JSON
          var deserialized = JsonConvert.DeserializeObject<T>(item.body);
          result.Add(deserialized);
        }
      }
      catch (Exception x) {
        if (x.Message.Contains("no such table")) {
          var sql = this.GetCreateTableSql();
          // Return value for CREATE TABLE in SQLite is 0, always:
          Database.TransactDDL(Database.BuildCommand(sql));
          if (!Database.TableExists(this.TableName)) {
            throw new Exception("Document table not created");
          }
          TryLoadData();
        } else {
          throw;
        }
      }
      return result;
    }

    protected override string GetCreateTableSql() {
      string tableName = this.DecideTableName();
      string pkName = this.GetKeyName();
      Type keyType = this.GetKeyType();
      bool isAuto = this.DecideKeyIsAutoIncrementing();

      string pkTypeStatement = "INTEGER PRIMARY KEY AUTOINCREMENT";
      string noRowId = "";
      if (!isAuto) {
        pkTypeStatement = "INT PRIMARY KEY";
        //noRowId = "WITHOUT ROWID";
      }
      if (keyType == typeof(string) || keyType == typeof(Guid)) {
        pkTypeStatement = "text primary key";
        // noRowId = "WITHOUT ROWID";
      }

      string sqlformat = @"CREATE TABLE {0} (id {1}, body TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP) {2}";
      return string.Format(sqlformat, tableName, pkTypeStatement, noRowId);
    }

    public override IEnumerable<IDbCommand> CreateInsertCommands(IEnumerable<T> items) {
      var commands = new List<IDbCommand>();
      if(items.Count() > 0) { 
        int nextReservedId = 0;
        if (this.KeyIsAutoIncrementing) {
          // We need to do this in order to keep the serialized Id in the JSON in sync with the relational record Id:

          // Find the last inserted id value:
          string sqlLastVal = string.Format("SELECT seq FROM sqlite_sequence WHERE name = '{0}'", this.TableName);
          object val = Database.ExecuteScalar(sqlLastVal);
          int lastVal = val == null ? 0 : (int)Convert.ChangeType(Database.ExecuteScalar(sqlLastVal), typeof(int));
          nextReservedId = lastVal + 1;

          // Update the SQLite Sequence table:
          int qtyToAdd = items.Count();
          string sqlSeq = string.Format("UPDATE sqlite_sequence SET seq = {0} WHERE name = '{1}'", lastVal + qtyToAdd, this.TableName);
          Database.Transact(sqlSeq);
        }
        string sqlFormat = "INSERT INTO {0} (id, body) VALUES ({1});";
        foreach (var item in items) {
          var args = new List<object>();
          var paramIndex = 0;

          // Set the next Id for each object:
          if (this.KeyIsAutoIncrementing) {
            this.SetKeyValue(item, nextReservedId);
          }
          var ex = this.SetDataForDocument(item);
          var itemAsDictionary = ex as IDictionary<string, object>;
          var parameterPlaceholders = new List<string>();

          // Gather paramter values and placeholders:
          foreach (var kvp in itemAsDictionary) {
            args.Add(kvp.Value);
            parameterPlaceholders.Add("@" + paramIndex++.ToString());
          }
          string commaDelimitedParameters = string.Join(",", parameterPlaceholders);
          nextReservedId++;

          string sql = string.Format(sqlFormat, TableName, commaDelimitedParameters);
          commands.Add(Database.BuildCommand(sql, args.ToArray()));
        }
      }
      return commands;
    }

    public override IEnumerable<IDbCommand> CreateUpdateCommands(IEnumerable<T> items) {
      var commands = new List<IDbCommand>();
      if (items.Count() > 0) {
        string ParameterAssignmentFormat = "{0} = @{1}";
        string sqlFormat = "UPDATE {0} SET {1} WHERE {2};";
        foreach (var item in items) {
          var paramIndex = 0;
          var args = new List<object>();

          var ex = this.SetDataForDocument(item);
          var dc = ex as IDictionary<string, object>;
          var setValueStatements = new List<string>();

          // Build the SET Statements:
          foreach (var kvp in dc) {
            if (kvp.Key != this.KeyName) {
              args.Add(kvp.Value);
              string setItem = string.Format(ParameterAssignmentFormat, kvp.Key, paramIndex++.ToString());
              setValueStatements.Add(setItem);
            }
          }
          args.Add(this.GetKeyValue(item));
          string commaDelimitedSetStatements = string.Join(",", setValueStatements);
          string whereCriteria = string.Format(ParameterAssignmentFormat, "id", paramIndex++.ToString());
          string updateSql = string.Format(sqlFormat, this.TableName, commaDelimitedSetStatements, whereCriteria);
          commands.Add(Database.BuildCommand(updateSql, args.ToArray()));
        }
      }
      return commands;
    }

    public override IEnumerable<IDbCommand> CreateDeleteCommands(IEnumerable<T> items) {
      var commands = new List<IDbCommand>();
      if (items.Count() > 0) {
        var args = new List<object>();
        var parameterPlaceholders = new List<string>();
        var paramIndex = 0;
        string sqlFormat = ""
          + "DELETE FROM {0} WHERE id in({1})";
        foreach (var item in items) {
          args.Add(this.GetKeyValue(item));
          parameterPlaceholders.Add("@" + paramIndex++.ToString());
        }
        var sql = string.Format(sqlFormat, this.TableName, string.Join(",", parameterPlaceholders));
        commands.Add(Database.BuildCommand(sql, args.ToArray()));
      }
      return commands;
    }

    public override IDbCommand CreateDeleteAllCommand() {
      string sql = string.Format("DELETE FROM {0}", this.TableName);
      return Database.BuildCommand(sql);
    }
  }
}