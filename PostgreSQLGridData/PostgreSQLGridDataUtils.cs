using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using SimioAPI;
using SimioAPI.Extensions;
using System.Net;
using System.IO;
using System.Xml;
using Npgsql;

namespace PostgreSQLGridData
{
    struct DbColumnInfo
    {
        public DbColumnInfo(string name, Type type, bool allowDbNull)
        {
            Name = name;
            Type = type;
            AllowDbNull = allowDbNull;
        }
        public string Name { get; }
        public Type Type { get; }
        public bool AllowDbNull { get; }
    }

    public static class PostgreSQLGridDataUtils
    {
        /// <summary>
        /// Examines the given DataSet to compute column information.
        /// </summary>
        internal static IEnumerable<DbColumnInfo> GetColumnInfoForTable(DataSet ds)
        {            
            foreach (DataColumn col in ds.Tables[0].Columns)
            {
                yield return new DbColumnInfo(col.ColumnName, col.DataType, col.AllowDBNull);
            }
        }

        /// <summary>
        /// Communicate with the database to get column information about the given table.
        /// No data is returned.
        /// </summary>
        internal static IEnumerable<DbColumnInfo> GetColumnInfoForTable(NpgsqlConnection connection, string tableName, Int32 connectionTimeout)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $"SELECT * FROM {tableName} WHERE 1 = 0";
                cmd.CommandTimeout = connectionTimeout;

                using (var da = new NpgsqlDataAdapter())
                {
                    da.SelectCommand = cmd;
                    using (var ds = new DataSet())
                    {
                        da.Fill(ds);
                        foreach (DataColumn col in ds.Tables[0].Columns)
                        {
                            yield return new DbColumnInfo(col.ColumnName, col.DataType, col.AllowDBNull);
                        }
                    }
                }
            } // using CreateCommand
        }

        /// <summary>
        /// Given a tableName, create a dataset from a SQL Table.
        /// </summary>
        /// <param name="sqlStatement"></param>
        /// <param name="useStoredProcedure"></param>
        /// <returns></returns>
        internal static DataSet GetDataSet(NpgsqlConnection connection, string sqlStatement, Int32 connectionTimeout)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = sqlStatement;
                cmd.CommandTimeout = connectionTimeout;
                using (var da = new NpgsqlDataAdapter())
                {
                    da.SelectCommand = cmd;
                    using (var ds = new DataSet())
                    {
                        da.Fill(ds);
                        return ds;
                    }
                }
            }
        }
    }
}
