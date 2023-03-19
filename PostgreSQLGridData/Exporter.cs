using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Data;
using System.Globalization;
using SimioAPI.Extensions;
using System.Text.RegularExpressions;
using Npgsql;

using System.Web;
using NpgsqlTypes;

namespace PostgreSQLGridData
{
    public class ExporterDefinition : IGridDataExporterDefinition
    {
        public string Name => "PostgreSQL Data Exporter";
        public string Description => "An exporter to PostgreSQL";
        public Image Icon => null;

        static readonly Guid MY_ID = new Guid("5e59e505-8997-4f49-948a-e304344d65d6");
        public Guid UniqueID => MY_ID;

        public IGridDataExporter CreateInstance(IGridDataExporterContext context)
        {
            return new Exporter(context);
        }

        public void DefineSchema(IGridDataSchema schema)
        {
            var connectionStringProp = schema.OverallProperties.AddStringProperty("ConnectionString");
            connectionStringProp.DisplayName = "Connection String";
            connectionStringProp.Description = "Database Connection String.";
            connectionStringProp.DefaultValue = "Server=localhost;Username=postgres;Password=;Database=postgres";

            var connectionTimeOutProp = schema.OverallProperties.AddStringProperty("ConnectionTimeOut");
            connectionTimeOutProp.DisplayName = "Connection TimeOut (seconds)";
            connectionTimeOutProp.Description = "Connection TimeOut in Seconds.";
            connectionTimeOutProp.DefaultValue = "30";

            var dateTimeFormatProp = schema.OverallProperties.AddStringProperty("DateTimeFormat");
            dateTimeFormatProp.DisplayName = "DateTime Format";
            dateTimeFormatProp.Description = "DateTime Format Used To Save To Database (e.g. yyyy-MM-dd HH:mm:ss).  String value need to be defined.";
            dateTimeFormatProp.DefaultValue = "yyyy-MM-dd HH:mm:ss";

            var dataBaseTableNameProp = schema.PerTableProperties.AddStringProperty("DatabaseTableName");
            dataBaseTableNameProp.DisplayName = "Database Table Name";
            dataBaseTableNameProp.Description = "The Database table to write to";
            dataBaseTableNameProp.DefaultValue = String.Empty;

            var enabledTableExportProp = schema.PerTableProperties.AddBooleanProperty("EnableTableExport");
            enabledTableExportProp.DisplayName = "Enabled Table Export";
            enabledTableExportProp.Description = "If true, this table will be exported. If false, it will not.";
            enabledTableExportProp.DefaultValue = true;

            string[] exportTypes = ExportTypeStrings.ValidValues.Select(vv => vv.Item1).ToArray();
            var dataExportTypeProp = schema.PerTableProperties.AddListProperty("DataExportType", exportTypes);
            dataExportTypeProp.DisplayName = "Data Export Type";
            dataExportTypeProp.Description = "Type Of Data Export";
            dataExportTypeProp.DefaultValue = "Truncate And Repopulate";

            var preSaveSPProp = schema.PerTableProperties.AddStringProperty("PreSaveStoredProcedure");
            preSaveSPProp.DisplayName = "Pre Save Stored Procedure";
            preSaveSPProp.Description = "Stored Procedure Called Prior To Export";
            preSaveSPProp.DefaultValue = String.Empty;

            var portSaveSPProp = schema.PerTableProperties.AddStringProperty("PostSaveStoredProcedure");
            portSaveSPProp.DisplayName = "Post Save Stored Procedure";
            portSaveSPProp.Description = "Stored Procedure Called After Export";
            portSaveSPProp.DefaultValue = String.Empty;

            var enabledColumnExportProp = schema.PerColumnProperties.AddBooleanProperty("EnableColumnExport");
            enabledColumnExportProp.DisplayName = "Enabled Column Export";
            enabledColumnExportProp.Description = "If true, this column will be exported. If false, it will not.";
            enabledColumnExportProp.DefaultValue = true;
        }
    }

    enum ExportType
    {
        Invalid = -1,
        DropCreateAndRepopulate = 0,
        TruncateAndRepopulate = 1,
        UpdateAndInsert = 2,
        UpdateInsertAndDelete = 3,
        Insert = 4
    }

    // Class just to map the user-readable string representations of the ExportType values to the actual values
    static class ExportTypeStrings
    {
        static string SpaceOutCamelCase(string camelCaseString) => Regex.Replace(camelCaseString, "([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))", "$1 ");
        public static Tuple<string, ExportType>[] ValidValues { get; } = Enumerable.Range(0, 5) // Do not include Invalid
            .Select(i => (ExportType)i)
            .Select(e => Tuple.Create(SpaceOutCamelCase(e.ToString()), e))
            .ToArray();
    }

    class Exporter : IGridDataExporter
    {
        public Exporter(IGridDataExporterContext context)
        {
        }

        private object _sync = new object();

        public OpenExportDataResult OpenData(IGridDataOpenExportDataContext openContext)
        {
            try
            {
                GetValues(openContext.GridDataName, openContext.Settings, out var connectionString, out var connectionTimeOut, out var dateTimeFormat, out var databaseTableName, out var enableTableExport, out var dataExportType, out var preSaveStoredProcedure, out var postSaveStoredProcedure);

                if (enableTableExport)
                {
                    if (String.IsNullOrWhiteSpace(connectionString))
                        return OpenExportDataResult.Failed("The Connection String parameter is not specified");

                    if (connectionTimeOut <= 0)
                        return OpenExportDataResult.Failed("The Connection TimeOut parameter needs to be greater than zero");

                    if (String.IsNullOrWhiteSpace(dateTimeFormat))
                        return OpenExportDataResult.Failed("The DateTime Format parameter is not specified");

                    if (String.IsNullOrWhiteSpace(databaseTableName))
                        return OpenExportDataResult.Failed("The Database Table Name parameter is not specified");

                    if (String.IsNullOrWhiteSpace(dataExportType))
                        return OpenExportDataResult.Failed("The Data Export Type parameter is not specified");

                    ExportType exportType = ExportType.Invalid;
                    foreach (var vv in ExportTypeStrings.ValidValues)
                    {
                        if (vv.Item1 == dataExportType)
                        {
                            exportType = vv.Item2;
                            break;
                        }
                    }

                    if (exportType == ExportType.Invalid)
                        return OpenExportDataResult.Failed("Invalid Data Export Type parameter specified");

                    // Lock around this to allow only one thread to export using the exporter at a time, in cases like creating a table if it doesn't exist or whatnot.
                    //  This supposes most use cases will have a single connection string per exporter. If exporters share a connection string, this might not entirely 
                    //  work, and we might need to sync a different way.
                    lock (_sync)
                    {
                        using (var ExporterConnection = new ExporterConnection(connectionString, connectionTimeOut, dateTimeFormat))
                        {
                            SaveExportContextToDatabaseTable(ExporterConnection, ExporterConnection.DateTimeFormat, openContext, databaseTableName, exportType,
                                preSaveStoredProcedure, postSaveStoredProcedure);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                return OpenExportDataResult.Failed(string.Format("{0}", e.Message));
            }

            return OpenExportDataResult.Succeeded();
        }

        public string GetDataSummary(IGridDataSummaryContext context)
        {
            if (context == null)
                return null;

            GetValues(context.GridDataName, context.Settings, out _, out _, out _, out var databaseTableName, out _, out _, out _, out _);

            if (databaseTableName == null)
                return null;

            return String.Format("Exporting to ProsgreSQL : {0} table", databaseTableName);
        }

        private static void GetValues(string tableName, IGridDataOverallSettings settings, out string connectionString, out int connectionTimeOut, out string dateTimeFormat, out string databaseTableName, out bool enableTableExport, out string dataExportType, out string preSaveStoredProcedure, out string postSaveStoredProcedure)
        {
            connectionString = (string)settings.Properties["ConnectionString"].Value;
            connectionTimeOut = Convert.ToInt32(settings.Properties["ConnectionTimeOut"].Value);
            dateTimeFormat = (string)settings.Properties["DateTimeFormat"]?.Value;
            databaseTableName = (string)settings.GridDataSettings[tableName]?.Properties["DatabaseTableName"]?.Value;
            enableTableExport = (bool)settings.GridDataSettings[tableName]?.Properties["EnableTableExport"].Value;
            dataExportType = (string)settings.GridDataSettings[tableName]?.Properties["DataExportType"]?.Value;
            preSaveStoredProcedure = (string)settings.GridDataSettings[tableName]?.Properties["PreSaveStoredProcedure"]?.Value;
            postSaveStoredProcedure = (string)settings.GridDataSettings[tableName]?.Properties["PostSaveStoredProcedure"]?.Value;
        }

        public void Dispose()
        {
        }

        public static string BuildSqlCreateCommandFromExportContext(ExporterConnection exporterConnection, IGridDataOpenExportDataContext exportContext, bool[] columnsEnabledExport, string databaseTableName)
        {
            try
            {
                bool firstColumn = true;
                int colIdx = 0;
                string sqlCreate = $"CREATE TABLE {databaseTableName} (";
                // Add Property Columns
                foreach (var col in exportContext.Records.Columns)
                {
                    if (columnsEnabledExport[colIdx])
                    {
                        if (firstColumn == false)
                        {
                            sqlCreate += ", ";
                        }
                        else
                        {
                            firstColumn = false;
                        }

                        if (col.IsKey)
                        {
                            sqlCreate += $"{col.Name} {GetExportDataColumnType(exporterConnection, col, col.IsKey)} not null primary key";                            
                        }
                        else
                        {
                            if (col.DefaultValue != null && col.DefaultString.Length > 0)
                            {
                                DbColumnInfo dbColumnInfo = new DbColumnInfo(col.Name, col.Type, true);
                                string formattedValue = GetFormattedStringValue(exporterConnection, col.DefaultString, null, dbColumnInfo);
                                
                                sqlCreate += $"{col.Name} {GetExportDataColumnType(exporterConnection, col, col.IsKey)} Default '{formattedValue}'";
                            }
                            else
                            {
                                sqlCreate += $"{col.Name} {GetExportDataColumnType(exporterConnection, col, col.IsKey)}";                                
                            }
                        }
                    }
                    colIdx++;
                }

                if (firstColumn == true)
                    throw new Exception("No Columns Available To Create Table");

                sqlCreate += ")";

                return sqlCreate;
            }
            catch (Exception ex)
            {
                throw new ApplicationException($"Cannot build SQL CREATE command. Err={ex}");
            }
        }

        public static void SaveExportContextToDatabaseTable(ExporterConnection exporterConnection, String dateTimeFormat, IGridDataOpenExportDataContext exportContext, string databaseTableName,
        ExportType exportType, string preSaveStoredProcedure, string postSaveStoredProcedure)
        {
            try
            {
                string strCheckTable = "";
                string schemaName = "";
                // make name lower case
                databaseTableName = databaseTableName.ToLowerInvariant();
                string[] names = databaseTableName.Split('.');
                if (names.Length > 1)
                {
                    schemaName = names[0];
                    strCheckTable = $"SELECT EXISTS ( SELECT FROM pg_tables WHERE  schemaname = '{schemaName}' AND tablename  = '{names[1]}' );";
                }
                else
                {
                    throw new Exception("Include Schema with Table Name (e.g. SchemaName.TableName)");
                }

                var tableFound = false;
                using (var cmd = exporterConnection.Connection.CreateCommand())
                {
                    // find table
                    cmd.CommandText = strCheckTable;
                    cmd.CommandType = CommandType.Text;
                    tableFound = Convert.ToBoolean(cmd.ExecuteScalar());
                }

                if (exportType == ExportType.DropCreateAndRepopulate && tableFound == true)
                {
                    using (var cmd = exporterConnection.Connection.CreateCommand())
                    {
                        string sqlDrop = $"DROP TABLE {databaseTableName}";
                        cmd.CommandText = sqlDrop;
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandTimeout = exporterConnection.ConnectionTimeOut;
                        cmd.ExecuteNonQuery();
                    }
                    tableFound = false;
                }
                else if (exportType == ExportType.TruncateAndRepopulate && tableFound == true)
                {
                    using (var cmd = exporterConnection.Connection.CreateCommand())
                    {
                        string sqlTrunc = $"TRUNCATE TABLE {databaseTableName}";
                        cmd.CommandText = sqlTrunc;
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandTimeout = exporterConnection.ConnectionTimeOut;
                        cmd.ExecuteNonQuery();
                    }
                }

                // Get the column settings for the grid data we are currently exporting
                var columnSettings = exportContext.Settings.GridDataSettings[exportContext.GridDataName].ColumnSettings;

                // Create a boolean array matched up to the columns, indicating if they should be exported
                var columnsEnabledExport = exportContext.Records.Columns
                    .Select(c => (bool)columnSettings[c.Name].Properties["EnableColumnExport"].Value)
                    .ToArray();

                // If the SQL Table does not exist, then create it from the Simio Table
                if (tableFound == false)
                {
                    using (var cmd = exporterConnection.Connection.CreateCommand())
                    {
                        string sqlCreateCommand = BuildSqlCreateCommandFromExportContext(exporterConnection, exportContext, columnsEnabledExport, databaseTableName);
                        cmd.CommandText = sqlCreateCommand;
                        cmd.CommandTimeout = exporterConnection.ConnectionTimeOut;
                        cmd.ExecuteNonQuery();
                    }
                }

                // call PreSaveStoredProcedure
                if (preSaveStoredProcedure != null && preSaveStoredProcedure.Length > 0)
                {
                    using (var cmd = exporterConnection.Connection.CreateCommand())
                    {
                        cmd.CommandText = preSaveStoredProcedure;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = exporterConnection.ConnectionTimeOut;
                        cmd.ExecuteNonQuery();
                    }
                }

                // get db column names...These are retrieved from database in the GetColumnInfoForTable method
                List<DbColumnInfo> sqlColumnInfoList = new List<DbColumnInfo>(PostgreSQLGridDataUtils.GetColumnInfoForTable(exporterConnection.Connection,
                    databaseTableName, exporterConnection.ConnectionTimeOut));

                // make sure simio tables and datababase tables align
                CheckExportContextColumnsAgainstDatabaseColumns(exportContext, columnsEnabledExport, sqlColumnInfoList);

                // get table
                var dt = ConvertExportContextToDataTable(exporterConnection, exportContext, columnsEnabledExport, sqlColumnInfoList);

                // Save Data
                string exceptionMessage = String.Empty;
                SaveData(exporterConnection, exportContext, dt, schemaName, databaseTableName, exportType, tableFound, out exceptionMessage);
                if (exceptionMessage.Length > 0)
                {
                    throw new Exception(exceptionMessage);
                }

                // call PostSaveStoredProcedure
                if (postSaveStoredProcedure != null && postSaveStoredProcedure.Length > 0)
                {
                    using (var cmd = exporterConnection.Connection.CreateCommand())
                    {
                        cmd.CommandText = postSaveStoredProcedure;
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = exporterConnection.ConnectionTimeOut;
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex) when (ex.Message.Contains("There is already an open DataReader associated with this Command which must be closed first"))
            {
                throw new ApplicationException($"There was a problem exporting. You may need to add 'MultipleActiveResultSets=True' to your connection string. Table={databaseTableName} Err={ex}");
            }
            catch (Exception ex)
            {
                throw new ApplicationException($"There was a problem exporting. Table={databaseTableName} Err={ex}");
            }
        }

        public static void SaveData(ExporterConnection exporterConnection, IGridDataOpenExportDataContext exportContext, DataTable dt, string schemaName,
        string databaseTableName, ExportType exportType, bool tableFound, out string exceptionMessage)
        {
            exceptionMessage = String.Empty;

            var guidStr = Guid.NewGuid().ToString("N");
            string tempTableName = "tmptable" + guidStr;
            tempTableName = tempTableName.ToLowerInvariant();

            GetSQLParts(exporterConnection, exportContext, dt, databaseTableName, tempTableName,
                out var keyColumnName, out var updateSQL, out var insertSQL, out var valuesSQL);

            bool tempCreated = false;
            try
            {
                if (keyColumnName.Length == 0 && (exportType == ExportType.UpdateAndInsert || exportType == ExportType.UpdateInsertAndDelete))
                    throw new Exception("Key column not found in table.  Either add a key column or use Truncate or Drop Options Instead");

                if (updateSQL.Length == 0 && (exportType == ExportType.UpdateAndInsert || exportType == ExportType.UpdateInsertAndDelete))
                    throw new Exception("Table has no columns other than the key column to update.  Use Insert, Truncate or Drop Options Instead");

                CreateTemporaryTable(exporterConnection, databaseTableName, tempTableName, insertSQL);
                tempCreated = true;

                CopyDataToTable(exporterConnection, dt, insertSQL, tempTableName);

                WriteDataToFinalTable(exporterConnection, databaseTableName, exportType, keyColumnName, updateSQL, insertSQL, valuesSQL, tempTableName);
            }
            catch (Exception ex)
            {
                exceptionMessage = ex.Message;
            }
            finally
            {
                // Drop Temp Table
                if (tempCreated)
                {
                    using (var cmd = exporterConnection.Connection.CreateCommand())
                    {
                        string dropSQL = $"DROP TABLE {tempTableName}";
                        cmd.CommandText = dropSQL;
                        cmd.CommandTimeout = exporterConnection.ConnectionTimeOut;
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        private static void GetSQLParts(ExporterConnection exporterConnection, IGridDataOpenExportDataContext exportContext, DataTable dt, string databaseTableName, string tempTableName,
            out string keyColumnName, out string updateSQL, out string insertSQL, out string valuesSQL)
        {
            keyColumnName = string.Empty;
            updateSQL = string.Empty;
            insertSQL = string.Empty;
            valuesSQL = string.Empty;

            // Add Property Columns
            bool firstColumn = true;
            bool firstUpdateColumn = true;
            foreach (DataColumn dataColumn in dt.Columns)
            {
                foreach (var col in exportContext.Records.Columns)
                {
                    if (dataColumn.ColumnName.ToLowerInvariant() == col.Name.ToLowerInvariant())
                    {
                        if (firstColumn == false)
                        {
                            insertSQL += ", ";
                            valuesSQL += ", ";
                        }
                        else
                        {
                            firstColumn = false;
                        }

                        insertSQL += $"{col.Name}";
                        valuesSQL += $"{tempTableName}.{col.Name}";
                        
                        if (col.IsKey)
                        {
                            keyColumnName = col.Name;
                        }
                        else
                        {
                            if (firstUpdateColumn == false)
                            {
                                updateSQL += ", ";
                            }
                            else
                            {
                                firstUpdateColumn = false;
                            }

                            updateSQL += $"{col.Name} = {tempTableName}.{col.Name}";

                        }
                        break;
                    } // If Simio and SQL column names match
                } // Foreach DataColumn
            } // foreach Simio table column
        }

        private static void CreateTemporaryTable(ExporterConnection exporterConnection, string databaseTableName, string tempTableName, string insertSQL)
        {
            string sqlCreateTemp;
            sqlCreateTemp = $"CREATE TEMP TABLE {tempTableName} AS SELECT {insertSQL} FROM {databaseTableName} where 1 = 0";
           
            using (var cmd = exporterConnection.Connection.CreateCommand())
            {
                cmd.CommandText = sqlCreateTemp;
                cmd.CommandTimeout = exporterConnection.ConnectionTimeOut;
                cmd.ExecuteNonQuery();
            }
        }

        private static void CopyDataToTable(ExporterConnection exporterConnection, DataTable dt, string insertSQL, string tableName)
        {
            string tableNameAndColumns = tableName + "(";

            if (dt.Rows.Count > 0)
            {
                int colIdx = 0;
                foreach (DataColumn dataColumn in dt.Columns)
                {
                    if (colIdx == 0) tableNameAndColumns += dataColumn.ColumnName;
                    else tableNameAndColumns += "," + dataColumn.ColumnName;
                    colIdx++;
                }
                tableNameAndColumns += ")";

                using (var writer = exporterConnection.Connection.BeginBinaryImport($"COPY {tableNameAndColumns} FROM STDIN(Format BINARY)"))
                {
                    foreach (DataRow dataRow in dt.Rows)
                    {
                        writer.StartRow();
                        foreach (DataColumn dataColumn in dt.Columns)
                        {
                            var dataValue = dataRow[dataColumn];
                            if ((dataValue == null || Convert.ToString(dataValue).Length == 0) && dataColumn.AllowDBNull)
                            {
                                writer.WriteNull();
                            }
                            else
                            {
                                if (dataColumn.DataType.IsPrimitive) // int, bool, double, float, etc...
                                {
                                    object typedValue = null;
                                    if (dataValue == null)
                                    {
                                        // No data value given, however we failed the AllowDBNull check above, so 
                                        //  no null values allowed, just give them the default vaue for the type
                                        typedValue = Activator.CreateInstance(dataColumn.DataType);
                                    }
                                    else
                                    {
                                        typedValue = Convert.ChangeType(dataValue, dataColumn.DataType, CultureInfo.InvariantCulture);
                                    }

                                    if (dataColumn.DataType == typeof(bool)) writer.Write(dataValue, NpgsqlDbType.Boolean);
                                    else if (dataColumn.DataType == typeof(Single)) writer.Write(dataValue, NpgsqlDbType.Real);
                                    else if (dataColumn.DataType == typeof(double) || dataColumn.DataType == typeof(decimal)) writer.Write(dataValue, NpgsqlDbType.Double);
                                    else writer.Write(dataValue, NpgsqlDbType.Integer);

                                }
                                else if (dataColumn.DataType == typeof(DateTime))
                                {
                                    DateTime dateValue = new DateTime(2008, 1, 1); // It's the default we used elsewhere in here
                                    if (dataValue != null)
                                        dateValue = Convert.ToDateTime(dataValue);

                                    writer.Write(dateValue, NpgsqlDbType.Timestamp);
                                }
                                else
                                {
                                    string colValue = Convert.ToString(dataValue ?? String.Empty);
                                    if (colValue.Length > 0)
                                        writer.Write(colValue);
                                    else
                                        writer.Write(String.Empty);
                                }
                            }
                        }
                    }
                    writer.Complete();
                }
            }
        }

        private static void WriteDataToFinalTable(ExporterConnection ExporterConnection, string databaseTableName, ExportType exportType, string keyColumnName, string updateSQL, string insertSQL, string valuesSQL, string tempTableName)
        {
            string sql;
            if (exportType == ExportType.DropCreateAndRepopulate ||
                exportType == ExportType.TruncateAndRepopulate ||
                exportType == ExportType.Insert)
            {
                sql = $"INSERT INTO {databaseTableName} ( {insertSQL} ) SELECT  {valuesSQL} FROM {tempTableName}";
            }
            else  // UpdateAndInsert && UpdateInsertAndDelete
            {
                // PostgreSQL version 14 or less...first change temp table in updateSQL to excluded
                updateSQL = updateSQL.Replace(tempTableName, "excluded");
                sql = $"INSERT INTO {databaseTableName} ( {insertSQL} ) ";
                sql += $"SELECT {valuesSQL} FROM {tempTableName} ";
                sql += $"ON CONFLICT ({keyColumnName}) DO UPDATE SET {updateSQL} ;";
                // PostgreSQL version 15 or higher
                //sql = $"MERGE INTO {databaseTableName} USING {tempTableName} ";
                //sql += $"ON ( {databaseTableName}.{keyColumnName} = {tempTableName}.{keyColumnName} ) ";
                //sql += "WHEN MATCHED THEN UPDATE SET " + updateSQL + " ";
                //sql += $"WHEN NOT MATCHED THEN INSERT( " + insertSQL + " ) VALUES ( " + valuesSQL + " ) ;";
                if (exportType == ExportType.UpdateInsertAndDelete)
                {
                    using (var cmd = ExporterConnection.Connection.CreateCommand())
                    {
                        cmd.CommandTimeout = ExporterConnection.ConnectionTimeOut;
                        cmd.CommandText = sql;
                        cmd.ExecuteNonQuery();
                    }
                    sql = $"DELETE FROM {databaseTableName} ";
                    sql += $"WHERE NOT EXISTS ( SELECT 'X' FROM {tempTableName} WHERE {databaseTableName}.{keyColumnName} = {tempTableName}.{keyColumnName} );";
                }
            }
 
            // Updating destination table, and dropping temp table
            using (var cmd = ExporterConnection.Connection.CreateCommand())
            {
                cmd.CommandTimeout = ExporterConnection.ConnectionTimeOut;
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }
        }

        internal static void CheckExportContextColumnsAgainstDatabaseColumns(IGridDataOpenExportDataContext exportContext, bool[] columnsEnabledExport, List<DbColumnInfo> dbColumnNames)
        {
            int colIdx = -1;
            foreach (var col in exportContext.Records.Columns)
            {
                Boolean foundFlag = false;
                colIdx++;
                if (columnsEnabledExport[colIdx] == false)
                {
                    foundFlag = true;
                }
                else
                {
                    foreach (var dbColumnName in dbColumnNames)
                    {
                        if (dbColumnName.Name.ToLowerInvariant() == col.Name.ToLowerInvariant())
                        {
                            foundFlag = true;
                            break;
                        }
                    }
                }
                if (foundFlag == false)
                {
                    string exceptionMessage = $" {col.Name} column name is enabled in Simio table, but not in database.  "
                    + "Set 'Drop Create And Repopulate' in Data Export Type to make sure the "
                    + "Simio table structure and the database table structure are aligned.";
                    throw new Exception(exceptionMessage);
                }
            }
        }

        internal static DataTable ConvertExportContextToDataTable(ExporterConnection ExporterConnection, IGridDataOpenExportDataContext exportContext, bool[] columnsEnabledExport, List<DbColumnInfo> dbColumnInfoList)
        {
            // New table
            var dataTable = new DataTable();
            dataTable.TableName = exportContext.GridDataName;
            dataTable.Locale = CultureInfo.InvariantCulture;

            List<int> colExportRecordIndices = new List<int>();

            // For each column in the actual DB table...
            foreach (var dbColumnInfo in dbColumnInfoList)
            {
                // Find the corresponding column in the export records...
                bool bFound = false;
                int exportRecordIndex = 0;
                foreach (var col in exportContext.Records.Columns)
                {
                    if (columnsEnabledExport[exportRecordIndex] == true && dbColumnInfo.Name.ToLowerInvariant() == col.Name.ToLowerInvariant())
                    {
                        // When (and if) we find the column, then record the index into 
                        //  the export records we found the record column for this given 
                        //  DB column
                        colExportRecordIndices.Add(exportRecordIndex);
                        bFound = true;
                        break;
                    }
                    exportRecordIndex++;
                }

                // We did not find a column in the export records that corresponds with this DB column,
                //  record an invalid index for the column
                if (!bFound)
                {
                    colExportRecordIndices.Add(-1);

                }
                // Add database column to data table
                else
                {
                    var dtCol = dataTable.Columns.Add(dbColumnInfo.Name, dbColumnInfo.Type);
                    dtCol.AllowDBNull = dbColumnInfo.AllowDbNull;
                }
            }

            // Add Rows to data table
            foreach (var record in exportContext.Records)
            {
                object[] thisRow = new object[dataTable.Columns.Count];

                // For each defined DB column...
                int dbColIndex = 0;
                int dataTableColumnIndex = 0;
                foreach (var dbColumnInfo in dbColumnInfoList)
                {
                    // Get the index of the corresponding column in the export records
                    int exportRecordColIdx = colExportRecordIndices[dbColIndex];

                    if (exportRecordColIdx >= 0)
                    {
                        // There was a corresponding export records column, and it was enabled, so export its value
                        string formattedValue = GetFormattedStringValue(ExporterConnection, record.GetString(exportRecordColIdx), record.GetNativeObject(exportRecordColIdx), dbColumnInfo);
                        thisRow[dataTableColumnIndex] = formattedValue;
                        dataTableColumnIndex++;
                    }
                    dbColIndex++;
                }

                dataTable.Rows.Add(thisRow);
            }

            return dataTable;
        }

        const string FLOAT_UNRESOLVED_VALUE = "-1.7E308"; // magic number for 'unresolved'
        static readonly DateTime DATETIME_UNRESOLVED_VALUE = new DateTime(2504, 1, 1); // magic date for 'unresolved'
        private static string GetFormattedStringValue(ExporterConnection ExporterConnection, String valueString, object valueObject, DbColumnInfo dbColumnInfo)
        {
            if (valueString == null)
            {
                if (dbColumnInfo.AllowDbNull)
                    return null;
                else
                    valueString = String.Empty;
            }

            if (dbColumnInfo.Type == typeof(int))
            {
                Int64 intProp = 0;
                if (valueString.Length > 0)
                {
                    Int64.TryParse(valueString, NumberStyles.Any, CultureInfo.InvariantCulture, out intProp);
                }
                valueString = intProp.ToString(CultureInfo.InvariantCulture);
            }
            else if (dbColumnInfo.Type == typeof(Single) || dbColumnInfo.Type == typeof(double) || dbColumnInfo.Type == typeof(decimal))
            {
                if (valueString.Length > 0)
                {
                    if (valueString.ToLowerInvariant() == "nan")
                    {
                        if (dbColumnInfo.AllowDbNull)
                            valueString = null;
                        else
                            valueString = FLOAT_UNRESOLVED_VALUE;
                    }
                    else if (valueString == "∞")
                    {
                        valueString = "1.0E308"; // Simio magic number for SQL Server Infinity
                    }
                    else if (valueString == "-∞")
                    {
                        valueString = "-1.0E308"; // Simio magic number for SQL Server -Infinity
                    }
                    else if (valueString == "True" || valueString == "False")
                    {
                        // done for Oracle...Boolean uses NUMBER(1,0) type
                        bool boolProp = false;
                        if (valueString.Length > 0)
                        {
                            Boolean.TryParse(valueString, out boolProp);
                        }
                        valueString = Convert.ToString(Convert.ToInt32(boolProp));
                    }
                    else
                    {
                        if (valueObject is double valueDouble)
                            valueString = String.Format(CultureInfo.InvariantCulture, "{0}", valueDouble);
                        else
                        {
                            Double doubleProp = 0.0;
                            Double.TryParse(valueString, NumberStyles.Any, CultureInfo.InvariantCulture, out doubleProp);
                            valueString = doubleProp.ToString(CultureInfo.InvariantCulture);
                        }
                    }
                }
                else
                {
                    valueString = FLOAT_UNRESOLVED_VALUE;
                }
            }
            else if (dbColumnInfo.Type == typeof(DateTime) || dbColumnInfo.Type == typeof(Nullable<DateTime>))
            {
                if (valueString.Length > 0)
                {
                    DateTime dateProp = Exporter.DATETIME_UNRESOLVED_VALUE;
                    DateTime.TryParse(valueString, out dateProp);

                    valueString = dateProp.ToString(ExporterConnection.DateTimeFormat);
                }
                else
                {
                    if (dbColumnInfo.AllowDbNull)
                        valueString = null;
                    else
                        valueString = DATETIME_UNRESOLVED_VALUE.ToString(ExporterConnection.DateTimeFormat);
                }
            }
            else if (dbColumnInfo.Type == typeof(bool) || dbColumnInfo.Type == typeof(SByte) || dbColumnInfo.Type == typeof(Byte))
            {
                bool boolProp = false;
                if (valueString.Length > 0)
                {
                    Boolean.TryParse(valueString, out boolProp);
                }
                valueString = boolProp.ToString(CultureInfo.InvariantCulture);
            }

            return valueString;
        }

        private static string GetExportDataColumnType(ExporterConnection ExporterConnection, IGridDataExportColumnInfo col, bool primaryKey)
        {
            if (col.Type == typeof(double))
            {
                return "real";
            }
            else if (col.Type == typeof(int))
            {
                return "integer";
            }
            else if (col.Type == typeof(DateTime) || col.Type == typeof(Nullable<DateTime>))
            {
                return "timestamp";
            }
            else if (col.Type == typeof(bool))
            {
                return "boolean";                
            }
            else
            {
                if (primaryKey)
                {
                    return "varchar(1000)";
                }
                else
                {
                    return "varchar(1000)";
                }
            }
        }
    }

    internal class ExporterConnection : IDisposable
    {
        public ExporterConnection(string connectionString, Int32 connectionTimeout, string dateTimeFormat)
        {
            ConnectionTimeOut = connectionTimeout;
            DateTimeFormat = dateTimeFormat;
            Connection = new NpgsqlConnection(connectionString);
            Connection.ConnectionString = connectionString;
            Connection.Open();
        }

        public NpgsqlConnection Connection { get; }
        public Int32 ConnectionTimeOut { get; } = 600;
        public string DateTimeFormat { get; }

        public void Dispose()
        {
            if (Connection != null)
            {
                Connection.Close();
                Connection.Dispose();
            }
        }
    }
}
