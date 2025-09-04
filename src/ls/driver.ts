import AbstractDriver from '@sqltools/base-driver';
import { IConnectionDriver, MConnectionExplorer, NSDatabase, Arg0, ContextValue } from '@sqltools/types';
import queries from './queries';
import { v4 as generateId } from 'uuid';
import { Athena, AWSError, Credentials, SharedIniFileCredentials } from 'aws-sdk';
import { PromiseResult } from 'aws-sdk/lib/request';
import { GetQueryResultsInput, GetQueryResultsOutput } from 'aws-sdk/clients/athena';
import { HttpsProxyAgent } from 'https-proxy-agent';
import { DriverObjectCache } from './cache';
import { ColumnItem, DatabaseItem, TableViewItem } from './types';


export default class AthenaDriver extends AbstractDriver<Athena, Athena.Types.ClientConfiguration> implements IConnectionDriver {

  queries = queries;

  private readonly schema = 'AwsDataCatalog';
  private readonly cache = DriverObjectCache.getInstance();

  /**
   * If you driver depends on node packages, list it below on `deps` prop.
   * It will be installed automatically on first use of your driver.
   */
  public readonly deps: typeof AbstractDriver.prototype['deps'] = [{
    type: AbstractDriver.CONSTANTS.DEPENDENCY_PACKAGE,
    name: 'lodash',
    // version: 'x.x.x',
  }];

  /** if you need to require your lib in runtime and then
   * use `this.lib.methodName()` anywhere and vscode will take care of the dependencies
   * to be installed on a cache folder
   **/
  // private get lib() {
  //   return this.requireDep('node-packge-name') as DriverLib;
  // }

  public async open() {
    if (this.connection) {
      return this.connection;
    }

    if (this.credentials.connectionMethod !== 'Profile')
      var credentials = new Credentials({
        accessKeyId: this.credentials.accessKeyId,
        secretAccessKey: this.credentials.secretAccessKey,
        sessionToken: this.credentials.sessionToken,
      });
    else
      var credentials = new SharedIniFileCredentials({ profile: this.credentials.profile });

    this.connection = Promise.resolve(new Athena({
      credentials: credentials,
      region: this.credentials.region || 'us-east-1',
      httpOptions: {
        agent: this.credentials.httpsProxy ? new HttpsProxyAgent(this.credentials.httpsProxy) : undefined,
      }
    }));

    await Promise.all([
      this.getDatabases(await this.connection, this.schema),
      this.getTables("default", this.schema)
    ]);

    return this.connection;
  }
  private formatBytes = (bytes: number, decimals: number = 2) => {
    if (!+bytes) return '0 Bytes'

    const k = 1024
    const dm = decimals < 0 ? 0 : decimals
    const sizes = ['Bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']

    const i = Math.floor(Math.log(bytes) / Math.log(k))

    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`
  }

  public async close() {
    if (this.connection) {
      this.connection = null;
    }
  }

  private sleep = (time: number) => new Promise((resolve) => setTimeout(() => resolve(true), time));

  private rawQuery = async (query: string) => {
    const db = await this.open();

    const queryExecution = await db.startQueryExecution({
      QueryString: query,
      WorkGroup: this.credentials.workgroup,
      ResultConfiguration: {
        OutputLocation: this.credentials.outputLocation
      }
    }).promise();

    const endStatus = new Set(['FAILED', 'SUCCEEDED', 'CANCELLED']);

    let queryCheckExecution;

    do {
      queryCheckExecution = await db.getQueryExecution({
        QueryExecutionId: queryExecution.QueryExecutionId,
      }).promise();

      console.log(
        `Query ${queryExecution.QueryExecutionId} ` +
        `is ${queryCheckExecution.QueryExecution.Status.State} ` +
        `${queryCheckExecution.QueryExecution.Statistics?.TotalExecutionTimeInMillis} ms elapsed. ` +
        `${this.formatBytes(queryCheckExecution.QueryExecution.Statistics?.DataScannedInBytes)} scanned`
      );

      await this.sleep(200);
    } while (!endStatus.has(queryCheckExecution.QueryExecution.Status.State))

    if (queryCheckExecution.QueryExecution.Status.State === 'FAILED') {
      throw new Error(queryCheckExecution.QueryExecution.Status.StateChangeReason)
    }

    return queryCheckExecution;
  }

  private async getQueryResults(
    queryExecutionId: string
  ) {
    const results: PromiseResult<GetQueryResultsOutput, AWSError>[] = [];
    let result: PromiseResult<GetQueryResultsOutput, AWSError>;
    let nextToken: string | null = null;
    let db = await this.open();

    do {
      const payload: GetQueryResultsInput = {
        QueryExecutionId: queryExecutionId
      };
      if (nextToken) {
        payload.NextToken = nextToken;
        await this.sleep(200);
      }
      result = await db.getQueryResults(payload).promise();
      nextToken = result?.NextToken;
      results.push(result);
    } while (nextToken);

    return results;
  }

  public query: (typeof AbstractDriver)['prototype']['query'] = async (queries, opt = {}) => {
    const queryExecution = await this.rawQuery(queries.toString());
    const results = await this.getQueryResults(queryExecution.QueryExecution?.QueryExecutionId || '');
    const columns = results[0].ResultSet.ResultSetMetadata.ColumnInfo.map((info) => info.Name);
    const resultSet = [];
    results.forEach((result, i) => {
      const rows = result.ResultSet.Rows;
      if (i === 0) {
        rows.shift();
      }
      rows.forEach(({ Data }) => {
        resultSet.push(
          Object.assign(
            {},
            ...Data.map((column, i) => ({ [columns[i]]: column.VarCharValue }))
          )
        );
      });
    });

    const response: NSDatabase.IResult[] = [{
      cols: columns,
      connId: this.getId(),
      messages: [{
        date: new Date(), message: `Query "${queryExecution.QueryExecution?.QueryExecutionId}" ` +
          `ok with ${resultSet.length} results. ` +
          `${this.formatBytes(queryExecution?.QueryExecution?.Statistics?.DataScannedInBytes || 0)} scanned`
      }],
      results: resultSet,
      query: queries.toString(),
      requestId: opt.requestId,
      resultId: generateId(),
    }];

    return response;
  }

  /** if you need a different way to test your connection, you can set it here.
   * Otherwise by default we open and close the connection only
   */
  public async testConnection() {
    await this.open();
    await this.query('SELECT 1', {});
  }

  /**
   * This method is a helper to generate the connection explorer tree.
   * it gets the child items based on current item
   */
  public async getChildrenForItem({ item, parent }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    const db = await this.connection;

    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        return <MConnectionExplorer.IChildItem[]>[
          { label: 'Catalogs', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.SCHEMA },
        ]
      case ContextValue.SCHEMA:
        return <MConnectionExplorer.IChildItem[]>[
          { label: 'Databases', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.DATABASE },
        ];
      case ContextValue.DATABASE:
        return <MConnectionExplorer.IChildItem[]>[
          { label: 'Tables', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.TABLE },
          { label: 'Views', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.VIEW },
        ];
      case ContextValue.TABLE:
        return this.getColumns(db, item.label, item.database, item.schema, parent);
      case ContextValue.VIEW:
        return this.getColumns(db, item.label, item.database, item.schema, parent);
      case ContextValue.RESOURCE_GROUP:
        return this.getChildrenForGroup({ item, parent });
    }

    return [];
  }

  /**
   * This method is a helper to generate the connection explorer tree.
   * It gets the child based on child types
   */
  private async getChildrenForGroup({ parent, item }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    const db = await this.connection;

    switch (item.childType) {
      case ContextValue.SCHEMA:
        const catalogs = await db.listDataCatalogs().promise();

        return catalogs.DataCatalogsSummary.map((catalog) => ({
          database: '',
          label: catalog.CatalogName,
          type: item.childType,
          schema: catalog.CatalogName,
          childType: ContextValue.DATABASE,
        }));
      case ContextValue.DATABASE:
        return await this.getDatabases(db, parent.schema);
      case ContextValue.TABLE:
        return await this.getTables(parent.database, parent.schema);
      case ContextValue.VIEW:
        console.log('views')
        return await this.getViews(parent.database, parent.schema);
    }
    return [];
  }

  /**
   * This method is a helper for intellisense and quick picks.
   */
  public async searchItems(itemType: ContextValue, search: any, _extraParams: any = {}): Promise<NSDatabase.SearchableItem[]> {
    console.log(search);
    
    switch (itemType) {
      case ContextValue.DATABASE:
        {
          const cached = this.cache.getDatabases();
          const databases = cached || await this.getDatabases(await this.connection, this.schema);
          return databases.map(item => {
            return {
              database: item.database,
              label: item.label,
              type: ContextValue.DATABASE,
              schema: this.schema,
              childType: ContextValue.TABLE,
            }
          });
        }
      case ContextValue.TABLE:
        {
          let filter: string;

          if (
            _extraParams &&
            typeof _extraParams === 'object' &&
            Object.prototype.hasOwnProperty.call(_extraParams, 'database')
          ) {
            filter = _extraParams.database;
          }

          let tables = this.cache.getTables(filter);

          if (!tables) {
            tables = await this.getTables(filter, this.schema);
          }

          return tables.map(item => {
            return {
              database: item.database,
              label: item.label,
              type: ContextValue.TABLE,
              schema: this.schema,
              childType: ContextValue.COLUMN,
            }
          });
        }
      case ContextValue.VIEW:
        {
          let filter: string;

          if (
            _extraParams &&
            typeof _extraParams === 'object' &&
            Object.prototype.hasOwnProperty.call(_extraParams, 'database')
          ) {
            filter = _extraParams.database;
          }

          let tables = this.cache.getViews(filter);

          if (!tables) {
            tables = await this.getTables(filter, this.schema);
          }

          return tables.map(item => {
            return {
              database: item.database,
              label: item.label,
              type: ContextValue.VIEW,
              schema: this.schema,
              childType: ContextValue.COLUMN,
            }
          });
        }
      case ContextValue.COLUMN:
        {
          if (
            !_extraParams ||
            typeof _extraParams !== 'object' ||
            !Array.isArray(_extraParams.tables) ||
            _extraParams.tables.length === 0
          ) {
            return [];
          }

          const columnsArrays = await Promise.all(
            _extraParams.tables.map(async (table) => {
              let columns = this.cache.getColumns(table.database, table.label);

              if (!columns) {
                const parentItem: NSDatabase.SearchableItem = {
                  database: table.database,
                  label: table.label,
                  type: ContextValue.TABLE,
                  schema: this.schema,
                  childType: ContextValue.COLUMN,
                };
                const db = await this.connection;
                columns = await this.getColumns(db, table.label, table.database, this.schema, parentItem);
              }

              return columns || [];
            })
          );

          const columns: ColumnItem[] = columnsArrays.flat();

          return columns.map(item => ({
            database: item.database,
            label: item.label,
            type: ContextValue.COLUMN,
            dataType: item.dataType,
            isNullable: false,
            table: item.table,
            schema: this.schema,
            childType: ContextValue.NO_CHILD,
          }));
        }
    }
    return [];
  }

  public getStaticCompletions: IConnectionDriver['getStaticCompletions'] = async () => {
    return {};
  }

  /**
  * Retrieves the columns for a given table or view item from Athena.
  */
  private async getColumns(db: Athena, table: string, database: string, schema: string, parent: NSDatabase.SearchableItem): Promise<ColumnItem[]> {
    const tableMetadata = await db.getTableMetadata({
      CatalogName: schema,
      DatabaseName: database,
      TableName: table
    }).promise();

    return tableMetadata.TableMetadata.Columns.map((column) => {
      const columnItem: ColumnItem = {
        database: database,
        label: column.Name,
        type: ContextValue.COLUMN,
        dataType: column.Type,
        schema: schema,
        childType: ContextValue.NO_CHILD,
        isNullable: false,
        iconName: 'column',
        table: parent,
      };
      this.cache.add(columnItem)
      return columnItem;
    });
  }

  private async getTables(database: string, schema: string): Promise<TableViewItem[]> {
    const tablesExecution = await this.rawQuery(`SHOW TABLES IN \`${database}\``);
    const tablesResult = await this.getQueryResults(tablesExecution.QueryExecution?.QueryExecutionId || '');

    const views = await this.getViews(database, schema);
    const tables = tablesResult[0].ResultSet.Rows
      .map((row) => {
        const tableItem: TableViewItem = {
          database: database,
          label: row.Data[0].VarCharValue,
          type: ContextValue.TABLE,
          schema: schema,
          childType: ContextValue.COLUMN,
        };
        this.cache.add(tableItem);
        return tableItem;
      })
      .filter(tableItem => {
        // Remove from tables any object that is also in views (same database, label, schema)
        return !views.some(viewItem =>
          viewItem.database === tableItem.database &&
          viewItem.label === tableItem.label &&
          viewItem.schema === tableItem.schema
        );
      });

    return tables;
  }

  private async getViews(database: string, schema: string): Promise<TableViewItem[]> {
    const viewsExecution = await this.rawQuery(`SHOW VIEWS IN ${database}`);
    const viewsResult = await this.getQueryResults(viewsExecution.QueryExecution?.QueryExecutionId || '');

    return viewsResult[0].ResultSet.Rows
      .map((row) => {
        const viewItem: TableViewItem = {
          database: database,
          label: row.Data[0].VarCharValue,
          type: ContextValue.VIEW,
          schema: schema,
          childType: ContextValue.COLUMN,
        };
        this.cache.add(viewItem);
        return viewItem;
      });
  }

  private async getDatabases(db: Athena, schema: string): Promise<DatabaseItem[]> {
    let firstBatch: boolean = true;
    let nextToken: string | null = null;
    const items: Omit<DatabaseItem, 'childType'>[] = [];

    while (firstBatch == true || nextToken !== null) {
      firstBatch = false;
      let listDbRequest: any = {
        CatalogName: schema,
      }
      if (nextToken !== null) {
        Object.assign(listDbRequest, {
          NextToken: nextToken,
        });
      }
      const catalog = await db.listDatabases(listDbRequest).promise();
      nextToken = 'NextToken' in catalog ? catalog.NextToken : null;

      items.push(
        ...catalog.DatabaseList.map((database) => {
          const item: Omit<DatabaseItem, 'childType'> = {
            database: database.Name,
            label: database.Name,
            type: ContextValue.DATABASE,
            schema: schema,
          };
          this.cache.add(item as DatabaseItem);
          return item;
        })
      );
    }

    return items;
  }
}
