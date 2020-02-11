import pandas as pd 
import json
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.documents as documents
import azure.cosmos.http_constants as http_constants

print('Imported packages successfully.')

# Large Portions of this were copied from 
# https://github.com/Azure/azure-cosmos-python/blob/master/samples/CollectionManagement/Program.py#L84-L135

# Load config file
with open('config.json') as config_file:
    config = json.load(config_file)


class IDisposable(cosmos_client.CosmosClient):
    """ A context manager to automatically close an object with a close method
    in a with statement. """

    def __init__(self, obj):
        self.obj = obj

    def __enter__(self):
        return self.obj # bound to target

    def __exit__(self, exception_type, exception_val, trace):
        # extra cleanup in here
        self.obj = None


class AzCosmos:

    # Create the cosmos client
    def clientinit():

        client = cosmos_client.CosmosClient(url_connection=config["endpoint"], auth={"masterKey":config["primarykey"]}
        )
        return client
        print('Client initialized.')

    # create a Cosmos DB database
    def create_cosmosdb(client,database_name):

        try:
            database = client.CreateDatabase({'id': database_name})
            print('{} database created.'.format(database_name))
        except errors.HTTPFailure:
            print("{} already exists".format(database_name))

    # Create a document collection / container
    def create_container(client,database_name, container_name, partition_key):

        database_link = 'dbs/' + database_name
        container_definition = {'id': container_name,
                        'partitionKey':
                                    {
                                        'paths': ['/country'],
                                        'kind': documents.PartitionKind.Hash
                                    }
                        }
        try:
            container = client.CreateContainer(database_link=database_link, 
                                                collection=container_definition, 
                                                options={'offerThroughput': 400})
            print('{} Container created'.format(container_name))
        except errors.HTTPFailure as e:
            if e.status_code == http_constants.StatusCodes.CONFLICT:
                container = client.ReadContainer("dbs/" + database['id'] + "/colls/" + container_definition['id'])
            else:
                raise e
        

        # Write data to the container
    def insert_data(client, df, database_name, container_name):
            
        # Create Connection Link string
        collection_link = 'dbs/' + database_name + '/colls/' + container_name

        # Write rows of a pandas DataFrame as items to the Database Container
        for i in range(0,df.shape[0]):
            # Create dictionary of row being passed to the for loop
            data_dict = dict(df.iloc[i,:])
            # Convert that dictionary to json record
            data_dict = json.dumps(data_dict)
            # Insert the json record into the Azure Cosmos Db
            insert_data = client.UpsertItem(collection_link,json.loads(data_dict))
        print('Records inserted successfully.')


        # Query data from the container
    def query_data(client, database_name, container_name, query):
        # Initialize list
        dflist = []
        # Create Connection Link string
        collection_link = 'dbs/' + database_name + '/colls/' + container_name

        # For-loop to retrieve individual json records from Cosmos DB 
        # that satisfy our query
        for item in client.QueryItems(collection_link,
                                    query,
                                    {'enableCrossPartitionQuery': True}
                                    ):
            # Append each item as a dictionary to list
            dflist.append(dict(item))
            
        # Convert list to pandas DataFrame
        df = pd.DataFrame(dflist)
        print('Query successful.')
        return df


class download:


    def data():
        # Download and read csv file
        df = pd.read_csv('https://globaldatalab.org/assets/2019/09/SHDI%20Complete%203.0.csv',encoding='ISO-8859–1',dtype='str')
        # Reset index - creates a column called 'index'
        df = df.reset_index()
        # Rename that new column 'id'
        # Cosmos DB needs one column named 'id'. 
        df = df.rename(columns={'index':'id'})
        # Convert the id column to a string - this is a document database.
        df['id'] = df['id'].astype(str)

        # Testing - only top 3 items
        df = df.iloc[0:3,:]
        return df

class dataviz:

    def lineplot(query_data):

        import seaborn as sns 
        import matplotlib.pyplot as plt

        # Convert to type float
        index_df = df.loc[:,('healthindex','incindex','edindex','year')].astype('float')

        # Create the figure
        plt.figure(figsize=(14,8))
        # Set a Seaborn chart style
        sns.set(style='darkgrid',font_scale=1.5)
        # Plot three Seaborn line charts
        line1 = sns.lineplot(x='year',y='healthindex',data=index_df, label='Health Index')
        line2 = sns.lineplot(x='year',y='incindex',data=index_df,label='Income Index')
        line3 = sns.lineplot(x='year',y='edindex',data=index_df,label='Education Index')
        plt.ylabel('Index Score')
        plt.legend(loc="upper left")
        plt.title('Afghanistan Human Development Indexes')
        fig = plt.gcf()
        return fig



database_name = 'HDIdatabase2'
container_name = 'HDIcontainer2'
partition_key = 'country'
query = 'SELECT * FROM c where c.country="Afghanistan" and c.level="National"'

def main():

    with IDisposable(cosmos_client.CosmosClient(url_connection=config["endpoint"], auth={"masterKey":config["primarykey"]})) as client:
        
        try:
            
            # Create Database
            AzCosmos.create_cosmosdb(client,database_name)

            # Create Container
            AzCosmos.create_container(client,database_name,container_name,partition_key)

            # Download the data
            df = download.data()
            
            # Insert the data   
            AzCosmos.insert_data(client,df,database_name,container_name)

            # Query the data
            query_data = AzCosmos.query_data(client,database_name,container_name,query)  

        except Exception as e:

            print('You have errors.')

if __name__ == "__main__":
    main()


