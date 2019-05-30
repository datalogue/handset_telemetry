"""
    author: michael phelan
    
    description: datalogue class to call functions
"""

#
# library imports
#
import pandas as _pd
from uuid import UUID as _UUID
from os import path as _path

#
# Datalogue Imports
#
from datalogue.version import __version__
from datalogue.dtl import Dtl as _Dtl, DtlCredentials as _DtlCredentials
from datalogue.models.datastore import Datastore as _Datastore, FileFormat as _FileFormat,  AzureDatastoreDef as _AzureDatastoreDef, HttpDatastoreDef as _HttpDatastoreDef
from datalogue.models.ontology import DataRef as _DataRef
from datalogue.models.stream import Definition as _Definition, Stream as _Stream
from datalogue.models.transformations import Classify as _Classify

class DTL(object):
    
    __FILE_FORMATS = {'csv':_FileFormat.Csv, 'json':_FileFormat.Json}
    
    def __init__(self, host, username, password):
        try:
            self.dtl = _Dtl(credentials=_DtlCredentials(uri='{0}/api'.format(host), username=username, password=password))
            print('Datalogue v{0}'.format(__version__))
            print(self.dtl)
        except Exception as ex:
            print('DTL ERROR :: {0}'.format(ex))          


    ############################################################################################################
    #
    # SERVER
    #
    ############################################################################################################
    
    
    """
        get summary of stores, collections
    """
    def server_summary(self):
        try:
            # data stores
            stores = self.get_data_stores()
            stores = 0 if stores is None else len(stores)

            # data collections
            collections = self.get_data_collections()
            collections = 0 if collections is None else len(collections)

            # pipelines
            streams = self.get_streams()
            streams = 0 if streams is None else len(streams)

            print('\nDatalogue Server Summary :: Stores: {0}, Collections: {1}, Streams: {2}\n'.format(stores, collections, streams))

        except Exception as ex:
            print('error getting server summary :: {0}'.format(ex))
                

    """
        reset all data stores, collections and specified ontologies
    """
    def server_reset(self, ontology_ids, store_exclude=None, collection_exclude=None, stream_exclude=None):
        try:                
            # delete all stores
            self.delete_data_stores(exclude=store_exclude)
            if self.get_data_stores() is None:
                print('\nall data stores deleted ...\n')

            # delete all collections
            self.delete_data_collections(exclude=collection_exclude)
            if self.get_data_collections() is None:
                print('\nall data collections deleted ...\n')

            # delete ontologies
            for ontology_id in ontology_ids:
                if self.delete_ontology(ontology_id=ontology_id):
                    print('\nontologies deleted ...\n')

            # delete streams
            self.delete_streams(exclude=stream_exclude)
            if self.get_streams() is None:
                print('\nall pipelines deleted ...\n')

        except Exception as ex:
            print('error resetting server :: {0}'.format(ex))



    ############################################################################################################
    #
    # CREDENTIALS
    #
    ############################################################################################################


    """
        get a list of credentials
    """
    def get_credentials(self):
        df_credentials = None
        credentials = []
        try:
            for credential in self.dtl.credentials.list():
                credentials.append({'credential_id':credential.id, 'credential_name':credential.name, 'credential_type':credential.type.name})

            if len(credentials) > 0:
                df_credentials = _pd.DataFrame(credentials)[['credential_id','credential_name','credential_type']]

            return (df_credentials)

        except Exception as ex:
            print('error getting credentials :: {0}'.format(ex))



    ############################################################################################################
    #
    # DATA STORES
    #
    ############################################################################################################


    """
        get an individual or list of data stores
    """
    def get_data_stores(self, as_df=True, store_id=None):
        stores = []
        try:                
            # check if id entered
            if store_id is None:
                # get a list
                for store in self.dtl.datastore.list():
                    if as_df:
                        stores.append({'store_id':store.id, 'store_name':store.name})
                    else:
                        stores.append(store)
            else:
                # get an individual
                try:
                    store = self.dtl.datastore.get(datastore_id=store_id)
                    if as_df:
                        stores.append({'store_id':store.id, 'store_name':store.name})
                    else:
                        stores.append(store)                    
                except:
                    raise Exception('invalid store id')

            # return dataframe
            if len(stores) > 0:
                if as_df:
                    return (_pd.DataFrame(stores)[['store_id','store_name']])
                else:
                    return (stores)
            else:
                print('no data stores available')
                return (None)

        except Exception as ex:
            print('error getting data stores ({0}) :: {1}'.format('multiple' if store_id is None else 'single', ex))


    """
        get a specified datastore as a pandas dataframe
    """
    def get_data_store_dataframe(self, store_id):
        try:
            table = self.dtl.datastore.load_arrow_table(datastore_id=store_id)
            if table is None:
                raise Exception('not data found for that store')

            df = table.to_pandas()

            return (df)

        except Exception as ex:
            print('error creating dataframe from data store :: {0}'.format(ex))


    """
        get a specified datastore as a pandas dataframe
    """
    def get_data_store_details(self, store_id):
        try:
            df = self.get_data_store_dataframe(store_id)
            columns = sorted(list(df.columns))

            return ({'columns':columns})

        except Exception as ex:
            print('error getting data store details :: {0}'.format(ex))        


    """
        create a new data store
    """
    def insert_data_store(self, name, params):
        try:
            store = None
            definition = None    
            credential_id = None

            # validate params
            self._validate_params(params=params, items=['type','file_format'])

            # file format
            try:
                file_format = self.__FILE_FORMATS[params['file_format']]
            except:
                raise Exception('invalid file format')


            if params['type'] == 'http':
                #
                # HttpDatastoreDef
                #        
                self._validate_params(params=params, items=['url'])

                # create the definition & store
                try:
                    definition = _HttpDatastoreDef(url=params['url'], file_format=file_format)            
                except Exception as e:
                    raise Exception('HttpDatastoreDef :: {0}'.format(e))

            elif params['type'] == 'azure':
                #
                # AzureDatastoreDef
                #
                self._validate_params(params=params, items=['container','file_name','credential_id'])

                # create file name with extension
                file_name = '{0}.{1}'.format(_path.splitext(params['file_name'])[0], file_format.name.lower())

                credential_id = params['credential_id']

                # create the definition & store
                try:
                    definition = _AzureDatastoreDef(container=params['container'], file_name=file_name, file_format=file_format)            
                except Exception as e:
                    raise Exception('AzureDatastoreDef :: {0}'.format(e))

            else:
                raise Exception('invalid data store type')


            # create datastore
            try:
                store = self.dtl.datastore.create (
                    _Datastore(name=name, definition=definition, alias=None, credential_id=credential_id)
                )
                print('data store (id=\'{0}\') successfully created ...'.format(store.id))

                return (store)

            except Exception as e:
                raise Exception('data store creation :: {0}'.format(e))    

        except Exception as ex:
            print('error creating data stores :: {0}'.format(ex))


    """
        delete a data store
            dtl: server connection
            _id: if of specific store to delete
            exclude: list of store ids to be excluded    
    """
    def delete_data_stores(self, store_id=None, exclude=None):
        try:                
            # delete all stores
            if store_id is None:
                store_count = len(self.dtl.datastore.list())
                for store in self.dtl.datastore.list():
                    if exclude is None:
                        self.dtl.datastore.delete(datastore_id=store.id)
                    else:
                        if store.id not in exclude:
                            self.dtl.datastore.delete(datastore_id=store.id)

                if len(self.dtl.datastore.list()) == 0:
                    print('{0} data stores deleted ...'.format(store_count))

            else:
                # delete a single store
                self.dtl.datastore.delete(datastore_id=_UUID(store_id))
                print('data store deleted ...')

        except Exception as ex:
            print('error deleting data stores ({0}) :: {1}'.format('multiple' if store_id is None else 'single', ex))



    ############################################################################################################
    #
    # DATA COLLECTIONS
    #
    ############################################################################################################


    """
        get an individual or list of data collections with option of associated data stores
            datastore: include datastore details
            as_df: return as dataframe or object collection
            collection_id: specific collection
    """
    def get_data_collections(self, datastore=False, as_df=True, collection_id=None):
        collections = []
        flag = False
        try:                
            # check if id entered
            if collection_id is None:
                # get a list
                for collection in self.dtl.datastore_collection.list():
                    if as_df:
                        if not datastore or len(collection.storeIds) == 0:                                
                            collections.append({'collection_id':collection.id, 'collection_name':collection.name})
                        else:
                            for store in collection.storeIds:
                                collections.append({'collection_id':collection.id, 'collection_name':collection.name, 'store_id':store['id'], 'store_name':store['name']})
                            flag = True
                    else:
                        collections.append(collection)
            else:
                # get an individual collection
                try:
                    collection = self.dtl.datastore_collection.get(datastore_collection_id=_UUID(collection_id))
                    if as_df:
                        if not datastore or len(collection.storeIds) == 0:                                
                            collections.append({'collection_id':collection.id, 'collection_name':collection.name})
                        else:
                            for store in collection.storeIds:
                                collections.append({'collection_id':collection.id, 'collection_name':collection.name, 'store_id':store['id'], 'store_name':store['name']})
                            flag = True
                    else:
                        collections.append(collection)
                except:
                    raise Exception('invalid collection id')

            # return dataframe
            if len(collections) > 0:
                if as_df:
                    if flag:
                        return (_pd.DataFrame(collections)[['collection_id','collection_name','store_id','store_name']])
                    else:
                        return (_pd.DataFrame(collections)[['collection_id','collection_name']])
                else:
                    return (collections)
            else:
                print('no data collections available')
                return (None)

        except Exception as ex:
            print('error getting data collections ({0}) :: {1}'.format('multiple' if collection_id is None else 'single', ex))


    """
        insert a new data collection
    """
    def insert_data_collection(self, collection):
        try:
            collection = self.dtl.datastore_collection.create(collection)
            print('data collection (id=\'{0}\') successfully created ...'.format(collection.id))

            return (collection)

        except Exception as ex:
            print('error inserting data collection :: {0}'.format(ex))


    """
        delete a data collection
            dtl: server connection
            _id: if of specific collection to delete
            exclude: list of collection ids to be excluded    
    """
    def delete_data_collections(self, collection_id=None, exclude=None):
        try:                
            # delete all stores
            if collection_id is None:
                collection_count = len(self.dtl.datastore_collection.list())
                for collection in self.dtl.datastore_collection.list():
                    if exclude is None:
                        self.dtl.datastore_collection.delete(datastore_collection_id=collection.id)
                    else:
                        if collection.id not in exclude:
                            self.dtl.datastore_collection.delete(datastore_collection_id=collection.id)

                if len(self.dtl.datastore_collection.list()) == 0:
                    print('{0} data collections deleted ...'.format(collection_count))

            else:
                # delete a single store
                self.dtl.datastore_collection.delete(datastore_collection_id=_UUID(collection_id))
                print('data collection deleted ...')

        except Exception as ex:
            print('error deleting data collections ({0}) :: {1}'.format('multiple' if collection_id is None else 'single', ex))



    ############################################################################################################
    #
    # ONTOLOGIES
    #
    ############################################################################################################


    """
        insert a new ontology
    """
    def insert_ontology(self, ontology):
        try:
            # create the ontology
            ontology = self.dtl.ontology.create(ontology)

            # check the created ontology
            inserted_ontology = self.dtl.ontology.get(ontology.ontology_id)
            print('ontology \'{0}\' created ...'.format(inserted_ontology.name))

            return (inserted_ontology)

        except Exception as ex:
            print('error inserting ontology :: {0}'.format(ex))         


    """
        delete an ontology
    """
    def delete_ontology(self, ontology_id):
        try:
            # check ontology exists
            ontology = self.dtl.ontology.get(ontology_id=_UUID(ontology_id))

            try:
                name = ontology.name
            except:
                raise Exception('ontology does not exist')

            # delete ontology
            self.dtl.ontology.delete(ontology_id=_UUID(ontology_id))
            print('ontology \'{0}\' deleted ...'.format(name))
            return (True)
        
        except Exception as ex:
            print('error deleting ontology (id=\'{0}\') :: {1}'.format(ontology_id, ex))
            return (False)


    def add_ontology_training_data(self, stores, ontology, store_ref_id, store_attribute, ontology_leaf):
        try:
            # check if there is a store
            store = None
            try:
                store = [store for store in stores if store['id']==store_ref_id][0]
            except:
                raise Exception('store id does not exist in data stores')

            # check store_attribute is in store
            attributes = self.get_data_store_details(store_id=store['datastore_object'].id)['columns']
            if store_attribute not in attributes:
                raise Exception ('store attribute does not exist in data store, available attributes ::', attributes)

            # check the leaf exists in the ontology
            leaves = [leaf.name for leaf in ontology.leaves()]        
            if ontology_leaf not in leaves:
                raise Exception ('ontology leaf does not exist inontology, available leaves ::', leaves)

            leaf_id = leaves.index(ontology_leaf)
            path = ([store['url'], store_attribute])
            ref = _DataRef(ontology.leaves()[leaf_id], [path])

            stream_id = self.dtl.training.data.add(store_id=store['datastore_object'].id, store_name=store['datastore_object'].name, refs=[ref])
            print('ontology \'{0}\' training stream created ...'.format(stream_id))

            return (stream_id)

        except Exception as ex:
            print('error adding training data to ontology :: {0}'.format(ex))
            
            
    """
    """
    def model_train(self, ontology_id):
        try:
            result = self.dtl.training.run(ontology_id=ontology_id)
            return (result)
        except Exception as ex:
            print('error training the model :: {0}'.format(ex))


    """
    """
    def model_deploy(self, ontology_id, training_id):
        try:
            result = self.dtl.training.deploy(ontology_id=ontology_id, training_id=training_id)
            return (result)
        except Exception as ex:
            print('error deploying the model :: {0}'.format(ex))            


    """
    """
    def model_training_results(self, ontology_id, verbose=False):
        try:
            training = self.dtl.training.get_trainings(ontology_id=ontology_id)[0]
            
            if verbose:
                print('\n*** Model Training ***\nId: {0})\nStatus: {1}\nEpochs: {2}'.format(training.training_id, training.status, len(training.epochs)))
            
            return (training)
        
        except Exception as ex:
            print('error getting model trainings :: {0}'.format(ex))              



    ############################################################################################################
    #
    # PIPELINES
    #
    ############################################################################################################


    """
        get an individual or list of pipelines
    """
    def get_streams(self, as_df=True, stream_id=None):
        streams = []
        try:                
            # check if id entered
            if stream_id is None:
                # get a list            
                for stream in self.dtl.stream_collection.list():
                    if as_df:
                        streams.append({'stream_id':stream.id, 'stream_name':stream.name})
                    else:
                        streams.append(stream)
            else:
                # get an individual
                try:
                    if as_df:
                        stream = self.dtl.stream_collection.get(stream_collection_id=_UUID(stream_id))
                        streams.append({'stream_id':stream.id, 'stream_name':stream.name})
                    else:
                        streams.append(stream)                
                except:
                    raise Exception('invalid pipeline id')

            # return dataframe
            if len(streams) > 0:
                if as_df:
                    return (_pd.DataFrame(streams)[['stream_id','stream_name']])
                else:
                    return (streams)
            else:
                print('no pipelines available')
                return (None)

        except Exception as ex:
            print('error getting pipelines ({0}) :: {1}'.format('multiple' if stream_id is None else 'single', ex))


    """
        delete a pipeline
            dtl: server connection
            _id: if of specific stream to delete
            exclude: list of stream ids to be excluded
    """
    def delete_streams(self, stream_id=None, exclude=None):
        try:                
            # delete all stream
            if stream_id is None:
                stream_count = len(self.dtl.stream_collection.list())
                for stream in self.dtl.stream_collection.list():
                    if exclude is None:
                        self.dtl.stream_collection.delete(stream_collection_id=stream.id)
                    else:
                        if stream.id not in exclude:
                            self.dtl.stream_collection.delete(stream_collection_id=stream.id)

                if len(self.dtl.stream_collection.list()) == 0:
                    print('ALL {0} pipelines deleted'.format(stream_count))
                else:
                    print('PARTIAL {0} pipelines deleted'.format(stream_count))

            else:
                # delete a single store
                self.dtl.stream_collection.delete(stream_collection_id=_UUID(stream_id))
                print('pipeline deleted')

        except Exception as ex:
            print('error deleting pipeline ({0}) :: {1}'.format('multiple' if stream_id is None else 'single', ex))


    """
    """
    def insert_stream(self, pipeline_name, store_id_input, store_id_output, ontology_id, run=False):
        try:
            # get the data stores and ontology model
            store_input = self.dtl.datastore.get(datastore_id=store_id_input)
            store_output = self.dtl.datastore.get(datastore_id=store_id_output)
            ontology = self.dtl.ontology.get(ontology_id=ontology_id)

            # create pipeline definition
            pipeline_defn = _Definition (
                transformations = [_Classify(use_context=True, include_classes=True, include_scores=True)],
                pipelines = [],
                target = store_output 
            )

            # create a stream
            stream = _Stream(source=store_input, pipelines=[pipeline_defn])

            # create a stream collection
            stream_collection = self.dtl.stream_collection.create([stream], pipeline_name)

            print('Pipeline inserted with id: {0}'.format(stream_collection.id))

            if run:
                print('running pipeline ...')
                self.dtl.stream_collection.run(stream_collection.id)

            return (stream_collection)

        except Exception as ex:
            print('error inserting pipeline :: {0}'.format(ex))



    ############################################################################################################
    #
    # JOBS
    #
    ############################################################################################################


    """
        get a list of jobs
    """
    def get_jobs(self):
        df_jobs = None
        jobs = []
        try:
            for job in self.dtl.jobs.list():
                jobs.append({'job_id':job.job_id, 'job_status':job.status.value, 'stream_id':job.stream_collection_id, 'run_timestamp': job.run_at})        

            if len(jobs) > 0:
                df_jobs = _pd.DataFrame(jobs)

                # get streams and merge
                df_streams = self.get_streams()
                if df_streams is not None and len(df_streams) > 0:
                    df_jobs = df_jobs.merge(df_streams, how='left', on=['stream_id'], left_index=False, right_index=False)
                    df_jobs['stream_name'].fillna('', inplace=True)
                    df_jobs = df_jobs[['job_id','job_status','stream_name','stream_id','run_timestamp']]
                else:
                    df_jobs = df_jobs[['job_id','job_status','stream_id','run_timestamp']]

            return (df_jobs)

        except Exception as ex:
            print('error getting jobs :: {0}'.format(ex))



    ############################################################################################################
    #
    # HELPERS
    #
    ############################################################################################################        

    
    """
        get df details by column and name
    """
    def get_dtl_details(self, dtl_type, key, value):
        df_details = None
        try:
            if dtl_type == 'credentials':
                df_details = self.get_credentials()
            else:
                raise Exception('invalid dtl type')
            
            # filter data
            df_details = df_details[df_details[key]==value]

            return (df_details)

        except Exception as ex:
            print('error getting dtf details :: {0}'.format(ex))


    """
        validate paramter key values
    """
    def _validate_params(self, params, items):
        for item in items:
            if item not in params.keys():
                raise Exception('\'{0}\' parameter not defined'.format(item))
            else:
                if params[item] is None or len(str(params[item])) == 0:
                    raise Exception('\'{0}\' parameter value not defined'.format(item))


