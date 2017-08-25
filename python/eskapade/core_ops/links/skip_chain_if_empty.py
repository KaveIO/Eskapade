# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : SkipChainIfEmpty                                                      *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      Algorithm to skip to the next Chain if input dataset is empty             *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

from eskapade import DataStore
from eskapade import Link
from eskapade import StatusCode
from eskapade import process_manager


class SkipChainIfEmpty(Link):
    """
    Sents a SkipChain deenums.StatusCode signal when an appointed dataset is empty. 

    This signal causes that the processsManager to step immediately to the next Chain.
    Input collections can be either mongo collections or dataframes in the datastore.
    """

    def __init__(self, **kwargs):
        """
        Skip to the next Chain if any of the input dataset is empty.

        :param str name: name of link
        :param list collectionSet: datastore keys holding the datasets to be checked. If any of these is empty,
        the chain is skipped.
        :param bool skip_chain_when_key_not_in_ds: skip the chain as well if the dataframe is not present in the
        datastore. When True and if type is 'pandas.DataFrame', sents a SkipChain signal if key not in DataStore
        :param bool checkAtInitialize: perform dataset empty is check at initialize. Default is true.
        :param bool checkAtExecute: perform dataset empty is check at initialize. Default is false.
        """

        Link.__init__(self, kwargs.pop('name', 'SkipChainIfEmpty'))

        # process keyword arguments
        self._process_kwargs(kwargs, collectionSet=[], skip_chain_when_key_not_in_ds=False, checkAtInitialize=True,
                             checkAtExecute=False)
        self.check_extra_kwargs(kwargs)

        return

    def initialize(self):
        """ Initialize SkipChainIfEmpty """

        if self.checkAtInitialize:
            return self.check_collection_set()

        return StatusCode.Success

    def execute(self):
        """ Execute SkipChainIfEmpty """

        if self.checkAtExecute:
            return self.check_collection_set()

        return StatusCode.Success

    def check_collection_set(self):
        """ 
        Check existence of collection in either mongo or datastore, and check that they are not empty.
    
        Collections need to be both present and not empty.

        - For mongo collections a dedicated filter can be applied before doing the count. - For pandas dataframes the
        additional option 'skip_chain_when_key_not_in_ds' exists. Meaning, skip the chain as well if the dataframe is
        not present in the datastore.
        """

        # check if collection names are present in datastore
        ds = process_manager.service(DataStore)
        for k in self.collectionSet:
            if k not in list(ds.keys()):
                if self.skip_chain_when_key_not_in_ds:
                    self.log().warning('Key {key!s} not in DataStore. Sending skip chain signal.'.format(key=key))
                    return StatusCode.SkipChain
                else:
                    raise Exception('Key <%s> not in DataStore.' % k)
            df = ds[k]
            if len(df.index) == 0:
                self.log().warning('pandas.DataFrame with datastore key {key!s} is empty. Sending skip chain signal.'
                                   .format(key=key))
                return StatusCode.SkipChain

        return StatusCode.Success
