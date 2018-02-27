"""Project: Eskapade - A python-based package for data analysis.

Class: SkipChainIfEmpty

Created: 2016/11/08

Description:
    Algorithm to skip to the next Chain if input dataset is empty

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import process_manager, DataStore, Link, StatusCode


class SkipChainIfEmpty(Link):
    """Sents a SkipChain deenums.StatusCode signal when an appointed dataset is empty.

    This signal causes that the processsManager to step immediately to the next Chain.
    Input collections can be either mongo collections or dataframes in the datastore.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param list collection_set: datastore keys holding the datasets to be checked. If any of these is empty, the chain is skipped.
        :param bool skip_chain_when_key_not_in_ds: skip the chain as well if the dataframe is not present in the datastore. \
        When True and if type is 'pandas.DataFrame', sents a SkipChain signal if key not in DataStore
        :param bool check_at_initialize: perform dataset empty is check at initialize. Default is true.
        :param bool check_at_execute: perform dataset empty is check at initialize. Default is false.
        """
        Link.__init__(self, kwargs.pop('name', 'SkipChainIfEmpty'))

        # process keyword arguments
        self._process_kwargs(kwargs, collection_set=[], skip_chain_when_key_not_in_ds=False, check_at_initialize=True,
                             check_at_execute=False)
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize the link."""
        if self.check_at_initialize:
            return self.check_collection_set()

        return StatusCode.Success

    def execute(self):
        """Execute the link.

        Skip to the next Chain if any of the input dataset is empty.
        """
        if self.check_at_execute:
            return self.check_collection_set()

        return StatusCode.Success

    def check_collection_set(self):
        """Check existence of collection in either mongo or datastore, and check that they are not empty.

        Collections need to be both present and not empty.

        - For mongo collections a dedicated filter can be applied before doing the count.
        - For pandas dataframes the additional option 'skip_chain_when_key_not_in_ds' exists. Meaning, \
        skip the chain as well if the dataframe is not present in the datastore.

        """
        # check if collection names are present in datastore
        ds = process_manager.service(DataStore)
        for k in self.collection_set:
            if k not in ds:
                if self.skip_chain_when_key_not_in_ds:
                    self.logger.warning('Key {key!s} not in DataStore. Sending skip chain signal.', key=k)
                    return StatusCode.SkipChain
                else:
                    raise Exception('Key "{key}" not in DataStore.'.format(key=k))
            df = ds[k]
            if len(df.index) == 0:
                self.logger.warning(
                    'pandas.DataFrame with datastore key "{key!s}" is empty. Sending skip chain signal.', key=k)
                return StatusCode.SkipChain

        return StatusCode.Success
