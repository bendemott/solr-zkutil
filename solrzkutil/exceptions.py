class SolrCloudHealthError(Exception):
    """
    Top level exception, for which all health check exceptions inherit.
    """

class EnsembleStateError(SolrCloudHealthError):
    """
    Top level exception describing state errors.
    There is an invalid or incorrect state in a znode, its contents, or attribute.
    """

class EnsembleInconsistency(SolrCloudHealthError):
    """
    Top level exception for inconsistency that spans 2 or more zkhosts (the entire ensemble is inconsistent)
    """

class SolrZkStateFailure(SolrCloudHealthError):
    """
    Holds exception info for any form of comprehensive Solr state failure. 
    Solr state is completely missing for a solr host that is part of the ensemble.
    """

class SolrZkStateInconsitency(SolrCloudHealthError):
    """
    The state between solr and zookeeper is inconsistent
    """

class EnsembleLiveNodeInconsistency(EnsembleInconsistency):
    """
    Live nodes are used by solr to track live node information.
    """

class EnsembleDumpOutputInconsistency(EnsembleInconsistency):
    """
    The output of the administrative command 'dump' is not consistent between
    ensemble members.
    """
    pass

class EnsembleWatchOutputInconsistency(EnsembleInconsistency):
    """
    The output of the various watch administrative commands is not consistent between
    ensemble members.
    """
    pass

class EnsembleEphemeralInconsistency(EnsembleInconsistency):
    """
    There is an ephemeral znode that is not consistent across Zookeeper ensemble movies.
    """
    pass

class EnsembleWatchInconsistency(EnsembleInconsistency):
    """
    General watch inconsistency. More specific exceptions inherit this exception.
    """
    pass

class EnsembleCollectionStateInconsistency(EnsembleInconsistency):
    """
    Collection state.json contains inconsistent information
    """

class EnsembleRegistrationFailure(Exception):
    """
    Special exception, a 1 or more solr sessions/hosts is completely missing registration of
    watches, ephemerals, etc.
    """
    pass

class SolrWatchMissing(EnsembleStateError):
    """
    A known watch that should be present is missing, this error is thrown when a watch is missing
    altogether, not present on any Zk ensemble member.
    """
    pass

class SolrEphemeralMissing(EnsembleStateError):
    """
    A known ephemeral is missing that should be present, this error is thrown when a znode ephemeral
    should be present, but is missing from the cluster.
    """

class EnsembleSessionInvalid(EnsembleStateError):
    """
    Invalid or non-existent session is referenced by a znode.
    """

class EnsembleZnodeNameSessionInvalid(EnsembleSessionInvalid):
    """
    Znode name contains reference to a session that does not exist.
    """

class EnsembleSessionWatchInconsistency(EnsembleWatchInconsistency):
    """
    Each solr host should have a consistent set of watches present. 
    Each session associated with a solr host, should have a known, and valid set
    of watches present.
    """

class EnsembleSolrJSessionWatchInconsistency(EnsembleWatchInconsistency):
    """
    SolrJ client sessions should contain consistent watches, for specific files.
    A session has inconsistent watches, or is not watching something it should be.
    """

class EnsembleDirtyOrInvalidZnode(EnsembleInconsistency):
    """
    A znode is present within a directory that should not be. 
    This error can be thrown when a znode path should contain ONLY ephemerals, bu
    contains other znodes.  Or a path should contain only specific 
    """

class EnsembleOrphanedZnode(EnsembleDirtyOrInvalidZnode):
    """
    Thrown when a particular Zk host has a znode but no other ensemble members do. 
    This znode has been orphaned.
    """

class EnsembleOrphanedEphemeralZnode(EnsembleDirtyOrInvalidZnode):
    """
    Thrown when an ephemeral node is inconsistent between ensemble members.
    This ephemeral znode has been orphaned.  Possibly caused by various ephemeral bugs within ZK.
    This is the opposite of an inconsistency error... This means an extra znode is present that should
    not be present
    """