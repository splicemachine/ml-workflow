"""
Module containing utilities for Splice Machine
MLFlow Store Support
"""


def _get_splicemachine_impl():
    """
    Return an Alembic Impl so
    Splice Machine migrations work
    """
    from alembic.ddl import impl
    return type('SpliceMachineImpl', (impl.DefaultImpl, object),
                {'__dialect__': 'splicemachinesa', 'transactional_ddl': False})


SpliceMachineImpl = _get_splicemachine_impl()
