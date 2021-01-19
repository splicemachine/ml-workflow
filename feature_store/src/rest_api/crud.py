from sqlalchemy.orm import Session
from typing import List, Dict
from . import schemas
from .constants import SQL
from shared.models import feature_store_models
from shared.services.database import SQLAlchemyClient
from shared.logger.logging_config import logger
import re

def get_db():
    db = SQLAlchemyClient.SessionFactory()
    try:
        yield db
    except Exception as e:
        logger.error(f'Encountered error - {e}')
        db.rollback()
    else:
        logger.info("Committing...")
        db.commit()
        logger.info("Committed")
    finally:
        logger.info("Closing session")
        db.close()
        SQLAlchemyClient.SessionFactory.remove()

def validate_feature_set(db: Session, fset: schemas.FeatureSetCreate):
    """
    Asserts a feature set doesn't already exist in the database
    :param schema_name: schema name of the feature set
    :param table_name: table name of the feature set
    :return: None
    """
    logger.info("Validating Schema")
    str = f'Feature Set {fset.schema_name}.{fset.table_name} already exists. Use a different schema and/or table name.'
    # Validate Table
    assert not table_exists(fset.schema_name, fset.table_name), str
    # Validate metadata
    assert len(get_feature_sets(db, _filter={'table_name': fset.table_name, 'schema_name': fset.schema_name})) == 0, str

def validate_feature(db: Session, name: str):
    """
    Ensures that the feature doesn't exist as all features have unique names
    :param name: the Feature name
    :return:
    """
    # TODO: Capitalization of feature name column
    # TODO: Make more informative, add which feature set contains existing feature
    str = f"Cannot add feature {name}, feature already exists in Feature Store. Try a new feature name."
    l = len(db.execute(SQL.get_all_features.format(name=name.upper())).fetchall())
    assert l == 0, str

    if not re.match('^[A-Za-z][A-Za-z0-9_]*$', name, re.IGNORECASE):
        raise SpliceMachineException('Feature name does not conform. Must start with an alphabetic character, '
                                     'and can only contains letters, numbers and underscores')

def get_feature_sets(db: Session, feature_set_ids: List[int] = None, _filter: Dict[str, str] = None) -> List[schemas.FeatureSet]:
    """
    Returns a list of available feature sets

    :param feature_set_ids: A list of feature set IDs. If none will return all FeatureSets
    :param _filter: Dictionary of filters to apply to the query. This filter can be on any attribute of FeatureSets.
        If None, will return all FeatureSets
    :return: List[FeatureSet] the list of Feature Sets
    """
    feature_sets = []
    feature_set_ids = feature_set_ids or []
    _filter = _filter or {}

    sql = SQL.get_feature_sets

    # Filter by feature_set_id and filter
    if feature_set_ids or _filter:
        sql += ' WHERE '
    if feature_set_ids:
        fsd = tuple(feature_set_ids) if len(feature_set_ids) > 1 else f'({feature_set_ids[0]})'
        sql += f' fset.feature_set_id in {fsd} AND'
    for fl in _filter:
        sql += f" fset.{fl}='{_filter[fl]}' AND"
    sql = sql.rstrip('AND')

    feature_set_rows = db.execute(sql)
    for fs in feature_set_rows.fetchall():
        d = { k.lower(): v for k, v in fs.items() }
        pkcols = d.pop('pk_columns').split('|')
        pktypes = d.pop('pk_types').split('|')
        d['primary_keys'] = {c: k for c, k in zip(pkcols, pktypes)}
        feature_sets.append(schemas.FeatureSet(**d))
    return feature_sets

def register_feature_set_metadata(db: Session, fset: schemas.FeatureSetCreate):
    fset_metadata = SQL.feature_set_metadata.format(schema=fset.schema_name, table=fset.table_name,
                                                    desc=fset.description)

    db.execute(fset_metadata)
    fsid_results = db.execute(SQL.get_feature_set_id.format(schema=fset.schema_name,
                                                            table=fset.table_name))
    fsid = fsid_results.fetchone().values()[0]

    logger.info(f'Found Feature Set ID {fsid}')

    fsid_results.close()

    db.execute(SQL.get_feature_set_id.format(schema=fset.schema_name,
                                                            table=fset.table_name))

    for pk in list(fset.primary_keys.keys()):
        pk_sql = SQL.feature_set_pk_metadata.format(
            feature_set_id=fsid, pk_col_name=pk.upper(), pk_col_type=fset.primary_keys[pk]
        )
        db.execute(pk_sql)
    return schemas.FeatureSet(**fset.__dict__, feature_set_id=fsid)

def register_feature_metadata(db: Session, f: schemas.FeatureCreate):
    """
    Registers the feature's existence in the feature store
    """
    feature_sql = SQL.feature_metadata.format(
        feature_set_id=f.feature_set_id, name=f.name, desc=f.description,
        feature_data_type=f.feature_data_type,
        feature_type=f.feature_type, tags=','.join(f.tags) if isinstance(f.tags, list) else f.tags
    )
    db.execute(feature_sql)

def table_exists(schema_name, table_name):
    return SQLAlchemyClient.engine.dialect.has_table(SQLAlchemyClient.engine, table_name, schema_name)