from sqlalchemy.orm import Session
from typing import List, Dict
from . import schemas
from .constants import SQL
from shared.models import feature_store_models
from shared.services.database import SQLAlchemyClient
from shared.logger.logging_config import logger

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
    assert not SQLAlchemyClient.engine.dialect.has_table(SQLAlchemyClient.engine, fset.table_name, fset.schema_name), str
    # Validate metadata
    assert len(get_feature_sets(db, _filter={'table_name': fset.table_name, 'schema_name': fset.schema_name})) == 0, str

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

    for fs in feature_set_rows.collect():
        d = fs.asDict()
        pkcols = d.pop('pk_columns').split('|')
        pktypes = d.pop('pk_types').split('|')
        d['primary_keys'] = {c: k for c, k in zip(pkcols, pktypes)}
        feature_sets.append(schema.FeatureSet(**d))
    return feature_sets

def register_metadata(db: Session, fset: feature_store_models.FeatureSet):
    fset_metadata = SQL.feature_set_metadata.format(schema=fset.schema_name, table=fset.table_name,
                                                    desc=fset.description)

    db.execute(fset_metadata)
    fsid = db.execute(SQL.get_feature_set_id.format(schema=fset.schema_name,
                                                            table=fset.table_name)).collect()[0][0]
    fset.feature_set_id = fsid

    for pk in fset.pk_columns:
        pk_sql = SQL.feature_set_pk_metadata.format(
            feature_set_id=fsid, pk_col_name=pk.upper(), pk_col_type=fset.primary_keys[pk]
        )
        db.execute(pk_sql)