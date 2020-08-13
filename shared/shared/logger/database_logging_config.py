"""
Class for Logging information to the database
by updating the contents of a cell
"""


class DatabaseLogger:
    """
    Externally facing logger that updates the logs
    associated with the specific cells in a database
    """
    def __init__(self, session):
        """
        :param session: SQLAlchemy Session to use for logging
        """
