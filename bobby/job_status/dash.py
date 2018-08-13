import json
import logging
import os
from collections import namedtuple
from datetime import datetime

import cachetools.func
import jaydebeapi
from flask import Flask, render_template

logging.basicConfig()
logger = logging.getLogger('dash')
logger.setLevel(logging.DEBUG)

app = Flask(__name__)


class JDBCUtils(object):
    """A Class containing some utils to make connecting with JDBC (via Python) easier."""
    jdbc_url = os.environ.get('JDBC_URL')
    username = os.environ.get('USER')
    password = os.environ.get('PASSWORD')
    last_authenticated = None
    cursor = None
    Job = namedtuple('Job', ['job_id', 'timestamp', 'handler', 'status', 'payload', 'info'])

    @staticmethod
    def _renew():
        """Establish a new JDBC Connection to the server
        :return: nothing
        """
        jdbc_conn = jaydebeapi.connect("com.splicemachine.db.jdbc.ClientDriver",
                                       JDBCUtils.jdbc_url,
                                       {'user': JDBCUtils.username,
                                        'password': JDBCUtils.password, 'ssl': "basic"},
                                       "../utilities/db-client-2.7.0.1815.jar")
        JDBCUtils.cursor = jdbc_conn.cursor()
        JDBCUtils.last_authenticated = datetime.today()

    @staticmethod
    def renew_jdbc_connection():
        """Refresh every 51 mins, so that we won't time out JDBC connection
        :return: nothing
        """
        if JDBCUtils.last_authenticated:
            logger.debug('Checking JDBC Connection')
            last_renewed_diff = datetime.today() - JDBCUtils.last_authenticated

            logger.debug('minutes since last renewal ' + str(last_renewed_diff.total_seconds() /
                                                             60))
            if last_renewed_diff.total_seconds() / 60 >= 51:
                logger.debug('Renewing JDBC Connection')
                JDBCUtils._renew()

        else:
            logger.debug('Renewing JDBC Connection')
            JDBCUtils._renew()

    @staticmethod
    @cachetools.func.ttl_cache(maxsize=1024, ttl=10)
    def get_jobs():
        """Get Jobs From Splice Machine DB (cache results for 10 secs so page won't load extremely
        slowly every time
        :return: list of namedtuples containing jobs, or False if exception

        """
        JDBCUtils.renew_jdbc_connection()
        logger.debug('Getting Jobs')
        try:
            JDBCUtils.cursor.execute('SELECT * FROM ML.JOBS ORDER BY TIMESTAMP DESC')
            results = JDBCUtils.cursor.fetchall()
            old_jobs = [JDBCUtils.Job(*job) for job in results]
            jobs = [
                JDBCUtils.Job(o.job_id,
                              datetime.fromtimestamp(o.timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                              o.handler,
                              o.status,
                              json.loads(o.payload),
                              o.info)
                for o in old_jobs
            ]

            return jobs

        except Exception as e:
            logger.error(e)
            return False


@app.route('/', methods=['GET'])
@app.route('/dash', methods=['GET'])
def dash():
    """Flask Dashboard route -- renders HTML template dash.html. Can be accessed via "/" or "/dash"
    on the port it is running
    :return: HTML file rendered by browser

    """
    return render_template('dash.html', jobs=JDBCUtils.get_jobs())


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
