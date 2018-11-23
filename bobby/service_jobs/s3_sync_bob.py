#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
The S3 Daemon for both downloading and uploading (syncing) a local directory with S3. Was
intended for allowing the Splice Machine Zeppelin Container to talk to Splice Machine MLFlow
container
"""

import argparse
import os
import subprocess
import time

__author__ = "Splice Machine, Inc."
__copyright__ = "Copyright 2018, Splice Machine Inc. All Rights Reserved"
__credits__ = ["Amrit Baveja", "Murray Brown", "Monte Zweben"]

__license__ = "Commerical"
__version__ = "2.0"
__maintainer__ = "Amrit Baveja"
__email__ = "abaveja@splicemachine.com"
__status__ = "Quality Assurance (QA)"


class Utils(object):
    """
    A couple of utility functions
    """

    @staticmethod
    def check_output_code(command_name, code):
        """
        Make sure a command executed from Python is successful

        :param command_name: name to print out
        :param code: returned exit code
        :return: exception if code is not 0
        """
        if code != 0:
            raise Exception(
                command_name +
                ' finished with exit code ' +
                str(code))


class S3Daemon(object):
    """
    S3 Daemon was written for sharing MLFlow model metadata between the
    client and server containers. This can sync the MLFlow local directory
    with S3 every --loop seconds, or only once
    """

    @staticmethod
    def input_parser():
        """
        Get input from the user
        :return: parsed args
        """
        description = S3Daemon.__doc__  # use the docstring from the class as the description

        parser = argparse.ArgumentParser(description=description)

        parser.add_argument(
            'role',
            choices=[
                'upload',
                'download'],
            help='Either "upload" or "download" (eg. zeppelin is upload, mlflow client is '
                 'download)')

        parser.add_argument(
            '--bucket_url',
            '-b',
            help='<Required> The S3 Bucket URL where MLFlow model metadata should '
                 'be stored',
            required=True)

        parser.add_argument(
            "--mlflow_dir",
            "-m",
            help="<Required> The directory where MLFlow is writing metadata (including artifacts. "
                 "YOU MUST SET THIS AS THE SAME DIRECTORY TO WHICH MLFLOW IS WRITING ON BOTH "
                 "SERVERS! (mlflow.set_tracking_uri(<mlflow_dir>) in Python3 works)",
            required=True)

        parser.add_argument(
            "--intermediate_dir",
            '-i',
            help='The intermediate copy dir (only for download),'
                 'so we can change metadata')

        parser.add_argument(
            '--loop_interval',
            '-l',
            type=int,
            help='If present, how many seconds to wait '
                 'between S3 Syncs. If not present, will'
                 ' sync once and then exit.')

        parser.add_argument(
            '--version',
            action='version',
            version='Version: ' + __version__)

        return parser.parse_args()

    @staticmethod
    def loop(arguments):  # If you would like to keep checking and syncing continuously
        """
        Continuously check and sync every --loop seconds
        :param arguments: parsed args from user
        :return: n/a
        """
        if arguments.role == 'upload':  # download current S3 first time
            # sync as download daemon one time
            S3Daemon.sync(arguments, override_role=True)

        while True:
            S3Daemon.sync(arguments)  # run one time (but in an infinite loop)
            # Wait loop seconds before checking again
            time.sleep(arguments.loop_interval)

    @staticmethod
    def format_for_s3(role, path, root=True):
        """
        Put a file inside each empty directory so S3 doesn't delete it
        :param role: either upload or download
        :param path: starting directory
        :param root: whether or not to put an empty file in the root directory
        :return: n/a
        """
        if not os.path.isdir(path):  # base case
            return

        files = os.listdir(path)
        if files:  # if there are files in the dir

            for dirfile in files:
                fullpath = os.path.join(path, dirfile)  # get the full path

                if os.path.isdir(fullpath):  # check whether it is a
                    # Recursively format for s3 on all files
                    S3Daemon.format_for_s3(role, fullpath)
                    # until we hit a directory

        if root:
            if role == 'upload':  # check role
                S3Daemon.create_fake_metadata(path)  # create fake metadata
            else:
                S3Daemon.remove_fake_metadata(path)  # remove fake metadata

    @staticmethod
    def create_fake_metadata(path):
        """
        Create fake metadata so S3 won't delete folders
        :param path:
        :return:
        """
        if path.endswith('artifacts') or path.endswith(
                'artifacts/') and not os.path.isfile(path + '/upload_sp1c3.txt'):
            # write a fake artifact to a file so S3 won't ignore an empty
            # directory

            create_fake_artifact_code = os.system(
                'echo Hello World > ' + path + '/upload_sp1c3.txt')

            Utils.check_output_code(
                'fake artifact create',
                create_fake_artifact_code)

            # write a fake parameter
        elif path.endswith('params') or path.endswith('params/') and not \
                os.path.isfile(path + '/z_locz'):  # some random string a user will never upload
            # to a file so S3 won't ignore the empty directory

            create_fake_param_code = os.system('echo s3 > ' + path + '/z_locz')

            Utils.check_output_code(
                'fake param create',
                create_fake_param_code)

        elif path.endswith('metrics') or path.endswith('metrics/') and not os.path.isfile(
                path + '/valideeeto5'):
            # write a fake metric to  a file so S3 won't ignore the empty
            # directory

            create_fake_metric_code = os.system(
                'echo 1516407499 1 > ' + path + '/valideeeto5')

            Utils.check_output_code(
                'fake metric create',
                create_fake_metric_code)

    @staticmethod
    def remove_fake_metadata(path):
        """
        Remove Fake metadata generated by host before S3 upload
        :param path: intermediate dir on target
        :return: n/a
        """
        if os.path.isfile(path + '/upload_sp1c3.txt'):
            # delete fake artifacts
            delete_artifact_code = subprocess.call(
                ['rm', path + '/upload_sp1c3.txt'])
            Utils.check_output_code(
                'fake artifact delete',
                delete_artifact_code)

        elif os.path.isfile(path + '/z_locz'):
            # delete fake params
            delete_fake_param_code = subprocess.call(['rm', path + '/z_locz'])
            Utils.check_output_code(
                'fake param delete',
                delete_fake_param_code)

        elif os.path.isfile(path + '/valideeeto5'):
            # delete fake metrics
            delete_fake_metric_code = subprocess.call(
                ['rm', path + '/valideeeto5'])
            Utils.check_output_code(
                'fake metric delete',
                delete_fake_metric_code)

    @staticmethod
    def clean_upload(arguments):
        """
        Clean and create fake data
        :param arguments: user inputs
        :return: n/a
        """
        S3Daemon.format_for_s3(
            'upload', arguments.mlflow_dir)  # Create fake metadata

    @staticmethod
    def clean_download(arguments):
        """
        Clean and undo fake data made by uploader
        :param arguments: user inputs
        :return: n/a
        """
        S3Daemon.format_for_s3(
            'download',
            arguments.intermediate_dir)  # remove fake metadata

        sync_dir_cmd = subprocess.call(['rsync',
                                        '-tr',
                                        arguments.intermediate_dir + '/',
                                        arguments.mlflow_dir])
        Utils.check_output_code('rsync to dir', sync_dir_cmd)

    # Create fake metadata so S3 won't delete files

    @staticmethod
    def clean_and_format(arguments):
        """
        Create an intermediate directory so MLFlow doesn't throw a tantrum when its metadata is
        changed.
        :param arguments: user inputs
        :return: the intermediate directory path
        """

        if arguments.role == 'upload':
            S3Daemon.clean_upload(arguments)  # clean for uploading

        else:
            S3Daemon.clean_download(arguments)

    @staticmethod
    def sync(arguments, override_role=False):
        # If you would only like to run once (case: Zeppelin button scenario)
        """
        Check once and sync
        :param arguments: parsed args from user
        :param override_role: whether or not to force a download regardless of the input params
        :return: n/a (exception if process code != 0)
        """
        if override_role or arguments.role == 'download':
            # whether we are initially downloading the current S3 contents first
            # Execute download bash
            with open('/tmp/download.log', 'w') as log_file:
                if not override_role:
                    process_code = subprocess.call(['aws', 's3', 'sync',
                                                    arguments.bucket_url,
                                                    arguments.intermediate_dir,
                                                    '--size-only'], stdout=log_file)
                    S3Daemon.clean_and_format(arguments)
                else:
                    process_code = subprocess.call(['aws', 's3', 'sync',
                                                    arguments.bucket_url,
                                                    arguments.mlflow_dir], stdout=log_file)
        else:
            S3Daemon.clean_and_format(arguments)
            with open('/tmp/upload.log', 'w') as log_file:
                process_code = subprocess.call(['aws',
                                                's3',
                                                'sync',
                                                arguments.mlflow_dir,
                                                arguments.bucket_url,
                                                '--delete',
                                                '--size-only'], stdout=log_file)  # Execute upload
            # bash command

        Utils.check_output_code('aws s3 sync', process_code)

    @staticmethod
    def main():
        """
        Main Function
        """
        args = S3Daemon.input_parser()  # Get inputs from user
        if args.loop_interval:  # Check whether we are running continuously
            S3Daemon.loop(args)
        else:
            S3Daemon.sync(args)  # Check once and exit


if __name__ == '__main__':
    S3Daemon.main()
