# tar Program
# Given an input file consisting of file sizes, and paths to files (can be relative) 
#      create a tar archive in the destination from every TARRBALL_SIZE_LIMIT files 
# Brandon White, 2022

import argparse
import getpass
import logging
import math
import os
import os.path
import subprocess
import tempfile
from multiprocessing import Process, Queue

logging.basicConfig(format='%(asctime)-15s %(name)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger()

ONE_TERABYTE = int(math.pow(1024, 4))
TARBALL_SIZE_LIMIT = ONE_TERABYTE

class Sentinel:
    pass

def execute_tar(pid, tarlist_tempfile_path, archive_dest_path, fail_logger):
    logger.info(f'(pid:{pid}) Executing tar of files specified in {tarlist_tempfile_path} and compressing to archive {archive_dest_path}')
    tar_process = subprocess.Popen([
        'tar',
        '--create', # --preserve-permissions is implied by execution as a superuser
        f'--file={archive_dest_path}',
        f'--files-from={tarlist_tempfile_path}',
        '--atime-preserve', # preserve access times 
        '--dereference', # Follow symlinks, and build their referents into the tarchive
        '--gzip', # PHENOMENAL COSMIC POWER! itty bitty living space
        '--verbose' # let us know what's going on
    ], stdout=subprocess.PIPE)
    tar_stdout, tar_stderr = tar_process.communicate()
    logger.info(f'(pid:{pid}): TAR OUTPUT: {tar_stdout.decode().strip()}')
    if tar_process.returncode != 0:
        logger.info(f'!!!  TAR FAILURE  !!!')
        with open(tarlist_tempfile_path, 'r') as tarlist:
            for missed_file in tarlist:
                logger.error(f'Failed to include file {missed_file} in archive {archive_dest_path}')
                fail_logger.error(missing_file.encode(encoding='UTF-8'))

def do_processing(pid, tar_queue, args):
    # Create a tempfile for storing the accumulating list of files to be tarred
    tarlist_tempfile = tempfile.NamedTemporaryFile(prefix=args.tar_prefix, dir=args.tar_dest_dir) 
    tarlist_tempfile_path = tarlist_tempfile.name # absolute path to NamedTemporaryFile
    archive_dest_path = tarlist_tempfile_path + '.tar.gz' # Name of final archive output by the program
    logger.info(f'Opened new tarlist at: {tarlist_tempfile_path}')

    # Setup logging for failure on a per-tempfile basis
    fail_log_path = archive_dest_path + '.error' # Name of per-archive failure logs

    # Error logging
    fail_logger = logging.getLogger('fail_log')
    fh = logging.FileHandler(fail_log_path)
    fail_logger.addHandler(fh)

    tar_rolling_size = 0 # Hold the accumulation of the list of files to be tarred in this batch
    while True:
        tar_info = tar_queue.get()
        if isinstance(tar_info, Sentinel):
            tarlist_tempfile.flush()
            execute_tar(pid, tarlist_tempfile_path, archive_dest_path, fail_logger) # DO THE TAR
            tarlist_tempfile.close()
            logger.info(f'PID: {pid} complete. Waiting to join.')
            return

        tar_info = tar_info.split()
        file_size = int(tar_info[0])
        file_path = tar_info[1] + '\n'

        if tar_rolling_size < TARBALL_SIZE_LIMIT:
            tar_rolling_size += file_size
            b_file_path = file_path.encode(encoding='UTF-8')
            tarlist_tempfile.write(b_file_path)

        elif tar_rolling_size >= TARBALL_SIZE_LIMIT:
            # Flush I/O buffer to file
            tarlist_tempfile.flush()
            try:
                execute_tar(pid, tarlist_tempfile_path, archive_dest_path, fail_logger) # DO THE TAR
            except Exception:
                execute_tar(pid, tarlist_tempfile_path, archive_dest_path, fail_logger) # DO THE TAR
                tarlist_tempfile.close() # Clean up if we die
            tarlist_tempfile.close() # Close and delete tarlist upon successfull tar
            tarlist_tempfile = tempfile.NamedTemporaryFile(prefix=args.tar_prefix, dir=args.tar_dest_dir) # Open up the next tempfile
            tarlist_tempfile_path = tarlist_tempfile.name
            archive_dest_path = tarlist_tempfile_path + '.tar.gz' # Name of final archive output by the program
            logger.info(f'Opened new tarlist at: {tarlist_tempfile_path}')
            b_file_path = file_path.encode(encoding='UTF-8')
            tarlist_tempfile.write(b_file_path)
            tar_rolling_size = file_size # Reset the rolling sum, add the file that can't fit to the new list
        else:
            logger.error('Uh, this should not happen.')

def get_program_arguments():
    parser = argparse.ArgumentParser(description='Transfers a directory from a remote host(s) in parallel to a given local filesystem using rsync')
    parser.add_argument('file_info_f', type=str, help='File containing one file path on the remote host per line')
    parser.add_argument('--num-procs', type=int, default=1, help='Number of procs to divy up lines .')
    parser.add_argument('--tar-prefix', type=str, default='tar_', help='Name to append to tempfiles used to track files to be tarred in a batch.')
    parser.add_argument('--tar-dest-dir', type=str, default='/tmp', help='Number of procs to divy up lines.')
    args = parser.parse_args()
    return args

def main():
    args = get_program_arguments()
    logstr = f'Executing tar operation with tarball size limit: {TARBALL_SIZE_LIMIT}'
    logger.info(logstr)

    procs = []
    tar_queue = Queue(args.num_procs)
    logger.info(f'Starting {args.num_procs} procs')
    for i in range(args.num_procs):
        p = Process(target=do_processing, args=(i, tar_queue, args))
        p.start()
        procs.append(p)

    # See if there is a point in the input file we should resume at
    start_from = 0
    fmarkfile_path = f'{args.file_info_f}.resume'
    try:
        logger.info(f'Checking {fmarkfile_path} for a line offset...')
        with open(fmarkfile_path, 'r') as fmarkfile:
            start_from = int(fmarkfile.read())
    except OSError:
        pass
    logger.info(f'Starting processing from {start_from} lines into the input file...')

    with open(args.file_info_f) as f:
        for i, item in enumerate(f):
            if i < start_from:
                if i < 10 or i % 25000 == 0:
                    logger.info(f'Scrolling to where we left off...')
                continue
            tar_item = item.strip()
            tar_queue.put(tar_item)
            if i % 500 == 0:
                with open(fmarkfile_path, 'w') as fmarkfile_f:
                    fmarkfile_f.write(str(i))
            
    logger.info('All transfer items produced to consumer processes. Dispatching Sentinel.')
    for i in range(args.num_procs):
        tar_queue.put(Sentinel())
    for p in procs:
        p.join()

if __name__ == "__main__":
    main()
