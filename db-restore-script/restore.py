from argparse import ArgumentParser
from subprocess import Popen, PIPE
from typing import Any, Dict
import os

from logging import basicConfig, INFO
import logging

basicConfig(level=INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_restore_properties_file(restore_file_path: Any) -> Dict[str, str]:
    restore_data: Dict[str, str] = {}
    with open(restore_file_path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            if not line:
                continue
            key, value = line.split('=')
            restore_data[key] = value
    return restore_data


def replace_username_psql_file(restore_properties_data: Dict[str, str]) -> None:
    with open(restore_properties_data['restore_filename'], 'r') as file:
        sql_content = file.read()

    modify_content = sql_content.replace(restore_properties_data['from_username'],
                                         restore_properties_data['to_username'])

    with open(restore_properties_data['restore_filename'], 'w') as file:
        file.write(modify_content)

    return


def create_command_to_restore(restore_properties_data: Dict[str, str]) -> str:
    file_name = restore_properties_data['restore_filename'].split('\\')[-1]

    restore_command: str = (
        f"{restore_properties_data['psql_path']}psql -h {restore_properties_data['database_server']} -d "
        f"{restore_properties_data['db']} --username {restore_properties_data['from_username']} --port "
        f"{restore_properties_data['port']} -f {file_name}")
    return restore_command


def run_restore_command(restore_command: str, restore_properties_data: Dict[str, str]):
    logging.info(f"psql restore started running command for the user : {restore_properties_data['from_username']}")
    base_path = os.getcwd()
    output_path = os.path.join(base_path, 'restore_output')
    os.makedirs(output_path, exist_ok=True)
    env = os.environ.copy()
    env['PGPASSWORD'] = restore_properties_data['password']

    process = Popen(f'cd {output_path} && {restore_command}',
                    shell=True,
                    stdout=PIPE,
                    stderr=PIPE,
                    text=True,
                    env=env)

    output, error = process.communicate()

    if process.returncode == 0:
        logging.info(f"Command '{restore_command}' executed successfully.")
    else:
        logging.error(f"Error: {error}")
        logging.error(f"Command '{restore_command}' failed with return code {process.returncode}.")


if __name__ == "__main__":
    parser = ArgumentParser(description='Restore PostgreSQL databases.')
    parser.add_argument('restore_file', help='The properties file to use for the restore.')
    args = parser.parse_args()
    restore_properties_file = args.restore_file
    restore_properties = read_restore_properties_file(restore_properties_file)
    replace_username_psql_file(restore_properties)
    restore_command = create_command_to_restore(restore_properties)
    run_restore_command(restore_command, restore_properties)
