import argparse
import os
from datetime import datetime
import subprocess
import logging
import platform

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_properties_file(properties_file):
    common_info = {}
    user_info_list = []
    # read txt file
    with open(properties_file) as file:
        lines = file.readlines()
    for line in lines:
        # remove whitespace characters like `\n` at the end of each line
        line = line.strip()
        # skip empty lines
        if not line:
            continue

        if "=" in line and "||" not in line:
            key, value = line.split("=")
            common_info[key] = value


        elif "||" in line:
            user_info = {}
            pairs = line.split("||")
            for pair in pairs:
                key, value = pair.split("=")
                user_info[key] = value

            user_info_list.append(user_info)

    return common_info, user_info_list


def create_command_for_backup(common_info_dict, user_info_details):
    date_string = datetime.now().strftime("%Y%m%d.%H%M%S")
    base_path = os.getcwd()
    output_path = os.path.join(base_path, 'output')
    os.makedirs(output_path, exist_ok=True)

    for user_data in user_info_details:
        sql_command = (f"{common_info_dict['psql_path']}pg_dump -h {common_info_dict['database_server']} "
                       f"-d {user_data['db']} --username {user_data['username']} --port {common_info_dict['port']} "
                       f"--no-owner --schema {user_data['username']} > {user_data['username']}_{date_string}.sql")

        run_command(sql_command, output_path, user_data)


def run_command(command, path, user_data):
    logging.info(f"psql backup started running command for the user : {user_data['username']}")
    env = os.environ.copy()
    env['PGPASSWORD'] = user_data['password']

    process = subprocess.Popen(f'cd {path} && {command}',
                               shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               text=True,
                               env=env)

    output, error = process.communicate()

    if process.returncode == 0:
        logging.info(f"Command '{command}' executed successfully.")
    else:
        logging.error(f"Error: {error}")
        logging.error(f"Command '{command}' failed with return code {process.returncode}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Backup PostgreSQL databases.')
    parser.add_argument('properties_file', help='The properties file to use for the backup.')
    args = parser.parse_args()
    properties_file = args.properties_file
    common_info, user_info_list = read_properties_file(properties_file)
    create_command_for_backup(common_info, user_info_list)
