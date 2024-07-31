import argparse
import os
import tarfile
from datetime import datetime
import subprocess
import logging
import coloredlogs
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
coloredlogs.install(level='DEBUG')

# Email configuration details
SMTP_SERVER = 'smtp.rr-its.com'
SMTP_PORT = 25
SMTP_USERNAME = ''
SMTP_PASSWORD = ''
SMTP_FROM = 'sparchuri@redrockitsolutions.com'
SMTP_TO = 'sparchuri@redrockitsolutions.com'

# s3_file_details
s3_FILE_NAME = "//imagework-backup/postgres-dbbackup/"


def read_properties_file(properties_file):
    common_info = {}
    user_info_list = []
    # read txt file
    with open(properties_file) as file:
        lines = file.readlines()
    for line in lines:
        line = line.strip()
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
    success = []
    failure = []
    date_string = datetime.now().strftime("%Y%m%d.%H%M%S")
    base_path = os.getcwd()
    output_path = os.path.join(base_path, 'output')
    os.makedirs(output_path, exist_ok=True)

    for user_data in user_info_details:
        sql_command = (f"{common_info_dict['psql_path']}pg_dump -h {common_info_dict['database_server']} "
                       f"-d {user_data['db']} --username {user_data['username']} --port {common_info_dict['port']} "
                       f"--no-owner --schema {user_data['username']} > {user_data['username']}_{date_string}.sql")

        cmd_success, cmd_failure = run_command(sql_command, output_path, user_data)
        success.extend(cmd_success)
        failure.extend(cmd_failure)
    return success, failure


def send_email(subject, body):
    msg = MIMEMultipart()
    msg['From'] = SMTP_FROM
    msg['To'] = SMTP_TO
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'html'))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            if SMTP_USERNAME and SMTP_PASSWORD:
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM, SMTP_TO, msg.as_string())
        logging.info('Email sent successfully.')
    except Exception as e:
        logging.error(f"Failed to send email: {e}")


def run_command(command, path, user_data):
    success = []
    failure = []
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
        success.append(f"{user_data['username']} - Success")
    else:
        # send email with failure message
        logging.error(f"Error: {error}")
        failure.append(f"{user_data['username']} - Failure with error: {error}")

    return success, failure


def create_tar_file(output_path, file_name):
    file_name = os.path.basename(file_name)
    date_string = datetime.now().strftime("%Y%m%d.%H%M%S")
    tar_file_name = f"{os.path.splitext(file_name)[0]}_{date_string}.tar.gz"
    tar_file_path = os.path.join(os.getcwd(), "output_tar_files")
    os.makedirs(tar_file_path, exist_ok=True)
    tar_file_path = os.path.join(tar_file_path, tar_file_name)

    total_size = 0
    for root, dirs, files in os.walk(output_path):
        for file in files:
            if file.endswith('.sql'):
                total_size += os.path.getsize(os.path.join(root, file))

    progress_size = 0
    with tarfile.open(tar_file_path, 'w:gz') as tar:
        for root, dirs, files in os.walk(output_path):
            for file in files:
                if file.endswith('.sql'):
                    file_path = os.path.join(root, file)
                    tar.add(file_path, arcname=file)
                    progress_size += os.path.getsize(file_path)
                    progress_percentage = (progress_size / total_size) * 100
                    logging.info(f"Tar file creating... {progress_percentage:.2f}%")

    logging.info(f"Tar file '{tar_file_name}' created successfully.")
    return tar_file_path


def delete_files_in_folder(output_path, extension = None):
    for root, dirs, files in os.walk(output_path):
        for file in files:
            if extension is None or file.endswith(extension):
                os.remove(os.path.join(root, file))
    logging.info(f"All files {'' if extension is None else 'with extension ' + extension} deleted successfully.")


def upload_to_s3(tar_file_path):
    s3_command = f"aws s3 cp {tar_file_path} s3:{s3_FILE_NAME} --quiet --only-show-errors"
    process = subprocess.Popen(s3_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output, error = process.communicate()

    if process.returncode == 0:
        logging.info(f"File '{os.path.basename(tar_file_path)}' uploaded to S3 successfully.")
        return True
    else:
        logging.error(f"Failed to upload file to S3: {error}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Backup PostgreSQL databases.')
    parser.add_argument('properties_file', help='The properties file to use for the backup.')
    args = parser.parse_args()
    properties_file = args.properties_file
    common_info, user_info_list = read_properties_file(properties_file)
    success_list, failure_list = create_command_for_backup(common_info, user_info_list)
    tar_file_path = create_tar_file(os.path.join(os.getcwd(), 'output'), properties_file)



    # Email content
    body = ""
    subject = "Backup Success"
    if success_list:
        body += "<p style='color:green;'>Success:</p><ul>"
        for msg in success_list:
            body += f"<li style='color:green;'>{msg}</li>"
        body += "</ul>"

    if failure_list:
        subject = "Backup Failure"
        body += "<p style='color:red;'>Failure:</p><ul>"
        for msg in failure_list:
            body += f"<li style='color:red;'>{msg}</li>"
        body += "</ul>"

    send_email(subject, body)

    if upload_to_s3(tar_file_path):
        delete_files_in_folder(os.path.join(os.getcwd(), 'output'), extension = ".sql")
        delete_files_in_folder(os.path.join(os.getcwd(), 'output_tar_files'), extension='.tar.gz')


