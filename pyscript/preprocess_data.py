import pandas as pd
import re
import sys

WEB_LOG_PATH = sys.argv[1]
PREPROCESSED_FILE_PATH = sys.argv[2]

header_list = ['time', 'elb', 'client:port', 'backend:port', 'request_processing_time',
               'backend_processing_time', 'response_processing_time', 'elb_status_code',
               'backend_status_code', 'received_bytes', 'sent_bytes', 'request', 'user_agent',
               'ssl_cipher', 'ssl_protocol']


def remove_port(ip):
    return re.match(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', ip)[0]


def get_url(request):
    return re.search(r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\(["
                     r"^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))",
                     request)[0]


if __name__ == "__main__":
    session_raw_df = pd.read_csv(WEB_LOG_PATH,
                                 names=header_list,
                                 parse_dates=['time'],
                                 delimiter=' ',
                                 compression='gzip')
    session_sorted_df = session_raw_df.sort_values(by='time', ignore_index=True)
    session_sorted_df['client_ip'] = session_sorted_df['client:port'].apply(remove_port)
    session_sorted_df['url'] = session_sorted_df['request'].apply(get_url)
    session_sorted_df[['time', 'client_ip', 'url']].to_csv(PREPROCESSED_FILE_PATH, header = False, index=False)
