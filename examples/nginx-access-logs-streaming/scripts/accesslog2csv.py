# Adapted from: https://gist.github.com/joswr1ght/c2e08f520933bb36c0b19aa0dcb6a173
# accesslog2csv: Convert default, unified access log from Apache, Nginx
# servers to CSV format.
#
# Original source by Maja Kraljic, July 18, 2017
# Modified by Joshua Wright to parse all elements in the HTTP request as
# different columns, December 16, 2019


import csv
import re
import sys

if len(sys.argv) == 1:
    sys.stdout.write("Usage: %s <access.log> <accesslog.csv>\n"%sys.argv[0])
    sys.exit(0)

log_file_name = sys.argv[1]
csv_file_name = sys.argv[2]

pattern = re.compile(r'(?P<host>\S+).(?P<rfc1413_ident>\S+).(?P<user>\S+).\[(?P<date_time>\S+ \+[0-9]{4})]."(?P<http_verb>\S+) (?P<url>\S+) (?P<http_ver>\S+)" (?P<status>[0-9]+) (?P<size>\S+) "(?P<referer>.*)" "(?P<user_agent>.*)"\s*\Z')

# Malicious user-agents often send empty/invalid char-sequences in a <request> (consisting of <http_verb> + <url> + <http_ver>) and/or <user_agent>
# e.g. 34.102.69.80 - - [02/May/2024:23:11:59 +0000] "" 400 0 "-" "-"
malicious_pattern = re.compile(r'(?P<host>\S+).(?P<rfc1413_ident>\S+).(?P<user>\S+).\[(?P<date_time>[^\]]+)\] "(?P<request>[^"]*)" (?P<status>\d+) (?P<size>\d+) "(?P<referer>[^"]*)" "(?P<user_agent>[^"]*)"')

file = open(log_file_name)

with open(csv_file_name, 'w') as out:
    csv_out=csv.writer(out)
    csv_out.writerow(['remote_ip', 'rfc1413_ident', 'remote_user', 'date_time', 'http_verb', 'path', 'http_ver', 'status', 'size', 'referer', 'user_agent', 'malicious_request'])
    
    row_count = 0
    malicious_count = 0

    for line in file:
        m = pattern.match(line)
        if m:
            result = m.groups()
            result = list(result) + [''] # non-malicious request
            csv_out.writerow(result)
            row_count += 1

        else:
#             print("Line did not match pattern:", line.strip())

            match = malicious_pattern.match(line)
            if match:
                csv_out.writerow([match.group('host'), match.group('rfc1413_ident'), match.group('user'), match.group('date_time'), '', '', '', match.group('status'), match.group('size'), match.group('referer'), match.group('user_agent'), match.group('request')])
                malicious_count += 1

#                 print("   Host:", match.group('host'))
#                 print("   Ident:", match.group('rfc1413_ident'))
#                 print("   User:", match.group('user'))
#                 print("   Datetime:", match.group('date_time'))
#                 print("   Status:", match.group('status'))
#                 print("   Size:", match.group('size'))
#                 print("   Referer:", match.group('referer'))
#                 print("   User Agent:", match.group('user_agent')) 
#                 print("   Request:", match.group('request'))                

            else:
                raise Exception("[Python] Should not happen. This line slipped through the malicious requests regex: {}".format(line))

    print("[Python] Converting [%s] to [%s]: (valid => %d, malicious => %d). Total rows processed: %d." % (log_file_name.split('/')[-1], csv_file_name.split('/')[-1], row_count, malicious_count, row_count + malicious_count))

