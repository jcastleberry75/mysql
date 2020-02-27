#!/usr/local/bin/python3.6

import MySQLdb
import calendar
import sys
import csv
from _collections import defaultdict
from datetime import datetime
from custom_logger import cus_logger


today = datetime.today()
logger = cus_logger("parser.log")
# arguments to script must be month (1 or 2 digits, no 0s allowed) and 4 digit year
month_arg = int(sys.argv[1])
year_arg = int(sys.argv[2])

def error_handler(error):
    error_msg = str(error)
    logger.critical("The SQL Client has generated the following error")
    logger.debug(error_msg)
    support_contact = ("""FOO""")
    logger.info(support_contact)


def cursor():
    db = MySQLdb.connect(host="localhost",
                         user="FOO",
                         passwd="FOO",
                         db="FOO")
    c = db.cursor()
    return c


def sql_execute_fetchone(query):
    cursor.execute(query)
    rsp = cursor.fetchone()
    return rsp[0]


def sql_execute_fetchall(query):
    cursor.execute(query)
    rsp = cursor.fetchall()
    return rsp


def get_months_server_orders(m, y):
    order_success = []
    order_fail = []
    date_gen = cal.itermonthdays(year=y, month=m)
    dates = []
    for d in date_gen:
        if d is not 0:
            dates.append(d)
    first_last_days = (dates[0], dates[-1])
    query = f"""select * from orders_order WHERE name = 'Installation of Custom Server'
            and create_date between '{y}-{m}-{first_last_days[0]} 00:00:00'
            and '{y}-{m}-{first_last_days[1]} 23:59:59';"""
    rsp = sql_execute_fetchall(query)
    for i in rsp:
        if i[7] == "FAILURE":
            order_fail.append(i)
        elif i[7] == "SUCCESS":
            order_success.append(i)
    num_orders = len(order_fail + order_success)
    return num_orders, order_fail, order_success


def get_months_order_grp_ids(m, y):
    ids = []
    cal = calendar.Calendar()
    date_gen = cal.itermonthdays(year=y, month=m)
    dates = []
    for d in date_gen:
        if d is not 0:
            dates.append(d)
    first_last_days = (dates[0], dates[-1])
    query = f"""select * from orders_order WHERE name = 'Installation of Custom Server'
            and create_date between '{y}-{m}-{first_last_days[0]} 00:00:00'
            and '{y}-{m}-{first_last_days[1]} 23:59:59';"""
    rsp = sql_execute_fetchall(query)
    for i in rsp:
        ids.append(i[10])
    return ids


def get_months_provision_jobs(m, y):
    jobs = []
    date_gen = cal.itermonthdays(year=y, month=m)
    dates = []
    for d in date_gen:
        if d is not 0:
            dates.append(d)
    first_last_days = (dates[0], dates[-1])
    query = f"""select * from jobs_job where type = "provision" and status = "SUCCESS"
            and start_date between '{y}-{m}-{first_last_days[0]} 00:00:00'
            and '{y}-{m}-{first_last_days[1]} 23:59:59';"""
    rsp = sql_execute_fetchall(query)
    for i in rsp:
        jobs.append(i)
    return jobs


def get_provision_times(monthly_jobs):
    job_times = {}
    hostname = lambda h: h[5].split()[-1][:-1]
    start_date = lambda time1: time1[3]
    end_date = lambda time3: time3[4]
    for i in monthly_jobs:
        try:
            host = hostname(i)
            start = start_date(i)
            end = end_date(i)
            elapsed_time = str(end - start)[:-3]
            job_times[host] = elapsed_time

        except (OSError, SystemError, LookupError, NameError, KeyError) as err:
            error_handler(err)

    if job_times:
        return job_times
    else:
        return None

cursor = cursor()
cal = calendar.Calendar()
csv_name = f"""{sys.argv[1]}_{sys.argv[2]}.csv"""
with open(csv_name, 'w', newline='') as csvfile:
    fieldnames = ["Month", "Number_of_Provisioning_Orders", "Provisioning_Success_Rate", "Provisioning_Fail_Rate"]
    dictwriter = csv.DictWriter(csvfile, fieldnames=fieldnames)
    spamwriter = csv.writer(csvfile, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_MINIMAL)
    dictwriter.writeheader()
    try:
        order_results = get_months_server_orders(month_arg, year_arg)
        num_orders = order_results[0]
        logger.info(f"""Number of Provisioning Orders: {num_orders}""")
        order_fails = order_results[1]
        num_fails = len(order_fails)
        order_successes = order_results[2]
        num_successes = len(order_successes)
        success_percentage = '{0:.2f}'.format((num_successes / num_orders * 100))
        failure_percentage = '{0:.2f}'.format((num_fails / num_orders * 100))
        logger.info(f"""Number of Provisioning Order Successes: {num_successes}, {success_percentage}%""")
        logger.info(f"""Number of Provisioning Order Failures: {num_fails}, {failure_percentage}%""")

    except (OSError, SystemError, LookupError, NameError, KeyError) as err:
        error_handler(err)

    try:
        grp_ids = get_months_order_grp_ids(month_arg, year_arg)
        id_counts = defaultdict(int)

        for id in grp_ids:
            query = f"""select name from accounts_group where id = {id}""";
            rsp = sql_execute_fetchone(query)
            logger.warning(rsp)
            grp_id = rsp
            id_counts[grp_id] += 1

        id_ct_result = dict(id_counts)
        logger.warning(id_ct_result)
        logger.info(f"""Number of Servers Provisioned Per Department: {id_ct_result}""")

    except (OSError, SystemError, LookupError, NameError, KeyError) as err:
        error_handler(err)

    try:
        monthly_provision_jobs = get_months_provision_jobs(month_arg, year_arg)

    except (OSError, SystemError, LookupError, NameError, KeyError) as err:
        error_handler(err)

    try:
        provision_time_data = get_provision_times(monthly_provision_jobs)
        logger.info(f"""Server Provisioning Times {provision_time_data}""")

    except (OSError, SystemError, LookupError, NameError, KeyError) as err:
        error_handler(err)
    month_val = str(sys.argv[1])
    dictwriter.writerow({'Month': month_val,
                    'Number_of_Provisioning_Orders': num_orders,
                    'Provisioning_Success_Rate': success_percentage,
                    'Provisioning_Fail_Rate': failure_percentage})
    spamwriter.writerow('')
    spamwriter.writerow('Servers Per Department')
    for k, v in id_ct_result.items():
        new_row = f"""{k},{v}"""
        spamwriter.writerow(new_row)
    spamwriter.writerow('')
    spamwriter.writerow('Server Provision Times')
    for k, v in provision_time_data.items():
        new_row = f"""{k},{v}"""
        spamwriter.writerow(new_row)
