import os
import gzip
import json
import random
from datetime import datetime, timedelta

def create_random_spur_data():
    return {
        "ip": f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}",
        "organization": f"Org-{random.randint(1,1000)}",
        "as": {"number": random.randint(1000,99999), "organization": f"AS-Org-{random.randint(1,1000)}"},
        "client": {"proxies": [random.choice(["LUMINATI_PROXY", "TOR", "VPN", "OTHER"])]},
        "location": {
            "city": f"City-{random.randint(1,100)}",
            "state": f"State-{random.randint(1,50)}",
            "country": random.choice(["US", "JP", "UK", "DE", "FR", "CA"])
        },
        "risks": [random.choice(["CALLBACK_PROXY", "TOR", "VPN", "SPAM", "ATTACK"])]
    }

def create_random_rrp_data():
    return f"{random.randint(100000000,999999999)}-{random.choice(['M', 'F'])}-{random.randint(10000000,99999999)}-" \
           f"{random.randint(1000000,9999999)}-MeF-{random.randint(2020,2024)}-{random.randint(1,5)}-" \
           f"{random.randint(100,9999)}.{random.randint(0,99):02d}--{random.randint(1000000000,9999999999)}--" \
           f"{random.randint(10000000,99999999)}-{random.randint(1000000000,9999999999)}---" \
           f"{random.randint(100000000,999999999)}---{random.randint(100000000,999999999)}-" \
           f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}-" \
           f"{random.choice(['U', 'R'])}-{random.choice(['U', 'R'])}-{random.choice(['U', 'R'])}-" \
           f"{random.random():.3f}\n"

def create_random_tds_data():
    return f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} " \
           f"{random.randint(1000000000,9999999999)} ESRV_OLG_TINMATCH_BUS in: <AmzasbdozESRV_OLG_TINMATCH_BUS " \
           f"pnd <AmzasSec>{random.randint(1000000000,9999999999)} USERNAMEHERE " \
           f"{random.randint(100000000000000,999999999999999)} A v1.3 00000000 " \
           f"{datetime.now().strftime('%Y%m%d%H%M%S')}z.{random.randint(1,255)}.{random.randint(0,255)}." \
           f"{random.randint(0,255)}.{random.randint(0,255)} </AmzasSec> <soapenv:Envelope ...>\n"

def create_random_esam_data():
    return f"{random.randint(1,1000)} {datetime.now().strftime('%Y-%m-%d %H:%M:%S')},{random.randint(0,999):03d} " \
           f"[{random.randint(1000000000,9999999999)}] - /escr/esam.bss/api/esrv-account/" \
           f"W{random.randint(100000000,999999999)}-{random.randint(10000000,99999999)}/roles " \
           f"{'in' if random.choice([True, False]) else 'out'}: GET " \
           f"{random.choice([200, 201, 204, 400, 401, 403, 404, 500])} " \
           f"{','.join(random.sample(['AUTH', 'AMEF', 'IDTP', 'ROLE1', 'ROLE2'], random.randint(1,5)))}\n"

def create_random_ice_data():
    return f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')},{random.randint(0,999):03d} " \
           f"[{random.randint(1000000000,9999999999)}] {random.randint(1000000000000000000000,9999999999999999999999)} " \
           f"ICCE_BUSINESS_TRANSACTION in: <soapenv:Envelope ...>\n"

def create_sample_structure(base_dir):
    feeds = {
        'spur': (create_random_spur_data, "%Y%m%d_spur_latest.json.gz", 10),
        'rrp': (create_random_rrp_data, "RRPEntitydatazeAuth%Y%m%d.data.gz", 10),
        'tds': (create_random_tds_data, "tim-messages.log.%Y%m%d%H.gz", 5),
        'esam': (create_random_esam_data, "esam-messages.log.%Y%m%d-%H.gz", 5),
        'ice': (create_random_ice_data, "vrsxf.log.%Y%m%d-%H.gz", 5)
    }

    servers = ['vp2txapesvprd07', 'vp2xmtbappice12']

    for feed, (data_func, file_pattern, days) in feeds.items():
        feed_dir = os.path.join(base_dir, feed)
        os.makedirs(feed_dir, exist_ok=True)

        if feed in ['tds', 'esam', 'ice']:
            for server in servers:
                server_dir = os.path.join(feed_dir, server)
                os.makedirs(server_dir, exist_ok=True)
                create_files(server_dir, data_func, file_pattern, days, hourly=True)
        else:
            create_files(feed_dir, data_func, file_pattern, days, hourly=False)

def create_files(directory, data_func, file_pattern, days, hourly):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days-1)
    current_date = start_date

    while current_date <= end_date:
        if hourly:
            for hour in range(24):
                file_datetime = current_date.replace(hour=hour)
                file_name = os.path.join(directory, file_datetime.strftime(file_pattern))
                create_file(file_name, data_func)
        else:
            file_name = os.path.join(directory, current_date.strftime(file_pattern))
            create_file(file_name, data_func)
        
        current_date += timedelta(days=1)

def create_file(file_name, data_func):
    with gzip.open(file_name, 'wt') as f:
        for _ in range(1000):
            f.write(json.dumps(data_func()) if isinstance(data_func(), dict) else data_func())
    print(f"Created file: {file_name}")

if __name__ == "__main__":
    base_directory = "/tmp/kafka/cui_cfam/cui_cfam_prod_collection_droppoint"
    create_sample_structure(base_directory)
