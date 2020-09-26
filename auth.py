import os
import sys
import time
import json
import requests
import datetime
from db_utils import create_connection, insert_item_timestamp_to_table, insert_item_to_table, create_connection_pg, \
    handle_logs
import sqlite3
import psycopg2
from psycopg2.extras import execute_batch


SUPPORTED_REGIONS = ['eu', 'ru']


# reference: https://github.com/seanwlk/warface-crate-manager/blob/424c9ff8ed11ba4ff64931ae5ba428792339f093/gui/crate-manager.py#L540
def login(session, region='eu'):
    if region == 'eu':
        dir_path = os.path.dirname(os.path.realpath(__file__))
        # TODO: convert credentials as args
        with open('{}/creds.json'.format(dir_path), 'r') as json_file:
            credentials_string = json_file.read()
        credentials = json.loads(credentials_string)

        payload = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9,it-IT;q=0.8,it;q=0.7',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Cookie': 'amc_lang=en_US; ',
            'DNT': '1',
            'Host': 'auth-ac.my.games',
            'Origin': 'https://account.my.games',
            'Referer': 'https://account.my.games/oauth2/login/?continue=https%3A%2F%2Faccount.my.games%2Foauth2%2F%3Fredirect_uri%3Dhttps%253A%252F%252Fpc.warface.com%252Fdynamic%252Fauth%252F%253Fo2%253D1%26client_id%3Dwf.my.com%26response_type%3Dcode%26signup_method%3Demail%2Cphone%26signup_social%3Dmailru%252Cfb%252Cvk%252Cg%252Cok%252Ctwitch%252Ctw%252Cps%252Cxbox%252Csteam%26lang%3Den_US&client_id=wf.my.com&lang=en_US&signup_method=email%2Cphone&signup_social=mailru%2Cfb%2Cvk%2Cg%2Cok%2Ctwitch%2Ctw%2Cps%2Cxbox%2Csteam',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'
        }
        login_data = {
            'email': credentials["email"],
            'password': credentials["password"],
            'continue': 'https://account.my.games/oauth2/?redirect_uri=https%3A%2F%2Fpc.warface.com%2Fdynamic%2Fauth%2F%3Fo2%3D1&client_id=wf.my.com&response_type=code&signup_method=email,phone&signup_social=mailru%2Cfb%2Cvk%2Cg%2Cok%2Ctwitch%2Ctw%2Cps%2Cxbox%2Csteam&lang=en_US',
            'failure': 'https://account.my.games/oauth2/login/?continue=https%3A%2F%2Faccount.my.games%2Foauth2%2Flogin%2F%3Fcontinue%3Dhttps%253A%252F%252Faccount.my.games%252Foauth2%252F%253Fredirect_uri%253Dhttps%25253A%25252F%25252Fpc.warface.com%25252Fdynamic%25252Fauth%25252F%25253Fo2%25253D1%2526client_id%253Dwf.my.com%2526response_type%253Dcode%2526signup_method%253Demail%252Cphone%2526signup_social%253Dmailru%25252Cfb%25252Cvk%25252Cg%25252Cok%25252Ctwitch%25252Ctw%25252Cps%25252Cxbox%25252Csteam%2526lang%253Den_US%26client_id%3Dwf.my.com%26lang%3Den_US%26signup_method%3Demail%252Cphone%26signup_social%3Dmailru%252Cfb%252Cvk%252Cg%252Cok%252Ctwitch%252Ctw%252Cps%252Cxbox%252Csteam&amp;client_id=wf.my.com&amp;lang=en_US&amp;signup_method=email%2Cphone&amp;signup_social=mailru%2Cfb%2Cvk%2Cg%2Cok%2Ctwitch%2Ctw%2Cps%2Cxbox%2Csteam',
            'nosavelogin': '0'
        }
        while True:
            try:
                r = session.post('https://auth-ac.my.games/auth',
                                 headers=payload,
                                 data=login_data,
                                 allow_redirects=False
                                 )
                for i in range(0, 5):
                    """
                    1- Auth redirect to oauth2
                    2- Oauth2 redirect to sdc
                    3- Generates link to get to sdc token
                    4- SDC token redirects to oauth2
                    5- Auth link for pc.warface.com is generated
                    6- GET auth link for session
                    """
                    r = session.get(r.headers['location'], allow_redirects=False)
                get_token = session.get('https://pc.warface.com/minigames/user/info').json()
                handle_logs(0, str(get_token))
                session.cookies['mg_token'] = get_token['data']['token']
                session.cookies['cur_language'] = "en"
            except Exception as eu_login_exception:
                handle_logs(2, str(eu_login_exception))
                continue
            break
        handle_logs(0, 'eu login successful')

    elif region == 'ru':
        dir_path = os.path.dirname(os.path.realpath(__file__))
        # TODO: convert credentials as args
        with open('{}/creds.json'.format(dir_path), 'r') as json_file:
            credentials_string = json_file.read()
        credentials = json.loads(credentials_string)

        payload = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9,it-IT;q=0.8,it;q=0.7',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Cookie': 'amc_lang=en_US; ',
            'DNT': '1',
            'Host': 'auth-ac.my.games',
            'Origin': 'https://account.my.games',
            'Sec-Fetch-Dest': 'script',
            'Sec-Fetch-Mode': 'no-cors',
            'Sec-Fetch-Site': 'same-site',
            'Referer': 'https://account.my.games/oauth2/login/?continue=https%3A%2F%2Faccount.my.games%2Foauth2%2F%3Fredirect_uri%3Dhttps%253A%252F%252Fru.warface.com%252Fdynamic%252Fauth%252F%253Fo2%253D1%26client_id%3Dru.warface.com%26response_type%3Dcode%26signup_method%3Demail%252Cphone%26signup_social%3Dmailru%252Cfb%252Cvk%252Cg%252Cok%252Ctwitch%252Ctw%26lang%3Dru_RU%26gc_id%3D0.1177&client_id=ru.warface.com&lang=ru_RU&signup_method=email%2Cphone&signup_social=mailru%2Cfb%2Cvk%2Cg%2Cok%2Ctwitch%2Ctw&gc_id=0.1177',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
        }

        login_data = {
            'email': credentials['ru_email'],
            'password': credentials['ru_password'],
            'continue': 'https://account.my.games/oauth2/?redirect_uri=https%3A%2F%2Fru.warface.com%2Fdynamic%2Fauth%2F%3Fo2%3D1&client_id=ru.warface.com&response_type=code&signup_method=email%2Cphone&signup_social=mailru%2Cfb%2Cvk%2Cg%2Cok%2Ctwitch%2Ctw&lang=ru_RU&gc_id=0.1177',
            'failure': 'https://account.my.games/oauth2/login/?continue=https%3A%2F%2Faccount.my.games%2Foauth2%2Flogin%2F%3Fcontinue%3Dhttps%253A%252F%252Faccount.my.games%252Foauth2%252F%253Fredirect_uri%253Dhttps%25253A%25252F%25252Fru.warface.com%25252Fdynamic%25252Fauth%25252F%25253Fo2%25253D1%2526client_id%253Dru.warface.com%2526response_type%253Dcode%2526signup_method%253Demail%25252Cphone%2526signup_social%253Dmailru%25252Cfb%25252Cvk%25252Cg%25252Cok%25252Ctwitch%25252Ctw%2526lang%253Dru_RU%2526gc_id%253D0.1177%26client_id%3Dru.warface.com%26lang%3Dru_RU%26signup_method%3Demail%252Cphone%26signup_social%3Dmailru%252Cfb%252Cvk%252Cg%252Cok%252Ctwitch%252Ctw%26gc_id%3D0.1177&client_id=ru.warface.com&lang=ru_RU&signup_method=email%2Cphone&signup_social=mailru%2Cfb%2Cvk%2Cg%2Cok%2Ctwitch%2Ctw&gc_id=0.1177',
            'nosavelogin': '0',
            'g-recaptcha-response': None
        }
        try:
            r = session.post('https://auth-ac.my.games/auth', headers=payload, data=login_data, allow_redirects=False)
            while "location" in r.headers:
                r = session.get(r.headers['location'], allow_redirects=False)
            g = session.get("https://account.my.games/profile/userinfo/", allow_redirects=False).text
            _csrfMiddlewareToken = g.split("name=\"csrfmiddlewaretoken\" value=\"")[1].split('"')[0]
            data = {
                'csrfmiddlewaretoken': _csrfMiddlewareToken,
                'response_type': 'code',
                'client_id': 'ru.warface.com',
                'redirect_uri': 'https://ru.warface.com/dynamic/auth/?o2=1',
                'scope': '',
                'state': '',
                'hash': 'be7ced8c2ae834813f503822e744fade',
                'gc_id': '0.1177',
                'force': '1'
            }
            payload = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-User': '?1',
                'Sec-Fetch-Dest': 'document',
                'Origin': 'https://account.my.games',
                'Referer': 'https://account.my.games/oauth2/?redirect_uri=https%3A%2F%2Fru.warface.com%2Fdynamic%2Fauth%2F%3Fo2%3D1&client_id=ru.warface.com&response_type=code&signup_method=email%2Cphone&signup_social=mailru%2Cfb%2Cvk%2Cg%2Cok%2Ctwitch%2Ctw&lang=ru_RU&gc_id=0.1177',
                'Accept-Language': 'en-US,en;q=0.9,it-IT;q=0.8,it;q=0.7',
                'Upgrade-Insecure-Requests': '1',
                'Content-Type': 'application/x-www-form-urlencoded',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
            }
            r = session.post("https://account.my.games/oauth2/", headers=payload, data=data)
            user_info = session.get(
                'https://ru.warface.com/minigames/user/info').json()  # has to be json implicitly. IT will loop if site is down
            session.cookies['mg_token'] = user_info['data']['token']
            session.cookies['cur_language'] = 'ru'
            handle_logs(0, str(user_info))
        except ValueError as ru_login_exception:
            handle_logs(2, str(ru_login_exception))
        handle_logs(0, 'ru login successful')


def get_mg_token(session, region='eu'):
    if region == 'eu':
        get_token = session.get('https://pc.warface.com/minigames/user/info').json()
        session.cookies['mg_token'] = get_token['data']['token']

    elif region == 'ru':
        get_token = session.get('https://ru.warface.com/minigames/user/info').json()
        session.cookies['mg_token'] = get_token['data']['token']


def main(region='eu'):
    if region not in SUPPORTED_REGIONS:
        handle_logs(3, "invalid region {region}".format(region=region))
        exit(1)

    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open('{}/creds.json'.format(dir_path), 'r') as json_file:
        credentials_string = json_file.read()
    credentials = json.loads(credentials_string)

    # conn = create_connection('./marketplace.db')

    conn = create_connection_pg(dbname=credentials['psql_db_name'],
                                user=credentials['psql_user'],
                                password=credentials['psql_password'],
                                host=credentials['psql_host'],
                                port=credentials['psql_port'])

    timeseries_table_name = ""
    market_url = ""
    items_table_name = ""
    if region == 'eu':
        timeseries_table_name = "timeseries"
        market_url = "https://pc.warface.com/minigames/marketplace/api/all"
        items_table_name = 'items'
    elif region == 'ru':
        timeseries_table_name = "timeseries_ru"
        market_url = "https://ru.warface.com/minigames/marketplace/api/all"
        items_table_name = 'items_ru'

    # start login
    sess = requests.Session()
    while True:
        try:
            login(sess, region=region)
        except Exception as e:
            err_str = "Error when log in: \n" + str(e)
            handle_logs(3, err_str)
            continue
        break

    try_count = 0
    total_fail_count = 0
    fail_count = 0
    # TODO: market url as a part of region
    while True:
        try_count += 1
        # refresh mg_token for every 50 requests
        if try_count % 50 == 0:
            refresh_mg_token_msg = "refreshing mg_token... at {try_count} try count/{total_fail_count} fail count".format(
                try_count=try_count,
                total_fail_count=total_fail_count)
            handle_logs(0, refresh_mg_token_msg)
            while True:
                try:
                    get_mg_token(sess, region='eu')
                except Exception as e:
                    mg_token_exception_msg = "unable to get_mg_token: " + str(e)
                    handle_logs(2, mg_token_exception_msg)
                    continue
                break

        if fail_count > 10:
            handle_logs(1, "Need restart")
            fail_count = 0
            login(sess, region=region)

        # fetch marketplace data
        try:
            req = sess.get(market_url)
            # check HTTP code for validation
            if req.status_code != 200:
                # TODO: record marketplace availability here?
                status_code_exception_msg = "unable to access Marketplace;" + " HTTP Code: {}".format(str(req.status_code))
                handle_logs(2, status_code_exception_msg)
                total_fail_count += 1
                fail_count += 1
                time.sleep(5)
                continue
        except ConnectionError:
            total_fail_count += 1
            fail_count += 1
            handle_logs(0, "connection: server just had a hiccup...")
            time.sleep(5)
            handle_logs(0, "resuming task...")
            continue
        except Exception as e:
            total_fail_count += 1
            fail_count += 1
            market_url_exception_msg = "get requests: {}".format(str(e))
            handle_logs(2, market_url_exception_msg)
            iter_msg = "loop counter: {} iteration(s) done".format(str(try_count))
            handle_logs(0, iter_msg)
            time.sleep(5)
            continue

        # interpret marketplace data
        try:
            main_json = json.loads(req.text)
        except ValueError as value_error:
            total_fail_count += 1
            fail_count += 1
            value_err_msg = "unable to parse json: {}".format(str(value_error))
            handle_logs(2, value_err_msg)
            time.sleep(5)
            continue

        # check if marketplace is under maintenance
        if not main_json:
            total_fail_count += 1
            fail_count += 1
            handle_logs(1, "no items in Marketplace; the user possibly has no access to the marketplace; standby...")
            time.sleep(30)
            continue
        if main_json == "error":
            total_fail_count += 1
            fail_count += 1
            handle_logs(2, "marketplace returns an error; standby...")
            time.sleep(30)
            continue
        if 'state' in main_json:
            if main_json['state'] == 'Fail':
                total_fail_count += 1
                fail_count += 1
                fail_state_msg = "fail state detected in main_json: {}".format(str(main_json))
                handle_logs(2, fail_state_msg)
                continue
        sys.stdout.flush()

        current_timestamp = time.time()
        entity_time_stamp = int(current_timestamp)
        cur = conn.cursor()

        start_time = time.time()
        main_db_check_time = 0
        check_and_set_time = 0
        fetch_old_set_time = 0
        new_insert_time = 0
        main_list = []
        for item in main_json['data']:
            if item['item'] is not None:
                main_list.append({'entity_id': item['entity_id'],
                                  'min_price': item['min_cost'],
                                  'entity_count': item['count'],
                                  'kind': item['kind'],
                                  'entity_type': item['type'],
                                  'item_id': item['item']['id']})

        # find what entity_ids are already in the database
        cur.execute("select entity_id from {items_table_name}".format(items_table_name=items_table_name))
        result = cur.fetchall()

        # find if there are any new entity_id in newly fetched entity_id
        items_diff = set([i['entity_id'] for i in main_list]) - set([i[0] for i in result])
        # formulate a list of entity_ids and their properties that are new
        items_waitlist = list(filter(lambda x: x['entity_id'] in items_diff, main_list))

        # insert newly found entities into items and timeseries table
        if items_waitlist:
            s = time.time()
            cur.execute("BEGIN TRANSACTION")
            for entity in items_waitlist:
                miss_item_msg = "Found missing item: {item_id}".format(item_id=entity['item_id'])
                handle_logs(0, miss_item_msg)

                item_tuple = (entity["entity_id"], entity['item_id'], entity["kind"], entity["entity_type"])
                item_time_tuple = (entity['entity_id'], entity_time_stamp, entity['min_price'], entity['entity_count'])

                handle_logs(0, "insert: {item_tuple}...".format(item_tuple=item_tuple))
                insert_item_to_table(cur, item_tuple, items_table_name)

                handle_logs(0, "insert: {item_time_tuple}...".format(item_time_tuple=item_time_tuple))
                insert_item_timestamp_to_table(cur, item_time_tuple, timeseries_table_name)

            cur.execute("COMMIT")

            e = time.time()
            elapsed = e - s
            handle_logs(0, "Entity insertion {n_items} item(s) took {t}s".format(n_items=str(len(items_diff)), t=elapsed))
        conn.commit()

        # check if there's a change in count or/and price for all entities
        # DBG: timing the fetch speed
        s = time.time()

        # DBG: fetch old set from DB AFTER missing values are inserted
        # PostgreSQL method to fetch all entities with newest timestamp; SQLite can be much simplier
        sql = """SELECT {timeseries_table_name}.entity_id,
                        {timeseries_table_name}.min_price,
                        {timeseries_table_name}.entity_count,
                        {timeseries_table_name}.entity_timestamp
                 FROM {timeseries_table_name}
                     JOIN
                (SELECT {timeseries_table_name}.entity_id,
                        MAX({timeseries_table_name}.entity_timestamp) AS newest_timestamp
                 FROM {timeseries_table_name}
                GROUP BY entity_id) t2
                    ON
                {timeseries_table_name}.entity_id = t2.entity_id 
                    AND 
                {timeseries_table_name}.entity_timestamp=t2.newest_timestamp;
                """.format(timeseries_table_name=timeseries_table_name)
        cur.execute(sql)
        fetch = cur.fetchall()
        old = [(i[0], i[1], i[2]) for i in fetch]
        # DBG: timing the fetch speed
        e = time.time()
        elapsed = e - s
        fetch_old_set_time += elapsed

        # DBG: Do diff to find entities with changed count or/and price
        diff = set(
            set([(entity['entity_id'], entity['min_price'], entity['entity_count']) for entity in main_list])) - set(
            old)

        # if no entities with changed count or/and price, then do nothing and go to next cycle
        if diff == set():
            time.sleep(5)
            continue
        """else:
            for i, (new_e_id, new_price, new_count) in enumerate([i for i in diff]):
                for j, (old_e_id, old_price, old_count) in enumerate([j for j in old]):
                    if new_e_id == old_e_id:
                        pass
                        
                        print(
                            "Change(s) for item: {entity_id}, {old_price}K/{new_price}K, {old_count}/{new_count}".format(
                                entity_id=new_e_id,
                                old_price=old_price,
                                new_price=new_price,
                                old_count=old_count,
                                new_count=new_count))"""

        # DBG: Insert diffs into DB
        s = time.time()
        #####
        try:
            cur.execute("BEGIN TRANSACTION")
        except sqlite3.OperationalError as e:
            handle_logs(3, str(e))
            time.sleep(5)
            continue
        except psycopg2.Error as e:
            handle_logs(3, str(e))
            time.sleep(5)
            continue

        if type(cur) == sqlite3.Cursor:
            for t in diff:
                insert_item_timestamp_to_table(cur, (t[0], entity_time_stamp, t[1], t[2]), timeseries_table_name)
            cur.execute("COMMIT")
        # use psycopg2 execute_batch to accelerate multi-inserts
        elif type(cur) == psycopg2.extensions.cursor:
            params_list = [(single_params[0],
                            entity_time_stamp,
                            single_params[1],
                            single_params[2]) for single_params in diff]
            execute_batch(cur,
                          '''INSERT INTO {timeseries_table_name} (entity_id, entity_timestamp, min_price, entity_count)
                             VALUES(%s, %s, %s, %s)
                          '''.format(timeseries_table_name=timeseries_table_name),
                          argslist=params_list)
            cur.execute("COMMIT")

        #####
        e = time.time()
        new_insert_time = e - s
        conn.commit()

        # handle_logs(0, "main db check took {}s".format(str(main_db_check_time)))
        # handle_logs(0, "check and set took {}s".format(str(check_and_set_time)))
        # handle_logs(0, "fetch old set took {}s".format(str(fetch_old_set_time)))
        handle_logs(0, "batch inserted {n} item(s) took {t}s".format(n=len(diff), t=str(new_insert_time)))
        time.sleep(5)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("option: auth.py [eu|ru]")
        exit(1)
    input_region = sys.argv[1]
    if input_region not in SUPPORTED_REGIONS:
        print("option: auth.py [eu|ru]")
        exit(1)
    main(region=input_region)
