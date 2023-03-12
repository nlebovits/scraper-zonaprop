import datetime
import os
import re
import time
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
columns = {
    'postingId': 'posting_id',
    'priceOperationTypes[0].operationType.name': 'status',
    'priceOperationTypes[0].prices[0].formattedAmount': 'price',
    'priceOperationTypes[0].prices[0].currency': 'currency_price',
    'expenses.formattedAmount': 'expenses',
    'expenses.currency': 'currency_expenses',
    'mainFeatures.CFT100.value': 'm2_total',
    'mainFeatures.CFT101.value': 'm2_covered',
    'mainFeatures.CFT1.value': 'room',
    'mainFeatures.CFT2.value': 'bedroom',
    'mainFeatures.CFT3.value': 'bathroom',
    'mainFeatures.CFT5.value': 'antiquity',
    'mainFeatures.CFT7.value': 'garage',
    'publisher.publisherId': 'publisher_id',
    'publisher.name': 'publisher_name',
    'realEstateType.name': 'type',
    'postingLocation.postingGeolocation.geolocation.latitude': 'geo_latitude',
    'postingLocation.postingGeolocation.geolocation.longitude': 'geo_longitude',
}

def remove_host_from_url(url):
    host_regex = r'(^https?://)(.*/)'
    return re.sub(host_regex, '', url)

def get_filename_from_datetime(base_url, extension):
    base_url_without_host = remove_host_from_url(base_url)
    return f'data/{base_url_without_host}-{datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.{extension}'


def save_df_to_csv(df, base_url):
    def save_file(suffix, selected_columns, rename_columns):
        filename = get_filename_from_datetime(base_url + suffix, 'csv')
        create_root_directory(filename)
        if selected_columns:
            df_selected = df.loc[:, selected_columns].rename(columns=rename_columns)
        else:
            df_selected = df.copy()
        df_selected.to_csv(filename, index=False)
        logging.info(f'Data saved to {filename}')

    save_file("_COMPLETE", None, None)
    save_file("_PARTIAL", list(columns.keys()), columns)


def parse_zonaprop_url(url):
    return url.replace('.html', '')

def create_root_directory(filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)


def flatten_json(data, prefix=''):
    result = {}

    for key, value in data.items():
        new_key = prefix + key

        if isinstance(value, dict):
            nested = flatten_json(value, new_key + '.')
            result.update(nested)
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    nested = flatten_json(item, new_key + f'[{i}].')
                    result.update(nested)
                else:
                    result[new_key + f'[{i}]'] = item
        else:
            result[new_key] = value

    return result

def monitoring(df, start_time):
    num_rows = df.shape[0]
    end_time = time.time()
    elapsed_time = end_time - start_time
    elapsed_time_formatted = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
    row_rate = elapsed_time / num_rows * 100
    row_rate_formatted = time.strftime("%H:%M:%S", time.gmtime(row_rate))

    logging.info(f'Se procesaron {num_rows} establecimientos en {elapsed_time_formatted}.'
                 f'A razón de 100 establecimientos cada {row_rate_formatted}')

