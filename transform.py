"""Script to transform the data for the markov chain model"""
import json
import re
import pandas as pd

file = 'data.json'

def get_products(file):
    with open(file) as f:
        data = json.load(f)
    product_list = []
    for session in data:
        tmp = list({
            x['flow_name']
            for x in session['path'] if 'flow_name' in x.keys() and
            x['flow_name'] not in ['', 'index.html']
        })
        product_list.extend(tmp)
    return set(product_list)

def transform_data(file, product="all", prod_nodes=True):
    with open(file) as f:
        data = json.load(f)
    source_list = []
    for session in data:
        tmp = list({
            x['session']['source'] if 'session' in x.keys() else x['flow_name']
            for x in session['path']
        })
        source_list.extend(tmp)
    source_set = set(source_list)
    sources = ([
        "google seo", "directo", "google adwords", "facebook", "referrers",
        "email", "seo otros buscadores", "redes sociales orgánico"
        ] + [x for x in source_set if 'referido' in x])
    sources_dict = {x: str(i + 1) for i, x in enumerate(sources)}
    path_list = []
    profit_list = []
    for session in data:
        path = ''
        path_len = len(session['path'])
        for index, elem in enumerate(session['path']):
            if index < (path_len - 1):
                if 'session' in elem.keys():
                    if 'source' in elem['session'].keys():
                        if elem['session']['source'] in sources_dict:
                            path += (sources_dict[elem['session']['source']]
                                     + ' > ')
                        else:
                            path += '99' + ' > '
                    else:
                        print("ERROR IN EVENT (SOURCE NOT FOUND) " +
                              session['event_id'])
                elif 'flow_name' in elem.keys() and prod_nodes:
                    if 'profit' in elem.keys():
                        if elem['flow_name'] in sources_dict:
                            path += sources_dict[elem['flow_name']] + ' > '
                        else:
                            path += '999' + ' > '
                    else:
                        print("ERROR IN EVENT (PROFIT NOT FOUND)" +
                              session['event_id'])
                elif not prod_nodes:
                    pass
                else:
                    print("ERROR IN EVENT (NEITHER SOURCE OR FLOW NAME)" +
                          session['event_id'])
            else:
                if 'flow_name' in elem.keys() and 'profit' in elem.keys():
                    product_name = elem['flow_name']
                    if not re.match('^-', elem['profit']):
                        profit = elem['profit'].replace(',', '')
                    else:
                        profit = elem['profit'].replace(',', '')
                        #profit = 0
                    if path == '':
                        profit = 0
                        break
                    path += '*'
                    path = path.replace(' > *', '')
                    if profit:
                        try:
                            profit = float(profit.replace(',', ''))
                        except:
                            print(profit)
                            profit = 0
        if path != '' and (product_name == product or product == "all"):
            path_list.append(path)
            profit_list.append(profit)
    df = pd.DataFrame.from_dict({'path': path_list,
                                 'conversion': [1] * len(path_list),
                                 'value': profit_list})
    return df
# Total products
# With products as nodes
df = transform_data('data.json')
df.to_csv('data.csv', index=False)
# Only sources as nodes
df_only_sources = transform_data('data.json', prod_nodes=False)
df_only_sources.to_csv('data_sources.csv', index=False)

products_dict = get_products(file)
for product in products_dict:
    df_product = transform_data(file, product)
    if df_product.shape[0] > 45:
        df_product.to_csv('data-' + product + '.csv', index=False)

for product in products_dict:
    df_product = transform_data(file, product, False)
    if df_product.shape[0] > 45:
        df_product.to_csv('data-' + product + '-sources.csv', index=False)



sources_df = pd.DataFrame.from_dict({
    'nombre_canal': list(sources_dict.keys()),
    'canal': list(sources_dict.values())})
sources_df.to_csv('dict.csv', index=False)
