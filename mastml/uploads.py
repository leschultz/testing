from foundry import Foundry
from pyngrok import ngrok

import pandas as pd
import requests
import pickle
import json
import os


def units(x):
    '''
    Grab units from data if available.

    inputs:
        x = The complete feature name.
    outputs:
        units = The units for the feature.
    '''

    if ('(' in x) and (')' in x):
        units = x[x.find('(')+1:x.find(')')]
    elif ('[' in x) and (']' in x):
        units = x[x.find('[')+1:x.find(']')]
    else:
        units = '-'

    return units


def meta_super(df, target=None):
    '''
    Create the meatadata for a csv file.

    inputs:
        df = A pandas dataframe with X features and target y.
        target = (optional) The name of the target variable.

    outputs:
        metadata = Metadata for upload.
    '''

    # Features processing
    if target is not None:
        X = df.drop([target], axis=1)
    else:
        X = df

    X_names = list(X.columns)
    X_units = list(map(units, X_names))
    X = X.values

    # Target processing
    if target:
        y = df[target].values
        y_units = units(target)

    # Include dataframe metadata
    metadata = {}
    metadata['inputs'] = X_names
    metadata['input_units'] = X_units

    if target:
        metadata['outputs'] = [target]
        metadata['output_units'] = [y_units]

    return metadata


def mdf_data(
             data,
             target,
             exclude,
             update,
             title,
             authors,
             model_path=None,
             servable_type=None
             ):
    '''
    Upload a dataset to the materials data facility (MDF).

    inputs:
        data = The location of the data file.
        target = The column name for the target variable.
        exclude = A list of columns to exclude from data.
        update = Whether to update or create a new submission.
        title = The title of the dataset.
        authors = A list of authors.
    '''

    # Load data
    df = pd.read_csv(data)

    # Remove unused features
    if exclude is not None:
        df = df.drop(exclude, axis=1)

    metadata = meta_super(df, target)

    # Make json
    parsed = json.dumps(df.to_dict(), indent=2)
    with open('foundry_dataframe.json', 'w') as outfile:
        json.dump(parsed, outfile)

    # Make url host
    tunel = ngrok.connect()
    url = tunel.public_url

    # Push data
    data = open('foundry_dataframe.json', 'rb')
    requests.post(url, files={'data_': data})

    # Upload to foundry
    f = Foundry(no_browser=True, no_local_server=True)
    res = f.publish(
                    metadata,
                    url,
                    title,
                    authors,
                    update=update
                    )

    print('Data submission: {}'.format(res))  # Status

    # Status print
    with open('log.txt', 'w') as outfile:
        for i, j in res.items():
            outfile.write('{}: {}\n'.format(i, j))

        outfile.write('url: {}'.format(url))

    # Load model
    if (model_path is not None) and (servable_type is not None):

        model_info = {}
        model_info['authors'] = authors
        model_info['title'] = title
        model_info['short_name'] = title+'_short'
        model_info['servable'] = {}
        model_info['servable']['type'] = servable_type
        model_info['servable']['filepath'] = model_path
        model_info['servable']['n_input_columns'] = len(metadata['input_units'])
        model_info['servable']['classes'] = df[target].unique().tolist()

        res = f.publish_model(model_info)
        print('Model submission: {}'.format(res))  # Status

    # Disconnect url
    ngrok.disconnect(tunel.public_url)
