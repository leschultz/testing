from foundry import Foundry
from pyngrok import ngrok

import pandas as pd
import requests
import logging
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


def data_meta(df, target=None):
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


class pack(object):
    '''
    A class to handle data and model curation.
    '''

    def __init__(self, target, data_loc, model_loc=None):
        '''
        Start logging and load applicable items:

        inputs:
            target = The target variable.
            data_loc = The location of data or MDF data name.
            model_loc = (optional) The model location.
        '''

        self.foundry = Foundry()
        self.target = target
        self.data_loc = None
        self.model_loc = None

        logging.basicConfig(
                            filename="log.txt",
                            level=logging.INFO,
                            filemode="a+",
                            format="%(asctime)-15s %(message)s"
                            )

        # Load data
        try:

            # New data not in MDF
            df = pd.read_csv(data_loc)
            self.data_path = data_loc

        except Exception:
            try:
                df = self.foundry.load(data_loc, globus=False)
                df = df.load_data()  # Foundry bug
            except Exception:
                raise Exception('No supported data format nor MDF ID.')

        self.df = df
        logging.info('Loaded data from {}'.format(data_loc))

        # Load model
        if model_loc:
            try:

                # New model not in DlHub
                with open(model_loc, 'rb') as infile:
                    model = pickle.load(infile)
                self.model_path = model_loc

            except Exception:
                try:
                    model = 'dlhub'
                except Exception:
                    raise Exception('No supported model format nor DlHub ID.')

            logging.info('Loaded model from {}'.format(model_loc))

            self.model = model

        # Both mode and data uploads require data information
        self.data_info = data_meta(df, self.target)

    def get_data(self):
        return self.df

    def get_model(self):
        return self.model

    def publish_data(
                     self,
                     title,
                     authors,
                     ):
        '''
        Upload a dataset to the materials data facility (MDF).

        inputs:
            title = The title of the dataset.
            authors = A list of authors.
        '''

        # Load data
        df = self.df

        # If data taken from MDF or user created.
        if self.data_loc:
            update = True
        else:
            update = False

        # Make json
        parsed = json.dumps(df.to_dict(), indent=2)

        # Make url host
        url = ngrok.connect().public_url

        # Push data
        requests.post(url, files={'data_': parsed})

        # Upload to foundry
        res = self.foundry.publish(
                                   self.data_info,
                                   url,
                                   title,
                                   authors,
                                   update=True
                                   )

        ngrok.disconnect(url)

        logging.info('Data submission: {}'.format(res))  # Status
        logging.info('url: {}'.format(url))

    def publish_model(
                      self,
                      title,
                      authors,
                      ):

        print(self.model._get_tags())
        model_info = {}
        model_info['authors'] = authors
        model_info['title'] = title
        model_info['short_name'] = ''
        model_info['servable'] = {}
        model_info['servable']['type'] = servable_type
        model_info['servable']['filepath'] = model_path
        model_info['servable']['n_input_columns'] = len(metadata['input_units'])
        model_info['servable']['classes'] = df[target].unique().tolist()

        res = self.foundry.publish_model(model_info)
        logging.info('Model submission: {}'.format(res))  # Status
