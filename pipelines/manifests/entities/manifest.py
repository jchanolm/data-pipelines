from datetime import datetime, timezone
import logging
import os
import time
from tqdm import tqdm
from .cyphers import EntityCyphers
from ..helpers.manifest import Manifest 
import pandas as pd

class EntityManifest(Manifest):
    def __init__(self):
        self.cyphers = EntityCyphers()
        super().__init__("manifest-ents")

    def create_category_array(self,df):
        category_columns = ['pri_cat', 'sec_cat', 'terc_cat', 'quar_cat']
        df['category_array'] = df.apply(
            lambda row: [row[col] for col in category_columns if pd.notna(row[col]) and row[col] != ''], axis=1
        )
        return df

    def create_ents_and_cats(self):
        cats_df = pd.read_csv('pipelines/manifests/entities/data/ent_cats.csv')
        cats_df_w_array = self.create_category_array(cats_df) 
        cats_urls = self.save_df_as_csv(cats_df_w_array, f"ent_cats_.csv")
        self.cyphers.create_ents_and_cats(cats_urls)

    def create_ents_and_ids(self):
        ids_df = pd.read_csv('pipelines/manifests/entities/data/ent_cats.csv')
        ids_urls = self.save_df_as_csv(ids_df, f"ent_cats_.csv")
        self.cyphers.create_ents_and_ids(ids_urls)

    def run(self):
        self.create_ents_and_cats()
        self.create_ents_and_ids()

if __name__ == "__main__":
    M = EntityManifest()
    M.run()

