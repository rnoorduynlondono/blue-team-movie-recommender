import os
import json
import pandas as pd
import numpy as np


class RecommenderMarco:
    def __init__(self, data_path):
        self.matrices = {}
        self.index_mappings = {}

        corr_matrices_path = os.path.join(data_path, 'corr_matrices')
        index_mappings_path = os.path.join(data_path, 'index_mappings')

        for f in os.listdir(corr_matrices_path):
            name, _ = f.split('.')
            self.matrices[name] = np.load(os.path.join(corr_matrices_path, f))

        for f in os.listdir(index_mappings_path):
            name, _ = f.split('.')

            with open(os.path.join(index_mappings_path, f)) as fin:
                self.index_mappings[name] = json.load(fin)

    def get_movie_corr_idx(self, age_group, idx):
        """Given a db index, return a corr matrix idx"""
        return self.index_mappings[age_group]['movie_to_idx'][str(idx)]

    def get_movie_db_idx(self, age_group, idx):
        """Given a corr matrix idx, return a db inx"""
        return self.index_mappings[age_group]['idx_to_movie'][str(idx)]

    def predict(self, user_id, age_group, list_of_movies, n_predictions=10):
        assert age_group in self.matrices, f'Do not understand {age_group}!'

        k = n_predictions + len(list_of_movies)

        predictions = []

        for movie_id in list_of_movies:
            movie_id_corr = self.get_movie_corr_idx(age_group, movie_id)

            correlation_matrix = self.matrices[age_group]
            correlations = correlation_matrix[movie_id_corr, :]  # shape of (n_movies,)

            # index = movie ids (DB), values = correlation scores

            df = pd.Series(
                index=[self.get_movie_db_idx(age_group, i) for i, _ in enumerate(correlations)],
                data=correlations
            ).sort_values(ascending=False).iloc[:k]

            predictions.append(df)

        predictions = (
            pd.concat(predictions).drop(index=list_of_movies)
                .sort_values(ascending=False).iloc[:n_predictions]
        )

        recommendations = []

        for idx, score in predictions.iteritems():
            recommendations.append({'id': idx, 'score': score})

        return {
            "recommendation": recommendations
        }