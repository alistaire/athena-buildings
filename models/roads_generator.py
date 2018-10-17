from io import StringIO

import boto3
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString

from models.utils import AthenaWaiter, download_file_from_s3


class RoadsGenerator(object):

    def __init__(self, min_x, max_x, min_y, max_y, bucket, folder):
        self.min_x, self.max_x = min_x, max_x
        self.min_y, self.max_y = min_y, max_y
        self.bucket = bucket
        self.folder = folder

    def get_query_string(self):
       return "WITH nodes_in_bbox AS ("\
              "SELECT id, lat, lon, type, tags FROM planet"\
              " WHERE "\
              "type = 'node' "\
              "AND lon between {0} AND {1}"\
              "AND lat BETWEEN {2} AND {3}"\
              "), "\
              "ways AS ("\
              "SELECT type, id, tags, nds FROM planet"\
              "WHERE"\
              "type = 'way'"\
              ") "\
              "SELECT"\
              "w.id as way_id"\
              ",n.id as node_id"\
              ",n.lon, n.lat"\
              ",node_position"\
              "FROM ways w "\
              "CROSS JOIN unnest(w.nds)"\
              "WITH ORDINALITY as t(nd, node_position)"\
              "JOIN nodes_in_bbox n ON n.id = nd.ref"\
              "WHERE element_at ( coalesce(w.tags), 'highway') IS NOT NULL"\
              "ORDER BY way_id, node_position"\
              .format(self.min_x, self.max_x, self.min_y, self.max_y)
            

    def get_query_id(self):
        client = boto3.client(
            'athena',
            region_name='us-east-1'
        )
        response = client.start_query_execution(
            QueryString=self.get_query_string(),
            QueryExecutionContext={
                'Database': 'default'
            },
            ResultConfiguration={
                'OutputLocation': 's3://{0}/{1}'.format(
                    self.bucket,
                    self.folder
                )
            }
        )
        return response['QueryExecutionId']

    def get_results_key(self, query_id):
        return '{0}/{1}.csv'.format(self.folder, query_id)

    def get_results_df(self, query_id):
        waiter = AthenaWaiter(max_tries=100)
        waiter.wait(
            bucket=self.bucket,
            key=self.get_results_key(query_id),
            query_id=query_id
        )
        raw_result = StringIO(
            download_file_from_s3(
                self.get_results_key(query_id),
                self.bucket
            )
        )
        return pd.read_csv(raw_result, encoding='utf-8')

    @staticmethod
    def create_lineString(way):
        node_list = list(zip(way.lon, way.lat))
        return LineString(node_list) if len(node_list) >= 3 \
            else None

    def generate(self):
        all_roads = gpd.GeoDataFrame()
        query_id = self.get_query_id()
        results = self.get_results_df(query_id)
        ways = results.groupby(by=['way_id'])
        for _, way in ways:
            lstring = self.create_linestring(way)
            if lstring:
                metadata = way.iloc[0]
                road_gdf = gpd.GeoDataFrame(
                    [[
                        metadata['name'],
                        lstring
                    ]],
                    columns=[
                        'name',
                        'geometry'
                    ]
                )
                all_roads = all_roads.append(road_gdf)
        return all_roads.to_json(ensure_ascii=False)
