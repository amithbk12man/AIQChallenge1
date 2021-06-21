#!/usr/bin/env python3

import pandas as pd
from flask import Flask
from flask import request
from flask import jsonify
from pandasql import sqldf
import argparse


app = Flask(__name__)
file_path = "data/egrid2016_data.xlsx"
print('loding file')
xl1 = pd.read_excel(file_path,sheet_name="PLNT16")
#xl1.rename(columns=lambda x: x.replace(' ','_'), inplace=True)
xl1.rename(columns=lambda x: x.replace(' ','_').replace('(MWh)',''), inplace=True)


@app.route('/topN')
def topNPlants():
    size = request.args.get('size')
    state = request.args.get('state')
    sql = "SELECT t1.plant_state_abbreviation as state, " \
          "t1.plant_name as plant_name," \
          "t1.plant_latitude as plant_lat," \
          "t1.plant_longitude as plant_long, " \
          "t1.Plant_annual_net_generation_ as actual_value , " \
          "t2.Plant_annual_net_generation as aggregate_value , " \
          "(cast(t1.Plant_annual_net_generation_ as real)/t2.Plant_annual_net_generation)*100 as federal_state_percentage " \
          "FROM xl1 as t1 inner join " \
          "(select Plant_state_abbreviation, sum(Plant_annual_net_generation_) as Plant_annual_net_generation " \
          "from xl1 group by Plant_state_abbreviation) t2 " \
          "on t1.Plant_state_abbreviation=t2.Plant_state_abbreviation " \
          "where t1.eGRID2016_Plant_file_sequence_number!='SEQPLT16'" \

    if state:
        sql = sql + " and t1.plant_state_abbreviation='"+state+"' "
    if size:
        sql = sql + "order by t1.Plant_annual_net_generation_ desc limit "+size
    else:
        sql = sql + "order by t1.Plant_annual_net_generation_ desc limit 10"

    df = sqldf(
            sql
        )

    res = []
    for ind in df.index:
        res.append({'state': df['state'][ind],
                    'plant_name': df['plant_name'][ind],
                    'plant_lat': float(df['plant_lat'][ind]),
                    'plant_long': float(df['plant_long'][ind]),
                    'aggregate_value_per_state': float(df['aggregate_value'][ind]),
                    'actual_value': df['actual_value'][ind],
                    'percentage': float(df['federal_state_percentage'][ind])})
    return jsonify(res)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=9080, help='The port to run the server')
    args = parser.parse_args()
    app.run(host='0.0.0.0', port=args.port, debug=True)
