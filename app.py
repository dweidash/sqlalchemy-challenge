import numpy as np
import pandas as pd
import datetime as dt
from datetime import timedelta
from collections import defaultdict

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

# Database setup
engine = create_engine("sqlite:///Resources/hawaii.sqlite", pool_pre_ping=True)
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

Measurement = Base.classes.measurement
Station = Base.classes.station


app = Flask(__name__)

@app.route("/")
def home():
    return  ("<div align='center'><h1>Welcome to my Hawaii Climate App</h1><br/></div>"
        "<hr><br/>"
        "<h2>Available routes:</h2><br/>"
        "<ul>"
        "<li><h4>A dictionary of the last 12 months of precipitation data:</h4></li>"
        "/api/v1.0/precipitation<br/>"
        "<li><h4>A list of all available stations:</h4></li>"
        "/api/v1.0/stations<br/>"
        "<li><h4>A list of  Temperature Observations (tobs) for the previous year:</h4></li>"
        "/api/v1.0/tobs<br/>"
        "<li><h4>A list of the minimum temperature, the average temperature, and the max temperature for all dates greater or equal to YYYY-mm-dd (example year: 2015-03-01):</h4></li>"
        "/api/v1.0/2015-03-01<br/>"
        "<li><h4>A list of the minimum temperature, the average temperature, and the max temperature for dates between the start and end date inclusive, formatted YYYY-mm-dd (example range: 2015-03-01 to 2016-09-06):</h4></li>"
        "/api/v1.0/2015-03-01/2016-09-06"
        "</ul>")


@app.route("/api/v1.0/precipitation")
def precipitation():

    session = Session(engine)

    measure_t = session.query(Measurement).\
            order_by(Measurement.date.desc())\
            .first()

    last_t = pd.to_datetime(measure_t.date)
    first_t = last_t - timedelta(days=365)
    first_date = dt.date(first_t.year, first_t.month, first_t.day)
    last_date = dt.date(last_t.year, last_t.month, last_t.day)

    measure_year = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= first_date).\
        order_by(Measurement.date.asc()).\
        all()
        
    session.close()


    prcp_data = []
    for date, prcp in measure_year:
        prcp_dict = {date:prcp}
        prcp_data.append(prcp_dict)
    d = defaultdict(list)
    for date,prcp in measure_year:
        d[date].append(prcp)
    prcp_dict_defaultdict = dict(d)

    return jsonify(prcp_dict_defaultdict)


@app.route("/api/v1.0/stations")
def stations():

    # Create session
    session = Session(engine)
    # Return a JSON list of stations from the dataset
    station_count = session.query(Measurement.station).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
    station_list = list(np.ravel(station_count))

    session.close()

    return (jsonify(station_list))


@app.route("/api/v1.0/tobs")
def tobs():

    session = Session(engine)

    top_station = session.query(Measurement.station, func.count(Measurement.station)).\
        group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).\
                first().station

    measure_t = session.query(Measurement).\
            order_by(Measurement.date.desc())\
            .first()

    last_t = pd.to_datetime(measure_t.date)
    first_t = last_t - timedelta(days=365)
    first_date = dt.date(first_t.year, first_t.month, first_t.day)
    last_date = dt.date(last_t.year, last_t.month, last_t.day)

    tobs_resp = session.query(Measurement.date,Measurement.tobs).\
        filter(Measurement.date >= first_date).\
        filter(Measurement.station == top_station).\
        order_by(Measurement.date.asc()).\
        all()

    tobs_li = list(np.ravel(tobs_resp))

    return jsonify(tobs_li)


@app.route("/api/v1.0/<start>")
def start_tobs(start):

    session = Session(engine)

    measure_t = session.query(Measurement).\
            order_by(Measurement.date.desc())\
            .first()
    last_t = pd.to_datetime(measure_t.date)
    time_change = last_t - pd.to_datetime(start)

    range_t = pd.Series(pd.date_range(start,periods=time_change.days+1,freq='D'))
    date_li = []
    for trip in range_t:
        date_li.append(trip.strftime('%Y-%m-%d'))
    def daily_normals(start_date):

        '''Daily Normals. 
        Args:
            date (str): A date string in the format '%Y-%m-%d'
            
        Returns:
            A list of tuples containing the daily normals, tmin, tavg, and tmax
        
        '''

        sel = [Measurement.date, func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
        norms = session.query(*sel).filter(func.strftime("%Y-%m-%d", Measurement.date) == start_date).all()
        return(norms)

    normals = []
    for date in date_li:
        normals.append(daily_normals(date))

    session.close()

    return jsonify(normals)


@app.route("/api/v1.0/<start>/<end>")
def range(start, end):

    session = Session(engine)

    time_change = pd.to_datetime(end) - pd.to_datetime(start)

    range_t = pd.Series(pd.date_range(start,periods=time_change.days+1,freq='D'))
    date_li = []

    for trip in range_t:
        date_li.append(trip.strftime('%Y-%m-%d'))
    def daily_normals(start_date):
        '''Daily Normals. 
        Args:
            date (str): A date string in the format '%Y-%m-%d'
            
        Returns:
            A list of tuples containing the daily normals, tmin, tavg, and tmax
        
        '''
    
        sel = [Measurement.date, func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
        norms = session.query(*sel).filter(func.strftime("%Y-%m-%d", Measurement.date) == start_date).all()
        return(norms)

    normals= []
    for date in date_li:
        normals.append(daily_normals(date))
    # Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
    session.close()
    return jsonify(normals)

if __name__ == "__main__":
    app.run(debug=True)

