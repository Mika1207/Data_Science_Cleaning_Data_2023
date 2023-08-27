from flask import Flask, render_template, redirect, url_for
import pandas as pd
import os 
from Modules.Module_1 import *

#Folder name
folder = "car_cleaned"
#File name
file = "Car.csv"

#Complet acces to file
file_path = os.path.join(folder, file)


app = Flask(__name__)

#Load of the data
df = pd.read_csv('file_path')


@app.route("/")
def welcome():
    return render_template("index.html")

@app.route("/data")
def data():
    return render_template("data.html", car_data=car_data.to_html(classes="table table-bordered"))

@app.route("/dashboard")
def dashboard():
    return render_template("dashboard.html")

if __name__ == '__main__':
  app.run(debug=True, port=1480)