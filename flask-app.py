from flask import Flask, render_template, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

app = Flask(__name__)
app.secret_key = "your_secret_key"

# Database setup
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///reservations.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

# Reservation model
class Reservation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    account_name = db.Column(db.String(80), nullable=False)
    datetime_slot = db.Column(db.DateTime, unique=True, nullable=False)

with app.app_context():
    db.create_all()

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        account_name = request.form.get("account_name")
        datetime_str = request.form.get("datetime_slot")

        if not account_name or not datetime_str:
            flash("Please enter your name and select a date and time.", "error")
        else:
            try:
                datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M")
            except ValueError:
                flash("Invalid date and time format.", "error")
                return redirect(url_for("index"))

            if Reservation.query.filter_by(datetime_slot=datetime_obj).first():
                flash("That time slot is already reserved.", "error")
            else:
                new_res = Reservation(account_name=account_name, datetime_slot=datetime_obj)
                db.session.add(new_res)
                db.session.commit()
                flash(f"Reserved {datetime_obj.strftime('%b %d, %Y %I:%M %p')} for {account_name}.", "success")

        return redirect(url_for("index"))

    reservations = Reservation.query.order_by(Reservation.datetime_slot).all()
    return render_template("index.html", reservations=reservations)
